"""
OPC UA simulation of an injection molding machine (IMM).

The machine produces one part roughly every 15 seconds. Every cycle runs through
the typical phases of injection molding:

    MoldClosing -> Injection -> Holding -> Plasticizing -> Cooling -> MoldOpening -> Ejection

Address space (namespace "http://mycoolcompany.com/InjectionMolding"):

    Objects/InjectionMoldingMachine
        Identification/   static nameplate data
        Status/           State, StateCode, CyclePhase, alarms
        Production/       active job, counters, job queue, cycle time
        Process/          sensor values (temperatures, pressures, screw, clamp, energy)
        Setpoints/        writable setpoints that influence the process
        AddJob(Order, Article, Quantity, TargetCycleTime) -> QueueLength
        Start(), Stop(), Reset(), ClearQueue(), InjectFault()

All node ids are stable string ids (e.g. ns=2;s=IMM.Process.MeltTemperature), so
clients can store them and still find them after a restart of the server.

State machine:

    Stopped --Start--> Idle (no job) / Running (job available)
    Idle    --AddJob-> Running
    Running --job done, queue empty--> Idle
    Running/Idle --Stop--> Stopped
    any     --fault--> Fault --Reset--> Stopped
"""

import asyncio
import json
import logging
import os
import random
import time
from collections import deque
from datetime import datetime, timezone

from asyncua import Server, ua
from asyncua.common.methods import uamethod

_logger = logging.getLogger("im_simulator")

ENDPOINT = os.getenv("OPCUA_ENDPOINT", "opc.tcp://0.0.0.0:4840/")
NAMESPACE = "http://mycoolcompany.com/InjectionMolding"
MACHINE_NAME = os.getenv("MACHINE_NAME", "IMM-01")

TICK_SECONDS = 0.25          # simulation resolution
RANDOM_FAULT_PROBABILITY = float(os.getenv("RANDOM_FAULT_PROBABILITY", "0.003"))  # per cycle

# Nominal durations of the cycle phases in seconds. Plasticizing and cooling are
# computed per cycle (cooling time is a setpoint). Default total: ~15 s.
MOLD_CLOSING = 1.4
INJECTION = 1.1
HOLDING = 3.0
MOLD_OPENING = 1.4
EJECTION = 1.1

NOMINAL_MELT_TEMPERATURE = 232.0   # material sweet spot (PP)
SHOT_SIZE_MM = 62.0
CUSHION_MM = 5.0
CLAMP_FORCE_KN = 750.0
NOMINAL_SHOT_WEIGHT_G = 24.6

STATE_CODES = {"Stopped": 0, "Idle": 1, "Running": 2, "Fault": 3}

RANDOM_FAULTS = [
    ("E201", "Nozzle heater failure"),
    ("E305", "Mold temperature controller alarm"),
    ("E412", "Ejection failed - part stuck in mold"),
    ("E518", "Hydraulic oil filter clogged"),
]


def utcnow():
    return datetime.now(timezone.utc)


def approach(actual, target, dt, tau):
    """First-order lag: move `actual` towards `target` with time constant `tau`."""
    return actual + (target - actual) * min(1.0, dt / tau)


class Machine:
    def __init__(self, server: Server, idx: int):
        self.server = server
        self.idx = idx
        self.nodes = {}     # name -> (node, variant type)
        self.cache = {}     # name -> last written value

        # machine state
        self.state = "Stopped"
        self.alarm = None
        self.job = None                 # dict(order, article, quantity, target_cycle_time)
        self.queue = deque()
        self.good_parts = 0
        self.reject_parts = 0
        self.total_parts = 0
        self.completed_jobs = 0

        # cycle state
        self.phase = None
        self.phase_elapsed = 0.0
        self.durations = {}
        self.cycle_started = None
        self.peak_pressure = 0.0
        self.cushion = CUSHION_MM

        # process state (machine is warm when the simulation starts)
        self.zones = [210.0, 220.0, 230.0, 230.0]
        self.nozzle = 235.0
        self.melt = NOMINAL_MELT_TEMPERATURE
        self.mold = 50.0
        self.oil = 40.0
        self.energy_kwh = 1520.0
        self.screw_position = SHOT_SIZE_MM

    # ------------------------------------------------------------------ nodes

    async def add_var(self, parent, path, name, value, vtype, description, writable=False):
        node = await parent.add_variable(
            ua.NodeId(f"IMM.{path}.{name}", self.idx),
            ua.QualifiedName(name, self.idx),
            ua.Variant(value, vtype),
        )
        await node.write_attribute(
            ua.AttributeIds.Description,
            ua.DataValue(ua.Variant(ua.LocalizedText(description), ua.VariantType.LocalizedText)),
        )
        if writable:
            await node.set_writable()
        self.nodes[name] = (node, vtype)
        self.cache[name] = value
        return node

    async def add_folder(self, parent, path, name):
        return await parent.add_object(ua.NodeId(f"IMM.{path}", self.idx), ua.QualifiedName(name, self.idx))

    async def set(self, name, value):
        """Write a value to the address space, but only if it changed."""
        node, vtype = self.nodes[name]
        if isinstance(value, float):
            value = round(value, 2)
        if self.cache.get(name) == value:
            return
        self.cache[name] = value
        await node.write_value(ua.DataValue(ua.Variant(value, vtype), SourceTimestamp=utcnow(), ServerTimestamp=utcnow()))

    async def get(self, name):
        node, _ = self.nodes[name]
        return await node.read_value()

    async def build(self):
        D, S, I32, U32, B = (ua.VariantType.Double, ua.VariantType.String, ua.VariantType.Int32,
                             ua.VariantType.UInt32, ua.VariantType.Boolean)
        objects = self.server.nodes.objects
        machine = await objects.add_object(ua.NodeId("IMM", self.idx), ua.QualifiedName("InjectionMoldingMachine", self.idx))
        self.machine_node = machine

        ident = await self.add_folder(machine, "Identification", "Identification")
        await self.add_var(ident, "Identification", "MachineName", MACHINE_NAME, S, "Name of the machine")
        await self.add_var(ident, "Identification", "Manufacturer", "SimuPlast GmbH", S, "Manufacturer")
        await self.add_var(ident, "Identification", "Model", "SP-750 hybrid", S, "Machine model")
        await self.add_var(ident, "Identification", "SerialNumber", "SP750-2026-0042", S, "Serial number")
        await self.add_var(ident, "Identification", "MaxClampForce", 750.0, D, "Max. clamping force [kN]")

        status = await self.add_folder(machine, "Status", "Status")
        await self.add_var(status, "Status", "State", self.state, S, "Machine state: Stopped, Idle, Running, Fault")
        await self.add_var(status, "Status", "StateCode", 0, I32, "Machine state as number: 0=Stopped 1=Idle 2=Running 3=Fault")
        await self.add_var(status, "Status", "CyclePhase", "-", S, "Current phase of the injection cycle")
        await self.add_var(status, "Status", "AlarmActive", False, B, "True while an alarm is active")
        await self.add_var(status, "Status", "AlarmCode", "", S, "Code of the active alarm")
        await self.add_var(status, "Status", "AlarmText", "", S, "Text of the active alarm")

        prod = await self.add_folder(machine, "Production", "Production")
        await self.add_var(prod, "Production", "Order", "", S, "Active production order")
        await self.add_var(prod, "Production", "Article", "", S, "Article produced by the active order")
        await self.add_var(prod, "Production", "TargetQuantity", 0, U32, "Number of good parts to produce for the active order")
        await self.add_var(prod, "Production", "TargetCycleTime", 0.0, D, "Target cycle time of the active order [s]")
        await self.add_var(prod, "Production", "GoodParts", 0, U32, "Good parts of the active order")
        await self.add_var(prod, "Production", "RejectParts", 0, U32, "Rejected parts of the active order")
        await self.add_var(prod, "Production", "JobProgress", 0.0, D, "Progress of the active order [%]")
        await self.add_var(prod, "Production", "PartStatus", -1, I32, "Quality of the last part: 1=good, 0=reject, -1=none yet")
        await self.add_var(prod, "Production", "CycleTime", 0.0, D, "Duration of the last cycle [s]")
        await self.add_var(prod, "Production", "TotalParts", 0, U32, "Shot counter over the machine lifetime")
        await self.add_var(prod, "Production", "CompletedJobs", 0, U32, "Number of completed orders")
        await self.add_var(prod, "Production", "QueuedJobs", 0, U32, "Number of orders waiting in the queue")
        await self.add_var(prod, "Production", "JobQueue", "[]", S, "Waiting orders as JSON")

        proc = await self.add_folder(machine, "Process", "Process")
        for i in range(4):
            await self.add_var(proc, "Process", f"BarrelTemperatureZone{i + 1}", self.zones[i], D, f"Barrel temperature zone {i + 1} [°C]")
        await self.add_var(proc, "Process", "NozzleTemperature", self.nozzle, D, "Nozzle temperature [°C]")
        await self.add_var(proc, "Process", "MeltTemperature", self.melt, D, "Melt temperature [°C]")
        await self.add_var(proc, "Process", "MoldTemperature", self.mold, D, "Mold temperature [°C]")
        await self.add_var(proc, "Process", "OilTemperature", self.oil, D, "Hydraulic oil temperature [°C]")
        await self.add_var(proc, "Process", "InjectionPressure", 0.0, D, "Current injection pressure [bar]")
        await self.add_var(proc, "Process", "PeakInjectionPressure", 0.0, D, "Peak injection pressure of the last cycle [bar]")
        await self.add_var(proc, "Process", "HoldingPressure", 0.0, D, "Actual holding pressure of the last cycle [bar]")
        await self.add_var(proc, "Process", "InjectionSpeed", 0.0, D, "Current injection speed [mm/s]")
        await self.add_var(proc, "Process", "ScrewPosition", self.screw_position, D, "Screw position [mm]")
        await self.add_var(proc, "Process", "ScrewSpeed", 0.0, D, "Screw rotation speed [rpm]")
        await self.add_var(proc, "Process", "ClampForce", 0.0, D, "Current clamping force [kN]")
        await self.add_var(proc, "Process", "Cushion", 0.0, D, "Melt cushion of the last cycle [mm]")
        await self.add_var(proc, "Process", "ShotWeight", 0.0, D, "Weight of the last shot [g]")
        await self.add_var(proc, "Process", "InjectionTime", 0.0, D, "Injection time of the last cycle [s]")
        await self.add_var(proc, "Process", "PlasticizingTime", 0.0, D, "Plasticizing time of the last cycle [s]")
        await self.add_var(proc, "Process", "PowerConsumption", 0.0, D, "Current electrical power [kW]")
        await self.add_var(proc, "Process", "EnergyConsumption", self.energy_kwh, D, "Energy meter [kWh]")

        sp = await self.add_folder(machine, "Setpoints", "Setpoints")
        await self.add_var(sp, "Setpoints", "BarrelTemperatureSetpoint", 230.0, D, "Barrel temperature setpoint, front zone [°C] (valid 160-300)", writable=True)
        await self.add_var(sp, "Setpoints", "MoldTemperatureSetpoint", 50.0, D, "Mold temperature setpoint [°C]", writable=True)
        await self.add_var(sp, "Setpoints", "HoldingPressureSetpoint", 600.0, D, "Holding pressure setpoint [bar]", writable=True)
        await self.add_var(sp, "Setpoints", "CoolingTimeSetpoint", 6.5, D, "Cooling time setpoint [s] - drives the cycle time", writable=True)

        await self.add_methods(machine)

    def arg(self, name, vtype, description):
        return ua.Argument(
            Name=name,
            DataType=ua.NodeId(vtype.value),
            ValueRank=-1,
            ArrayDimensions=[],
            Description=ua.LocalizedText(description),
        )

    async def add_methods(self, machine):
        S, U32, D = ua.VariantType.String, ua.VariantType.UInt32, ua.VariantType.Double

        async def method(name, func, inputs, outputs):
            await machine.add_method(ua.NodeId(f"IMM.{name}", self.idx), ua.QualifiedName(name, self.idx), func, inputs, outputs)

        await method(
            "AddJob", self.m_add_job,
            [self.arg("Order", S, "Production order number"),
             self.arg("Article", S, "Article number"),
             self.arg("Quantity", U32, "Number of good parts"),
             self.arg("TargetCycleTime", D, "Target cycle time in seconds (0 = 15 s)")],
            [self.arg("QueueLength", U32, "Orders waiting after this call")],
        )
        await method("Start", self.m_start, [], [])
        await method("Stop", self.m_stop, [], [])
        await method("Reset", self.m_reset, [], [])
        await method("ClearQueue", self.m_clear_queue, [], [])
        await method("InjectFault", self.m_inject_fault, [], [])

    # ---------------------------------------------------------------- methods

    @uamethod
    async def m_add_job(self, parent, order, article, quantity, target_cycle_time):
        if not order or quantity < 1:
            return ua.StatusCode(ua.StatusCodes.BadOutOfRange)
        self.queue.append({
            "order": order,
            "article": article,
            "quantity": int(quantity),
            "target_cycle_time": float(target_cycle_time) if target_cycle_time > 0 else 15.0,
        })
        _logger.info("job %s added (%d x %s), queue length %d", order, quantity, article, len(self.queue))
        if self.state == "Idle":
            await self.next_job()
        await self.publish_queue()
        return ua.Variant(len(self.queue), ua.VariantType.UInt32)

    @uamethod
    async def m_start(self, parent):
        if self.state != "Stopped":
            return ua.StatusCode(ua.StatusCodes.BadInvalidState)
        if self.job or self.queue:
            if not self.job:
                await self.next_job()
            await self.set_state("Running")
        else:
            await self.set_state("Idle")

    @uamethod
    async def m_stop(self, parent):
        if self.state not in ("Running", "Idle"):
            return ua.StatusCode(ua.StatusCodes.BadInvalidState)
        await self.abort_cycle()
        await self.set_state("Stopped")

    @uamethod
    async def m_reset(self, parent):
        if self.state != "Fault":
            return ua.StatusCode(ua.StatusCodes.BadInvalidState)
        self.alarm = None
        await self.set("AlarmActive", False)
        await self.set("AlarmCode", "")
        await self.set("AlarmText", "")
        await self.set_state("Stopped")

    @uamethod
    async def m_clear_queue(self, parent):
        self.queue.clear()
        await self.publish_queue()

    @uamethod
    async def m_inject_fault(self, parent):
        if self.state == "Fault":
            return ua.StatusCode(ua.StatusCodes.BadInvalidState)
        await self.fault("E999", "Manually injected fault (demo)")

    # ---------------------------------------------------------- state helpers

    async def set_state(self, state):
        if state != self.state:
            _logger.info("state %s -> %s", self.state, state)
        self.state = state
        await self.set("State", state)
        await self.set("StateCode", STATE_CODES[state])
        if state != "Running":
            await self.set("CyclePhase", "-")

    async def fault(self, code, text):
        _logger.warning("FAULT %s: %s", code, text)
        await self.abort_cycle()
        self.alarm = (code, text)
        await self.set("AlarmActive", True)
        await self.set("AlarmCode", code)
        await self.set("AlarmText", text)
        await self.set_state("Fault")

    async def abort_cycle(self):
        self.phase = None
        self.phase_elapsed = 0.0

    async def publish_queue(self):
        await self.set("QueuedJobs", len(self.queue))
        await self.set("JobQueue", json.dumps(list(self.queue)))

    async def publish_job(self):
        job = self.job or {"order": "", "article": "", "quantity": 0, "target_cycle_time": 0.0}
        await self.set("Order", job["order"])
        await self.set("Article", job["article"])
        await self.set("TargetQuantity", job["quantity"])
        await self.set("TargetCycleTime", float(job["target_cycle_time"]))
        await self.set("GoodParts", self.good_parts)
        await self.set("RejectParts", self.reject_parts)
        progress = 100.0 * self.good_parts / job["quantity"] if job["quantity"] else 0.0
        await self.set("JobProgress", float(progress))

    async def next_job(self):
        """Take the next job from the queue. Returns False if the queue is empty."""
        self.job = self.queue.popleft() if self.queue else None
        self.good_parts = 0
        self.reject_parts = 0
        await self.publish_job()
        await self.publish_queue()
        if self.job:
            _logger.info("starting job %s", self.job["order"])
            await self.set_state("Running")
            return True
        await self.set_state("Idle")
        return False

    # -------------------------------------------------------------- simulation

    def plan_cycle(self, cooling_sp):
        # a colder melt needs more torque/time to plasticize
        plast = random.gauss(3.8, 0.15) + max(0.0, NOMINAL_MELT_TEMPERATURE - self.melt) * 0.03
        cooling = max(cooling_sp, plast + 0.3)
        self.durations = {
            "MoldClosing": MOLD_CLOSING + random.gauss(0, 0.05),
            "Injection": INJECTION + random.gauss(0, 0.04) + max(0.0, NOMINAL_MELT_TEMPERATURE - self.melt) * 0.01,
            "Holding": HOLDING,
            "Plasticizing": plast,
            "Cooling": cooling - plast,
            "MoldOpening": MOLD_OPENING + random.gauss(0, 0.05),
            "Ejection": EJECTION + random.gauss(0, 0.05),
        }
        self.cycle_started = time.monotonic()
        self.peak_pressure = 0.0
        self.phase = "MoldClosing"
        self.phase_elapsed = 0.0

    async def run(self):
        last = time.monotonic()
        while True:
            await asyncio.sleep(TICK_SECONDS)
            now = time.monotonic()
            dt = now - last
            last = now
            try:
                await self.tick(dt)
            except Exception:  # keep the machine alive, whatever happens
                _logger.exception("simulation tick failed")

    async def tick(self, dt):
        barrel_sp = await self.get("BarrelTemperatureSetpoint")
        mold_sp = await self.get("MoldTemperatureSetpoint")
        holding_sp = await self.get("HoldingPressureSetpoint")
        cooling_sp = await self.get("CoolingTimeSetpoint")

        running = self.state == "Running"

        # --- thermal behaviour (heaters work in every state except Fault) ---
        heaters_on = self.state != "Fault"
        zone_targets = [barrel_sp - 20, barrel_sp - 10, barrel_sp, barrel_sp] if heaters_on else [25.0] * 4
        for i, target in enumerate(zone_targets):
            tau = 45.0 if heaters_on else 900.0
            self.zones[i] = approach(self.zones[i], target, dt, tau)
        self.nozzle = approach(self.nozzle, barrel_sp + 5 if heaters_on else 25.0, dt, 40.0 if heaters_on else 900.0)
        shear_heat = 2.5 if running else 0.0
        self.melt = approach(self.melt, (self.zones[3] + self.nozzle) / 2 + shear_heat, dt, 8.0)
        mold_bump = 3.0 if running and self.phase in ("Injection", "Holding") else 0.0
        self.mold = approach(self.mold, mold_sp + mold_bump, dt, 20.0)
        self.oil = approach(self.oil, 48.0 if running else 38.0, dt, 600.0)

        for i in range(4):
            await self.set(f"BarrelTemperatureZone{i + 1}", self.zones[i] + random.gauss(0, 0.3))
        await self.set("NozzleTemperature", self.nozzle + random.gauss(0, 0.3))
        await self.set("MeltTemperature", self.melt + random.gauss(0, 0.5))
        await self.set("MoldTemperature", self.mold + random.gauss(0, 0.2))
        await self.set("OilTemperature", self.oil + random.gauss(0, 0.1))

        # --- cycle ---
        pressure = speed = rpm = clamp = 0.0
        motor_kw = 1.5 if self.state in ("Idle", "Stopped") else 0.3

        if running:
            if self.phase is None:
                if not (160.0 <= barrel_sp <= 300.0):
                    await self.fault("E101", f"Barrel temperature setpoint {barrel_sp:.0f} °C out of range (160-300)")
                    return
                self.plan_cycle(cooling_sp)

            self.phase_elapsed += dt
            p = min(1.0, self.phase_elapsed / self.durations[self.phase])
            peak = 980.0 + (NOMINAL_MELT_TEMPERATURE - self.melt) * 9.0

            if self.phase == "MoldClosing":
                clamp = CLAMP_FORCE_KN * max(0.0, (p - 0.6) / 0.4)
                motor_kw = 14.0
            elif self.phase == "Injection":
                self.screw_position = SHOT_SIZE_MM - (SHOT_SIZE_MM - CUSHION_MM - 1.5) * p
                pressure = peak * (p ** 0.6) + random.gauss(0, 8)
                speed = 85.0 + random.gauss(0, 2)
                clamp = CLAMP_FORCE_KN
                motor_kw = 38.0
            elif self.phase == "Holding":
                self.screw_position = CUSHION_MM + 1.5 * (1 - p) + random.gauss(0, 0.02)
                pressure = holding_sp + random.gauss(0, 5)
                speed = 2.0
                clamp = CLAMP_FORCE_KN
                motor_kw = 20.0
            elif self.phase == "Plasticizing":
                self.screw_position = self.cushion + (SHOT_SIZE_MM - self.cushion) * p
                pressure = 80.0 + random.gauss(0, 3)       # back pressure
                rpm = 140.0 + random.gauss(0, 3)
                clamp = CLAMP_FORCE_KN
                motor_kw = 26.0
            elif self.phase == "Cooling":
                clamp = CLAMP_FORCE_KN
                motor_kw = 4.0
            elif self.phase == "MoldOpening":
                clamp = CLAMP_FORCE_KN * max(0.0, 1 - p / 0.3)
                motor_kw = 12.0
            elif self.phase == "Ejection":
                motor_kw = 6.0

            if clamp:
                clamp += random.gauss(0, 3)
            self.peak_pressure = max(self.peak_pressure, pressure)
            if self.phase == "Holding" and p >= 1.0:
                self.cushion = max(0.5, random.gauss(CUSHION_MM, 0.3) - max(0.0, 550 - holding_sp) * 0.01)
            await self.set("CyclePhase", self.phase)

            if p >= 1.0:
                await self.advance_phase(holding_sp, cooling_sp)

        heater_kw = 5.5 + sum(max(0.0, t - a) for t, a in zip(zone_targets, self.zones)) * 0.3 if heaters_on else 0.0
        power = motor_kw + heater_kw + random.gauss(0, 0.2)
        self.energy_kwh += power * dt / 3600.0

        await self.set("InjectionPressure", max(0.0, pressure))
        await self.set("InjectionSpeed", speed)
        await self.set("ScrewSpeed", rpm)
        await self.set("ScrewPosition", self.screw_position)
        await self.set("ClampForce", max(0.0, clamp))
        await self.set("PowerConsumption", max(0.0, power))
        await self.set("EnergyConsumption", self.energy_kwh)

    async def advance_phase(self, holding_sp, cooling_sp):
        phases = list(self.durations)
        index = phases.index(self.phase)
        if index + 1 < len(phases):
            self.phase = phases[index + 1]
            self.phase_elapsed = 0.0
            return
        await self.finish_cycle(holding_sp, cooling_sp)

    async def finish_cycle(self, holding_sp, cooling_sp):
        cycle_time = time.monotonic() - self.cycle_started
        self.phase = None

        # --- quality model: deviations from the process window increase the reject rate ---
        reject_probability = 0.02
        reject_probability += max(0.0, abs(self.melt - NOMINAL_MELT_TEMPERATURE) - 6.0) * 0.04
        reject_probability += max(0.0, 520.0 - holding_sp) * 0.002          # sink marks
        reject_probability += max(0.0, 5.5 - cooling_sp) * 0.15             # warpage
        reject_probability += max(0.0, abs(self.mold - 50.0) - 8.0) * 0.03
        if self.cushion < 2.0:
            reject_probability += 0.5                                       # short shot
        good = random.random() >= min(0.95, reject_probability)

        shot_weight = NOMINAL_SHOT_WEIGHT_G + random.gauss(0, 0.05) - max(0.0, 600 - holding_sp) * 0.002
        self.total_parts += 1
        if good:
            self.good_parts += 1
        else:
            self.reject_parts += 1

        await self.set("CycleTime", cycle_time)
        await self.set("InjectionTime", self.durations["Injection"])
        await self.set("PlasticizingTime", self.durations["Plasticizing"])
        await self.set("PeakInjectionPressure", self.peak_pressure)
        await self.set("HoldingPressure", holding_sp + random.gauss(0, 3))
        await self.set("Cushion", self.cushion)
        await self.set("ShotWeight", shot_weight)
        await self.set("TotalParts", self.total_parts)
        # PartStatus is written on every part, even if equal to the last one,
        # so subscribers see one notification per produced part.
        self.cache.pop("PartStatus", None)
        await self.set("PartStatus", 1 if good else 0)
        await self.publish_job()

        _logger.info("part %d of %s: %s (cycle %.1f s)", self.good_parts, self.job["order"],
                     "GOOD" if good else "REJECT", cycle_time)

        if self.good_parts >= self.job["quantity"]:
            _logger.info("job %s completed", self.job["order"])
            self.completed_jobs += 1
            await self.set("CompletedJobs", self.completed_jobs)
            await self.next_job()
            return

        if random.random() < RANDOM_FAULT_PROBABILITY:
            await self.fault(*random.choice(RANDOM_FAULTS))


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("asyncua").setLevel(logging.WARNING)

    server = Server()
    await server.init()
    server.set_endpoint(ENDPOINT)
    server.set_server_name(f"Injection Molding Simulation {MACHINE_NAME}")
    idx = await server.register_namespace(NAMESPACE)

    machine = Machine(server, idx)
    await machine.build()

    async with server:
        _logger.info("injection molding simulation %s running at %s (namespace index %d)", MACHINE_NAME, ENDPOINT, idx)
        await machine.run()


if __name__ == "__main__":
    asyncio.run(main())
