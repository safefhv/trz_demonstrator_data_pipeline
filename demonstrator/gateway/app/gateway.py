"""
OPC UA -> MQTT gateway core.

One long-lived OPC UA session is used for browsing *and* for the subscription that
forwards data changes to MQTT. A supervisor task re-establishes the session (and the
subscription) if the server goes away, and the MQTT client reconnects on its own.

The selected tags and all settings are persisted to a JSON file, so the gateway
resumes forwarding after a container restart.
"""

import asyncio
import base64
import json
import logging
import os
import re
import time
from collections import deque
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

import paho.mqtt.client as mqtt
from asyncua import Client, ua

_logger = logging.getLogger("gateway")

CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "/data/gateway_config.json"))

DEFAULT_CONFIG = {
    "opcua_url": os.getenv("OPCUA_URL", "opc.tcp://localhost:4841/"),
    "mqtt_host": os.getenv("MQTT_HOST", "localhost"),
    "mqtt_port": int(os.getenv("MQTT_PORT", "1883")),
    "topic_root": os.getenv("TOPIC_ROOT", "uns-v1/adm/segment01/productionline01/molding"),
    "machine_name": os.getenv("MACHINE_NAME", "imm_01"),
    "publishing_interval_ms": 500,
    "qos": 0,
    "retain": True,
    "forwarding": False,
    "subscription_list": [],
}

NODE_CLASS_NAMES = {nc.value: nc.name for nc in ua.NodeClass}


# --------------------------------------------------------------------- helpers

def utcnow():
    return datetime.now(timezone.utc)


def to_jsonable(value):
    """Convert OPC UA values into something json.dumps understands."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode()
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, ua.LocalizedText):
        return value.Text
    if isinstance(value, ua.QualifiedName):
        return value.to_string()
    if isinstance(value, ua.NodeId):
        return value.to_string()
    if isinstance(value, ua.StatusCode):
        return value.name
    if hasattr(value, "__dataclass_fields__"):
        return {k: to_jsonable(getattr(value, k)) for k in value.__dataclass_fields__}
    return str(value)


def data_type_name(type_node_id):
    if type_node_id is None:
        return None
    if type_node_id.NamespaceIndex == 0 and isinstance(type_node_id.Identifier, int):
        return ua.ObjectIdNames.get(type_node_id.Identifier, type_node_id.to_string())
    return type_node_id.to_string()


def variant_type_of(type_node_id):
    """Map a built-in DataType NodeId (ns=0;i=1..25) to its VariantType."""
    if type_node_id is not None and type_node_id.NamespaceIndex == 0 and isinstance(type_node_id.Identifier, int):
        try:
            return ua.VariantType(type_node_id.Identifier)
        except ValueError:
            pass
    return None


INT_TYPES = {ua.VariantType.SByte, ua.VariantType.Byte, ua.VariantType.Int16, ua.VariantType.UInt16,
             ua.VariantType.Int32, ua.VariantType.UInt32, ua.VariantType.Int64, ua.VariantType.UInt64}


def coerce(raw, vtype):
    """Convert a value coming from the web UI into the Python type for `vtype`."""
    if vtype in INT_TYPES:
        return int(raw)
    if vtype in (ua.VariantType.Double, ua.VariantType.Float):
        return float(raw)
    if vtype == ua.VariantType.Boolean:
        if isinstance(raw, str):
            return raw.strip().lower() in ("1", "true", "yes", "on")
        return bool(raw)
    if vtype == ua.VariantType.String:
        return "" if raw is None else str(raw)
    raise ValueError(f"data type {vtype.name if vtype else 'unknown'} is not supported by this UI")


def sanitize_tag(tag):
    """MQTT topic level: no wildcards, no separators, no spaces."""
    tag = re.sub(r"[^A-Za-z0-9_\-.]", "_", str(tag).strip())
    return tag or "tag"


def sanitize_path(path):
    parts = [p.strip() for p in str(path).split("/")]
    return "/".join(re.sub(r"[+#\s]", "_", p) for p in parts if p)


# ------------------------------------------------------------------------ MQTT

class MqttPublisher:
    def __init__(self):
        self.client = None
        self.connected = False
        self.error = None
        self.target = None
        self.status_topic = None

    def configure(self, host, port, client_id, status_topic):
        self.stop()
        self.target = f"{host}:{port}"
        self.status_topic = status_topic
        self.error = None
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        client.will_set(status_topic, "offline", qos=1, retain=True)
        client.reconnect_delay_set(min_delay=1, max_delay=10)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_connect_fail = self._on_connect_fail
        self.client = client
        try:
            client.connect_async(host, int(port), keepalive=15)
            client.loop_start()
        except Exception as exc:
            self.error = str(exc)
            _logger.warning("mqtt connect to %s failed: %s", self.target, exc)

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            self.connected = False
            self.error = str(reason_code)
            return
        self.connected = True
        self.error = None
        _logger.info("mqtt connected to %s", self.target)
        client.publish(self.status_topic, "online", qos=1, retain=True)

    def _on_connect_fail(self, client, userdata):
        self.connected = False
        self.error = "connection failed"

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected = False
        if reason_code.is_failure:
            self.error = f"disconnected: {reason_code}"
        _logger.info("mqtt disconnected from %s (%s)", self.target, reason_code)

    def publish(self, topic, payload, qos, retain):
        if not self.client or not self.connected:
            return False
        info = self.client.publish(topic, payload, qos=qos, retain=retain)
        return info.rc == mqtt.MQTT_ERR_SUCCESS

    def stop(self):
        if self.client:
            try:
                if self.connected:
                    self.client.publish(self.status_topic, "offline", qos=1, retain=True).wait_for_publish(1)
                self.client.disconnect()
                self.client.loop_stop()
            except Exception:
                pass
        self.client = None
        self.connected = False


# --------------------------------------------------------------------- Gateway

class _SubscriptionHandler:
    def __init__(self, gateway):
        self.gateway = gateway

    def datachange_notification(self, node, value, data):
        self.gateway.forward(node.nodeid.to_string(), data.monitored_item.Value)

    def status_change_notification(self, status):
        _logger.warning("subscription status changed: %s", status)


class Gateway:
    def __init__(self):
        self.config = self._load_config()
        self.client = None
        self.opcua_connected = False
        self.opcua_error = None
        self.want_connected = True
        self.subscription = None
        self.tag_errors = {}            # node_id -> error text
        self.live = {}                  # node_id -> last forwarded sample
        self.messages = deque(maxlen=60)
        self.published = 0
        self.dropped = 0
        self.started = time.time()
        self.mqtt = MqttPublisher()
        self.lock = asyncio.Lock()
        self._supervisor = None

    # ---------------------------------------------------------------- config

    def _load_config(self):
        config = dict(DEFAULT_CONFIG)
        if CONFIG_PATH.exists():
            try:
                config.update(json.loads(CONFIG_PATH.read_text()))
            except Exception as exc:
                _logger.warning("could not read %s: %s", CONFIG_PATH, exc)
        return config

    def save_config(self):
        try:
            CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            CONFIG_PATH.write_text(json.dumps(self.config, indent=2))
        except Exception as exc:
            _logger.warning("could not write %s: %s", CONFIG_PATH, exc)

    def topic_for(self, tag):
        return "/".join(p for p in (self.config["topic_root"], self.config["machine_name"], tag["description"]) if p)

    @property
    def status_topic(self):
        return f"gateway/{self.config['machine_name']}/status"

    def configure_mqtt(self):
        self.mqtt.configure(
            self.config["mqtt_host"],
            self.config["mqtt_port"],
            f"opcua-gateway-{self.config['machine_name']}",
            self.status_topic,
        )

    async def update_settings(self, settings):
        mqtt_keys = ("mqtt_host", "mqtt_port", "machine_name")
        mqtt_changed = any(k in settings and settings[k] != self.config[k] for k in mqtt_keys)
        interval_changed = settings.get("publishing_interval_ms", self.config["publishing_interval_ms"]) != self.config["publishing_interval_ms"]
        url_changed = settings.get("opcua_url", self.config["opcua_url"]) != self.config["opcua_url"]

        for key in ("opcua_url", "mqtt_host", "mqtt_port", "topic_root", "machine_name",
                    "publishing_interval_ms", "qos", "retain"):
            if key in settings:
                self.config[key] = settings[key]
        self.config["topic_root"] = sanitize_path(self.config["topic_root"])
        self.config["machine_name"] = sanitize_tag(self.config["machine_name"])
        self.config["mqtt_port"] = int(self.config["mqtt_port"])
        self.config["qos"] = max(0, min(2, int(self.config["qos"])))
        self.config["publishing_interval_ms"] = max(50, int(self.config["publishing_interval_ms"]))
        self.save_config()

        if mqtt_changed:
            self.configure_mqtt()
        if url_changed:
            await self.connect(self.config["opcua_url"])
        elif interval_changed and self.config["forwarding"]:
            async with self.lock:
                await self._subscribe()

    async def set_tags(self, tags):
        clean, seen = [], set()
        for tag in tags:
            node_id = str(tag["node_id"])
            if node_id in seen:
                continue
            seen.add(node_id)
            name = base = sanitize_tag(tag.get("description") or tag.get("display_name") or node_id)
            n = 2
            while any(t["description"] == name for t in clean):
                name = f"{base}_{n}"
                n += 1
            clean.append({"node_id": node_id, "description": name,
                          "display_name": tag.get("display_name") or name})
        old_ids = [t["node_id"] for t in self.config["subscription_list"]]
        self.config["subscription_list"] = clean
        self.save_config()
        for node_id in list(self.live):
            if node_id not in seen:
                del self.live[node_id]
        if self.config["forwarding"] and old_ids != [t["node_id"] for t in clean]:
            async with self.lock:
                await self._subscribe()

    # -------------------------------------------------------------- lifecycle

    async def start(self):
        self.configure_mqtt()
        self._supervisor = asyncio.create_task(self._supervise())

    async def shutdown(self):
        if self._supervisor:
            self._supervisor.cancel()
        async with self.lock:
            await self._drop_connection()
        self.mqtt.stop()

    async def _supervise(self):
        """Keep the OPC UA session (and the subscription) alive."""
        while True:
            try:
                if self.want_connected and not self.opcua_connected:
                    async with self.lock:
                        if not self.opcua_connected:
                            await self._connect()
                elif self.opcua_connected:
                    await self.client.check_connection()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.opcua_error = str(exc) or type(exc).__name__
                _logger.warning("opc ua connection problem: %s", self.opcua_error)
                async with self.lock:
                    await self._drop_connection()
            await asyncio.sleep(2 if self.opcua_connected else 5)

    async def connect(self, url):
        """Called from the UI: (re)connect right now and report the result."""
        self.config["opcua_url"] = url
        self.save_config()
        self.want_connected = True
        async with self.lock:
            await self._drop_connection()
            try:
                await self._connect()
            except Exception as exc:
                self.opcua_error = str(exc) or type(exc).__name__
                await self._drop_connection()

    async def disconnect(self):
        self.want_connected = False
        async with self.lock:
            await self._drop_connection()
        self.opcua_error = None

    async def _connect(self):
        url = self.config["opcua_url"]
        _logger.info("connecting to %s", url)
        client = Client(url=url, timeout=4)
        client.session_timeout = 30000
        await asyncio.wait_for(client.connect(), timeout=8)
        self.client = client
        self.opcua_connected = True
        self.opcua_error = None
        _logger.info("connected to %s", url)
        if self.config["forwarding"]:
            await self._subscribe()

    async def _drop_connection(self):
        client, self.client = self.client, None
        self.subscription = None
        self.opcua_connected = False
        if client:
            try:
                await asyncio.wait_for(client.disconnect(), timeout=3)
            except Exception:
                pass

    # ------------------------------------------------------------- forwarding

    async def start_forwarding(self):
        self.config["forwarding"] = True
        self.save_config()
        async with self.lock:
            if self.opcua_connected:
                await self._subscribe()

    async def stop_forwarding(self):
        self.config["forwarding"] = False
        self.save_config()
        async with self.lock:
            await self._unsubscribe()

    async def _unsubscribe(self):
        subscription, self.subscription = self.subscription, None
        if subscription:
            try:
                await subscription.delete()
            except Exception:
                pass

    async def _subscribe(self):
        await self._unsubscribe()
        self.tag_errors = {}
        tags = self.config["subscription_list"]
        if not tags or not self.client:
            return
        self.subscription = await self.client.create_subscription(
            self.config["publishing_interval_ms"], _SubscriptionHandler(self))
        # StatusValueTimestamp: also report writes of an unchanged value (e.g. two good
        # parts in a row on PartStatus), so every event on the machine becomes a message.
        # queuesize > 1 keeps several changes that happen within one publishing interval.
        change_filter = ua.DataChangeFilter(Trigger=ua.DataChangeTrigger.StatusValueTimestamp)
        for tag in tags:
            try:
                await self.subscription._subscribe(
                    self.client.get_node(tag["node_id"]), ua.AttributeIds.Value, change_filter, queuesize=10)
            except Exception as exc:
                self.tag_errors[tag["node_id"]] = str(exc) or type(exc).__name__
                _logger.warning("cannot subscribe %s: %s", tag["node_id"], exc)
        _logger.info("subscribed to %d of %d tags", len(tags) - len(self.tag_errors), len(tags))

    def forward(self, node_id, data_value):
        tag = next((t for t in self.config["subscription_list"] if t["node_id"] == node_id), None)
        if tag is None or not self.config["forwarding"]:
            return
        status = data_value.StatusCode
        good = status.is_good()
        timestamp = data_value.SourceTimestamp or data_value.ServerTimestamp or utcnow()
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        value = to_jsonable(data_value.Value.Value) if (good and data_value.Value) else None
        payload = {
            "value": value,
            "timestamp": timestamp.isoformat(),
            "node": tag["display_name"],
            "node_id": node_id,
            "status": "Good" if good else status.name,
        }
        topic = self.topic_for(tag)
        text = json.dumps(payload)
        ok = self.mqtt.publish(topic, text, self.config["qos"], self.config["retain"])
        if ok:
            self.published += 1
        else:
            self.dropped += 1
        sample = self.live.get(node_id, {"count": 0})
        self.live[node_id] = {
            "value": value,
            "timestamp": payload["timestamp"],
            "status": payload["status"],
            "count": sample["count"] + 1,
            "published": ok,
        }
        self.messages.appendleft({"topic": topic, "payload": text, "ok": ok, "at": utcnow().isoformat()})

    # ---------------------------------------------------------------- browsing

    def _require_client(self):
        if not self.client or not self.opcua_connected:
            raise RuntimeError("not connected to an OPC UA server")
        return self.client

    async def browse(self, node_id=None):
        client = self._require_client()
        parent = client.get_node(node_id) if node_id else client.nodes.objects
        refs = await parent.get_children_descriptions()

        async def describe(ref):
            child_id = ua.NodeId(ref.NodeId.Identifier, ref.NodeId.NamespaceIndex, ref.NodeId.NodeIdType)
            node = client.get_node(child_id)
            item = {
                "node_id": child_id.to_string(),
                "browse_name": ref.BrowseName.to_string(),
                "display_name": ref.DisplayName.Text or ref.BrowseName.Name,
                "node_class": NODE_CLASS_NAMES.get(int(ref.NodeClass), str(ref.NodeClass)),
                "has_children": False,
            }
            if item["node_class"] != "Method":
                try:
                    item["has_children"] = bool(await node.get_children_descriptions())
                except Exception:
                    pass
            if item["node_class"] == "Variable":
                try:
                    value_dv, dtype_dv = await node.read_attributes([ua.AttributeIds.Value, ua.AttributeIds.DataType])
                    item["value"] = to_jsonable(value_dv.Value.Value) if value_dv.Value else None
                    item["data_type"] = data_type_name(dtype_dv.Value.Value)
                except Exception as exc:
                    item["value"] = None
                    item["error"] = str(exc)
            return item

        items = await asyncio.gather(*(describe(r) for r in refs))
        order = {"Object": 0, "Variable": 1, "Method": 2}
        # the machine's own nodes first, the standard OPC UA nodes (namespace 0) below
        return sorted(items, key=lambda i: (i["node_id"].startswith("i="), order.get(i["node_class"], 3)))

    async def node_details(self, node_id):
        client = self._require_client()
        node = client.get_node(node_id)
        node_class = await node.read_node_class()
        details = {
            "node_id": node_id,
            "node_class": node_class.name,
            "browse_name": (await node.read_browse_name()).to_string(),
            "display_name": (await node.read_display_name()).Text,
        }
        try:
            details["description"] = (await node.read_description()).Text
        except Exception:
            details["description"] = None

        if node_class == ua.NodeClass.Variable:
            dv = await node.read_data_value(raise_on_bad_status=False)
            dtype = await node.read_data_type()
            access = await node.read_attribute(ua.AttributeIds.UserAccessLevel)
            level = access.Value.Value or 0
            details.update({
                "value": to_jsonable(dv.Value.Value) if dv.Value else None,
                "status": dv.StatusCode.name,
                "source_timestamp": to_jsonable(dv.SourceTimestamp),
                "server_timestamp": to_jsonable(dv.ServerTimestamp),
                "data_type": data_type_name(dtype),
                "writable": bool(level & ua.AccessLevel.CurrentWrite.mask),
                "writable_type_supported": variant_type_of(dtype) in INT_TYPES | {
                    ua.VariantType.Double, ua.VariantType.Float, ua.VariantType.Boolean, ua.VariantType.String},
            })
        elif node_class == ua.NodeClass.Method:
            details["inputs"] = await self._method_args(node, "InputArguments")
            details["outputs"] = await self._method_args(node, "OutputArguments")
        return details

    async def _method_args(self, method_node, prop):
        try:
            args = await (await method_node.get_child(f"0:{prop}")).read_value()
        except Exception:
            return []
        return [{
            "name": a.Name,
            "data_type": data_type_name(a.DataType),
            "description": a.Description.Text if a.Description else None,
        } for a in args or []]

    async def call_method(self, object_id, method_id, raw_args):
        client = self._require_client()
        method = client.get_node(method_id)
        try:
            args = await (await method.get_child("0:InputArguments")).read_value()
        except Exception:
            args = []
        if len(raw_args) != len(args):
            raise ValueError(f"method expects {len(args)} arguments, got {len(raw_args)}")
        variants = []
        for raw, arg in zip(raw_args, args):
            vtype = variant_type_of(arg.DataType)
            variants.append(ua.Variant(coerce(raw, vtype), vtype))
        result = await client.get_node(object_id).call_method(method, *variants)
        return to_jsonable(result)

    async def write_value(self, node_id, raw):
        client = self._require_client()
        node = client.get_node(node_id)
        vtype = variant_type_of(await node.read_data_type())
        await node.write_value(ua.DataValue(ua.Variant(coerce(raw, vtype), vtype)))

    # ------------------------------------------------------------------- state

    def state(self):
        return {
            "opcua": {
                "url": self.config["opcua_url"],
                "connected": self.opcua_connected,
                "error": self.opcua_error,
            },
            "mqtt": {
                "target": self.mqtt.target,
                "connected": self.mqtt.connected,
                "error": self.mqtt.error,
                "status_topic": self.status_topic,
            },
            "forwarding": self.config["forwarding"],
            "subscribed": self.subscription is not None,
            "published": self.published,
            "dropped": self.dropped,
            "tags": [
                {**t, "topic": self.topic_for(t), "live": self.live.get(t["node_id"]),
                 "error": self.tag_errors.get(t["node_id"])}
                for t in self.config["subscription_list"]
            ],
            "messages": list(self.messages)[:30],
            "settings": {k: self.config[k] for k in (
                "opcua_url", "mqtt_host", "mqtt_port", "topic_root", "machine_name",
                "publishing_interval_ms", "qos", "retain")},
        }
