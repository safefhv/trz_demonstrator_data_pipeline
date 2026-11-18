# Live Coding Guide: `demo_start` → `demo_04`

Builds the data pipeline step by step:

```
IM simulator ──OPC UA──► opc-ua client ──MQTT──► mqtt-broker ──► Redis (current state)
                                                      │                │ lookup
                                                      └──► hydration ◄─┘
                                                              │
                                                          Redpanda (data_product_performance)
                                                              │
                                                           insert ──► TimescaleDB
```

Each step matches a branch. To catch up or reset at any point: `git checkout demo_0X`.

| Step | Branch       | Topic                                      |
|------|--------------|--------------------------------------------|
| 0    | `demo_start` | Baseline: core stack, simulator, gateway   |
| 1    | `demo_01`    | OPC UA → MQTT (automated client)           |
| 2    | `demo_02`    | MQTT → Redis (current-state bridge)        |
| 3    | `demo_03`    | Hydration: MQTT + Redis → Redpanda         |
| 4    | `demo_04`    | Redpanda → TimescaleDB                     |

### UIs used during the demo

| Service          | URL                    | Login                |
|------------------|------------------------|----------------------|
| Gateway (OPC UA) | http://localhost:8000  | –                    |
| RedisInsight     | http://localhost:5540  | –                    |
| Redpanda Console | http://localhost:8080  | –                    |
| pgAdmin          | http://localhost:5050  | admin@example.com / admin |
| Grafana          | http://localhost:3000  | admin / admin        |

---

## Step 0: Baseline (`demo_start`)

**What is already there**

- `compose-core.yml`: Redis (with volume `demonstrator_redis`), RedisInsight, Mosquitto, Redpanda + Console, TimescaleDB, pgAdmin, Grafana.
- `compose-demonstrator.yml`: `im-simulator` (OPC UA server, port 4841 on the host) and `gateway` (web UI for browsing nodes and calling methods). The `imm-client` service is present but **commented out**.
- `demonstrator/opc_ua_client/client.py`: the generic OPC UA → MQTT client. It reads a JSON config, subscribes to the listed nodes and publishes each change as `{"value", "timestamp", "node"}` (retained) to `<topic_root>/<machine>/<machine>/<description>`.
- `startup.sh` / `shutdown.sh`: build missing images, create the `demonstrator` network, start core, then the demonstrator stack.

**Prepare**

```bash
git checkout demo_start
./startup.sh
```

**Show**

- Gateway at :8000: browse `InjectionMoldingMachine`, call `AddJob` + `Start`, watch values change.
- Point out that nothing reaches MQTT yet.

---

## Step 1: OPC UA → MQTT (`demo_01`)

**Changes**

1. **New file `demonstrator/imm_01.json`**: the client config for this machine.
   ```json
   {
     "machine_name": "imm_01",
     "opcua_url": "opc.tcp://im-simulator:4840/",
     "topic_root": "uns-v1/adm/segment01/productionline01/molding",
     "subscription_list": [
       { "node_id": "ns=2;s=IMM.Status.State",               "description": "state" },
       { "node_id": "ns=2;s=IMM.Status.StateCode",           "description": "state_code" },
       { "node_id": "ns=2;s=IMM.Production.CycleTime",       "description": "cycle_time" },
       { "node_id": "ns=2;s=IMM.Production.TargetCycleTime", "description": "target_cycle_time" },
       { "node_id": "ns=2;s=IMM.Production.Order",           "description": "order" },
       { "node_id": "ns=2;s=IMM.Production.Article",         "description": "article" },
       { "node_id": "ns=2;s=IMM.Process.InjectionTime",      "description": "injection_time" },
       { "node_id": "ns=2;s=IMM.Process.PlasticizingTime",   "description": "plasticizing_time" },
       { "node_id": "ns=2;s=IMM.Production.PartStatus",      "description": "part_status" }
     ]
   }
   ```
   Talking points: stable string node ids; `topic_root` follows the UNS hierarchy (enterprise/site/segment/line/cell).

2. **`compose-demonstrator.yml`**: uncomment and complete the `imm-client` service.
   ```yaml
   imm-client:
     container_name: imm-client
     image: opcua-client:latest
     environment:
       MQTT_BROKER: mqtt-broker
       MQTT_PORT: '1883'
     restart: on-failure
     depends_on:
       - im-simulator
     volumes:
       - ./demonstrator/imm_01.json:/app/config.json:ro
   ```
   Talking point: same image for every machine; only the mounted config differs.

**Apply**

```bash
docker compose -p demonstrator -f compose-demonstrator.yml up -d
docker logs -f imm-client          # shows "[push] ... = ..." lines
```

**Show**

```bash
docker exec mqtt-broker mosquitto_sub -t 'uns-v1/#' -v
```

---

## Step 2: MQTT → Redis (`demo_02`)

**Changes**

1. **`demonstrator/imm_01.json`**: rename the site level of the topic.
   ```diff
   - "topic_root": "uns-v1/adm/segment01/productionline01/molding",
   + "topic_root": "uns-v1/demonstrator/segment01/productionline01/molding",
   ```

2. **New file `pipelines/general/mqtt_redis.yaml`**: Redpanda Connect bridge that keeps the latest value of every tag in one Redis hash per workcell.
   ```yaml
   input:
     mqtt:
       urls:
         - mqtt://mqtt-broker:1883
       client_id: ${MQTT_CLIENT_ID:redis-bridge-connect-01}
       topics:
         - uns-v1/+/+/+/+/+/+/+
       qos: 0

   pipeline:
     processors:
       - mapping: |
           let parts = metadata("mqtt_topic").split("/")
           meta hash_name = "workcell:" + $parts.index(-2)
           root = { $parts.index(-1): this.value.not_null().string() }

   output:
     redis_hash:
       url: redis://redis:6379
       key: ${! metadata("hash_name") }
       walk_json_object: true
   ```
   Talking points: the filter has 8 levels (`uns-v1` + 7 `+`) to match the client's topics; hash key `workcell:imm_01`; field name = last topic level; values are stored as strings.

3. **New file `compose-pipelines.yml`**: separate stack for all pipelines, on the same external network.
   ```yaml
   services:
     pipeline-mqtt-redis:
       container_name: pipeline-mqtt-redis
       image: docker.redpanda.com/redpandadata/connect:latest
       command: run /pipelines/general/mqtt_redis.yaml
       volumes:
         - ./pipelines:/pipelines:ro

   networks:
     default:
       name: demonstrator
       external: true
   ```

4. **`startup.sh`**: append the pipeline stack.
   ```bash
   echo "::: Starting pipeline stack :::"
   docker compose -p pipelines -f compose-pipelines.yml up -d
   echo "Pipelines stack is up"
   ```

5. **`shutdown.sh`**: stop the pipeline stack **first**, before the demonstrator stack.
   ```bash
   echo "::: Stopping pipeline stack :::"
   if ! docker compose -p pipelines -f compose-pipelines.yml down; then
     echo "Failed to stop pipeline stack" >&2
     status=1
   fi
   ```

**Apply**

```bash
docker restart imm-client     # the client reads its config only at startup
docker compose -p pipelines -f compose-pipelines.yml up -d
```

> Note: retained messages under the old `uns-v1/adm/...` topics stay in the broker. Clear them, or ignore them, since they don't match later filters.

**Show**

```bash
docker exec redis redis-cli HGETALL workcell:imm_01
```
or RedisInsight at :5540; change a job in the gateway and watch `order` / `article` update.

---

## Step 3: Hydration → Redpanda (`demo_03`)

**Changes**

1. **New file `pipelines/performance/hydration.yaml`**: each new cycle time triggers one enriched event. The context (order, article, setpoints) is looked up from Redis.
   ```yaml
   input:
     mqtt:
       urls:
         - mqtt://mqtt-broker:1883
       topics:
         - uns-v1/+/segment01/+/+/+/+/cycle_time
       qos: 1

   pipeline:
     processors:
       - mapping: |
           let workcell = metadata("mqtt_topic").split("/").index(-2)
           meta workcell = $workcell
           root.workcell = $workcell
           root.plant = metadata("mqtt_topic").split("/").index(1)
           root.segment = metadata("mqtt_topic").split("/").index(2)
           root.productionLine = metadata("mqtt_topic").split("/").index(3)
           root.cycle_time = this.value.number()
           root.timestamp = this.timestamp
       - branch:
           processors:
             - redis:
                 url: redis://redis:6379
                 command: hmget
                 args_mapping: root = [ "workcell:" + metadata("workcell"), "order", "article", "target_cycle_time", "plasticizing_time", "injection_time"]
           result_map: |
             root.order = this.index(0)
             root.article = this.index(1)
             root.target_cycle_time = this.index(2).number()
             root.plasticizing_time = this.index(3).number()
             root.injection_time = this.index(4).number()

   output:
     redpanda:
       seed_brokers:
         - redpanda-0:9092
       topic: data_product_performance
       key: ${! metadata("workcell") }
   ```
   Talking points:
   - The topic filter must have **8 levels** like the published topic. `+` matches exactly one level, and a wrong filter fails silently with no errors in the log.
   - `branch` keeps the original message and merges the Redis lookup into it via `result_map`.
   - Redis returns strings, so `.number()` converts them back.
   - The message key is the workcell, so all events of one machine go to the same partition, in order.

2. **`compose-pipelines.yml`**: add the service.
   ```yaml
   pipeline-performance-hydration:
     container_name: pipeline-performance-hydration
     image: docker.redpanda.com/redpandadata/connect:latest
     command: run /pipelines/performance/hydration.yaml
     volumes:
       - ./pipelines:/pipelines:ro
   ```

3. **`compose-demonstrator.yml`**: cosmetic only (`restart: on-failure` moved below `depends_on`). No need to show this live.

**Apply**

```bash
docker compose -p pipelines -f compose-pipelines.yml up -d
```

**Show**

- Redpanda Console at :8080 → Topics → `data_product_performance`: one message per cycle (about every 15 s while running).
- CLI alternative: `docker exec redpanda-0 rpk topic consume data_product_performance -n 3`

---

## Step 4: Redpanda → TimescaleDB (`demo_04`)

**Changes**

1. **New file `demonstrator/create_table.sql`**: target table as a hypertable.
   ```sql
   CREATE TABLE IF NOT EXISTS data_product_performance (
       ts                 TIMESTAMPTZ      NOT NULL,  -- "timestamp"
       plant              TEXT             NOT NULL,
       segment            TEXT             NOT NULL,
       production_line    TEXT             NOT NULL,  -- "productionLine"
       workcell           TEXT             NOT NULL,
       order_no           TEXT,                       -- "order"
       article            TEXT,
       cycle_time         DOUBLE PRECISION,
       target_cycle_time  DOUBLE PRECISION,
       injection_time     DOUBLE PRECISION,
       plasticizing_time  DOUBLE PRECISION
   );

   SELECT create_hypertable('data_product_performance', by_range('ts'), if_not_exists => TRUE);

   CREATE INDEX IF NOT EXISTS data_product_performance_workcell_ts_idx
       ON data_product_performance (workcell, ts DESC);
   ```
   Talking points: `order` → `order_no` (reserved word), `timestamp` → `ts`, camelCase → snake_case.

2. **New file `pipelines/performance/insert.yaml`**: consume the data product and batch-insert it.
   ```yaml
   input:
     redpanda:
       seed_brokers:
         - redpanda-0:9092
       topics:
         - data_product_performance
       consumer_group: performance
       start_from_oldest: true

   output:
     sql_insert:
       driver: postgres
       dsn: postgres://demonstrator:demonstrator@timescaledb:5432/postgres?sslmode=disable
       table: data_product_performance
       columns:
         - ts
         - plant
         - segment
         - production_line
         - workcell
         - order_no
         - article
         - cycle_time
         - target_cycle_time
         - injection_time
         - plasticizing_time
       args_mapping: |
         root = [
           this.timestamp,
           this.plant,
           this.segment,
           this.productionLine,
           this.workcell,
           this.order.catch(null),
           this.article.catch(null),
           this.cycle_time.number().catch(null),
           this.target_cycle_time.number().catch(null),
           this.injection_time.number().catch(null),
           this.plasticizing_time.number().catch(null)
         ]
       batching:
         count: 100
         period: 15s
   ```
   Talking points:
   - The consumer group plus `start_from_oldest` means everything produced since step 3 is backfilled.
   - Batching writes at most every 15 s, or every 100 messages.
   - `.catch(null)` stores `NULL` instead of failing the insert.

3. **`compose-pipelines.yml`**: add the service.
   ```yaml
   pipeline-performance-insert:
     container_name: pipeline-performance-insert
     image: docker.redpanda.com/redpandadata/connect:latest
     command: run /pipelines/performance/insert.yaml
     volumes:
       - ./pipelines:/pipelines:ro
   ```

**Apply** (create the table **before** starting the pipeline; nothing runs the SQL automatically)

```bash
docker exec -i timescaledb psql -U demonstrator -d postgres < demonstrator/create_table.sql
docker compose -p pipelines -f compose-pipelines.yml up -d
```

**Show**

```bash
docker exec timescaledb psql -U demonstrator -d postgres \
  -c "SELECT ts, workcell, order_no, cycle_time, target_cycle_time FROM data_product_performance ORDER BY ts DESC LIMIT 5;"
```
or pgAdmin at :5050 / a Grafana panel at :3000 (TimescaleDB is the default data source).

---

## Pitfalls to watch for during the live session

- **Client started before the simulator was ready:** the first connection can be refused, and `restart: on-failure` recovers it. If `imm-client` connects but its log stops showing `[push]` lines, run `docker restart imm-client`.
- **Topic levels:** the client publishes `.../molding/imm_01/imm_01/<tag>` (machine name twice, 8 levels). Every MQTT filter has to match that.
- **Config changes in `imm_01.json`** only take effect after restarting `imm-client`.
- **Table already exists** from a rehearsal: `CREATE TABLE IF NOT EXISTS` won't change its columns. Drop it first if the schema changed.
- **Reset between rehearsals:** `./shutdown.sh`. Named volumes (Redis, Redpanda, TimescaleDB, Grafana, pgAdmin) keep their data. Use `docker volume rm` for a clean slate.
