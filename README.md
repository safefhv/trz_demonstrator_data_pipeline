# TRZ Demonstrator Data Pipeline

This is the code repository for the data pipeline that was shown during the 18/11/2026. The german description of the workshop content is available here: https://www.daten-verstehen.at/event/vom-sensor-zum-dashboard-wertvolle-datenprodukte-statt-teurer-datensumpf/

## Quickstart

1. clone the repository
2. `cd` into the repository
3. run `./startup.sh`

## Startup Script

The startup script builds the required docker images if they are not available in your environment yet and starts the docker compose stacks. The final version available in the main branch contains three different docker stacks:
1. `core` contains the main systems. This includes an MQTT broker, Redis + Redis Insight, Redpanda + Redpanda Console and TimescaleDB
2. `demonstrator` contains the simulation machine, the OPC-UA browser and the OPC-UA client that forwards OPC-UA nodes to the MQTT broker
3. `pipelines` contains all of the required redpanda connect pipelines that move data from A to B and transform it. Currently this stack contains three pipelines. One to connect MQTT and Redis, one to create the data product and store it in Redpanda and one to move the data product from redpanda to the TimescaleDB

## Additional branches

The various demo checkpoints from the demonstration are available as individual branches in this repository. This shows the evolution of the data pipeline from zero all the way to the finished pipeline. Before switching branches make sure to run the `./shutdown.sh` script.
