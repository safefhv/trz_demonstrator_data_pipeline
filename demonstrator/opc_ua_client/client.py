import asyncio
import json
import os
from datetime import datetime
from asyncua import Client, ua

import paho.mqtt.client as mqtt

CONFIG_FILE = os.getenv("CONFIG_FILE", "config.json")

SUBSCRIPTION_PERIOD_MS = 500

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

MQTT_CLIENT_ID = "opc_ua_client_123457"

def load_config():
    with open(CONFIG_FILE) as file:
        return json.load(file)


def to_jsonable(value):
    """Keep native JSON types (str, bool, int, float) as-is; stringify anything else."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, ua.LocalizedText):
        return value.Text
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    return str(value)


def format_value(value):
    if isinstance(value, float):
        return f"{value:.2f}"
    return repr(value)


class SubscriptionHandler:
    def __init__(self, mqtt_client, topics, display_names):
        self.mqtt_client = mqtt_client
        self.topics = topics
        self.display_names = display_names
    """Called by the client's background task whenever a subscribed node changes."""

    def datachange_notification(self, node, value, data):
        source_timestamp = data.monitored_item.Value.SourceTimestamp
        print(f"   [push] {node} = {format_value(value)}   (source timestamp {source_timestamp})")

        payload = {
            "value": to_jsonable(value),
            "timestamp": source_timestamp.isoformat(),
            "node": self.display_names[node.nodeid]
        }
        self.mqtt_client.publish(
            self.topics[node.nodeid],
            json.dumps(payload),
            qos=0,
            retain=True)

def connect_mqtt(client_id):
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id)
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
    mqtt_client.loop_start()
    return mqtt_client

def disconnect_mqtt(mqtt_client):
    mqtt_client.loop_stop()
    mqtt_client.disconnect()


async def main():
    config = load_config()

    print(f"connecting to {config['opcua_url']} ...")
    mqtt_client = connect_mqtt(f"opc_ua_client_{config['machine_name']}")

    try:
        await run_opc_ua(mqtt_client, config)
    finally:
        disconnect_mqtt(mqtt_client)


async def run_opc_ua(mqtt_client, config):
    async with Client(url=config['opcua_url']) as client:
        print("connected, session established\n")

        print("subscribing to all relevant node-ids until Ctrl+C, the server pushes:")
        topics = {
            ua.NodeId.from_string(node['node_id']) : f"{config['topic_root']}/{config['machine_name']}/{config['machine_name']}/{node['description']}"
            for node in config['subscription_list']
        }




        display_names = {
            node_id: (await client.get_node(node_id).read_display_name()).Text
            for node_id in topics
        }
        handler = SubscriptionHandler(mqtt_client, topics, display_names)
        subscription = await client.create_subscription(SUBSCRIPTION_PERIOD_MS, handler)
        handles = await subscription.subscribe_data_change([client.get_node(node_id) for node_id in topics])

        try:
            await asyncio.Event().wait()  # block until cancelled (Ctrl+C)
        finally:
            await subscription.unsubscribe(handles)
            await subscription.delete()
            print("\ndisconnecting")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("stopped by user")
