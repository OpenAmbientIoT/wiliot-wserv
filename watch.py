import json
import paho.mqtt.client as mqtt
import requests

# Wiliot Hackathon entry for William Wood Harter
# (c) copyright 2023 - William Wood Harter
#
# License: MIT License


def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected with result code {0}".format(str(rc)))
    # Print result of connection attempt
    client.subscribe("wood-wiliot-test")
    # Subscribe to the topic “digitest/test1”, receive any messages published on it


def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: {} {} {}".format(mid,granted_qos, properties))


def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    print("Message received Topic[" + msg.topic + "]  Payload[" + str(msg.payload) + "]")  # Print a received msg
    try:
        r = requests.post('http://localhost:5000/api/add_data', headers={"Content-Type":"Application/json"}, data=msg.payload)
        if r.status_code != 200:
            print("Error posting data to flask: {}".format(json.dumps(r)))
    except Exception as e:
        print("ERROR connecting to local flask server: {}".format(e))

client = mqtt.Client("wserv watcher")  # Create instance of client with client ID
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
client.on_subscribe = on_subscribe

# client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
client.connect('test.mosquitto.org', 1883)

print("wiliot watch is active and listening...")
client.loop_forever()  # Start networking daemon

