import argparse
import json
import paho.mqtt.client as mqtt
import time

# Wiliot Hackathon entry for William Wood Harter
# (c) copyright 2023 - William Wood Harter
#
# License: MIT License

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected result code {0}".format(str(rc)))


def main():
    parser = argparse.ArgumentParser(
        description='Pixel simulator. Will send fax temperature messages to the MQTT topic_name'
    )
    parser.add_argument(
        'topic_name',
        help="The name of the topic to publish to (Default=test.mosquitto.org)",
        )

    parser.add_argument(
        '-m', '--mqtt_host',
        help='The hostname of the MQTT server. default = test.mosquitto.org ',
        default = "test.mosquitto.org"
        )
    args = parser.parse_args()

    client = mqtt.Client("pixel sim")  # Create instance of client with client ID
    client.on_connect = on_connect  # Define callback function for successful connection

    client.connect(args.mqtt_host, 1883)

    STEPS = 5
    cur_step = STEPS
    cur_temp = 22
    delta = 0.01

    packet = {  "eventName": "temperature",
                "value": "21.0",
                "startTime": "1676068290536",
                "endTime": "0",
                "ownerId": "673344343533",
                "createdOn": "1676068343274",
                "assetId": "f7b28423-3b7e-436f-b97d-17afe8db6c4d",
                "categoryID": "Default",
                "confidence": "1.00",
                "keySet": "[(key:temperature,value:21.0)]"}

    while True:
        packet["value"] = cur_temp
        packet["startTime"] = time.time()
        packet["createdOn"] = time.time()
        client.publish(args.topic_name, json.dumps(packet))

        # cycle the temperature up and down
        if cur_step>0:
            cur_step -= 1
            cur_temp += delta
        else:
            cur_step = STEPS
            delta = -delta  # switch temperature direction

        print("Just published " + str(cur_temp) + " to topic: "+args.topic_name)
        time.sleep(3)


if __name__ == '__main__':
    main()