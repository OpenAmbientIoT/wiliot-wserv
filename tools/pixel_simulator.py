import argparse
import json
import paho.mqtt.client as mqtt
import random
import time

# Wiliot Hackathon entry for William Wood Harter
# (c) copyright 2023 - William Wood Harter
#
# License: MIT License

topic_name = "mqtt-test"

# poor man db using json files
db_cur_asset_count = 0
db_assets = []
db_locations = []
db_locations_by_id = {}
db_connections = []
db_shipping_events = []

def db_assets_init(db_folder):
    print("LOADING ASSETS DB")
    f = open(f'{db_folder}/assets.json')

    global db_assets
    db_assets = json.load(f)


def db_get_next_asset():
    global db_cur_asset_count
    a = db_assets[db_cur_asset_count]
    if db_cur_asset_count<len(db_assets)-1:
        db_cur_asset_count += 1
    else:
        print("Need more assets, reusing names and assetids...")
    return a

def db_locations_init(db_folder):
    print("LOADING LOCATIONS DB")
    f = open(f'{db_folder}locations.json')
    global db_locations
    global db_locations_by_id
    db_locations = json.load(f)
    db_locations_by_id = {l['id']:l for l in db_locations}

def db_get_location(id):
    return db_locations_by_id[id]

def db_connections_init(db_folder):
    print("LOADING CONNECTIONS DB")
    f = open(f'{db_folder}connections.json')
    global db_connections
    db_connections = json.load(f)


def db_shipping_events_init(db_folder):
    print("LOADING SHIPPING EVENTS DB")
    f = open(f'{db_folder}shipping_events.json')
    global db_shipping_events
    db_shipping_events = json.load(f)

def db_init(db_folder):
    db_assets_init(db_folder)
    db_locations_init(db_folder)
    db_connections_init(db_folder)
    db_shipping_events_init(db_folder)
    print("locations: {}\n\n".format(json.dumps(db_locations)))


# Event handler

# Function to find the shortest
# path between two nodes of a graph
# started from here: https://www.geeksforgeeks.org/building-an-undirected-graph-and-finding-shortest-path-using-dictionaries-in-python/
def BFS_SP(graph, origin, dest):
    explored = []

    # Queue for traversing the
    # graph in the BFS
    queue = [[origin]]

    # If the desired node is
    # reached
    if origin == dest:
        print("dest = origin")
        return

    while queue:
        path = queue.pop(0)
        node = path[-1]

        # Condition to check if the
        # current node is not visited
        if node not in explored:
            neighbours = graph[node]

            # Loop to iterate over the
            # neighbours of the node
            for neighbour in neighbours:
                new_path = list(path)
                new_path.append(neighbour)
                queue.append(new_path)

                # Condition to check if the
                # neighbour node is the destination
                if neighbour == dest:
                    print("Shortest path = ", *new_path)
                    return new_path
            explored.append(node)

    print(f"No path exists {origin} to {dest}")
    return

def build_traversal_tree(connections):
    tree = {}
    for c in connections:
        if not c["from"] in tree:
            tree[c["from"]] = []

        print(f"adding {c['from']} to {c['to']}")
        tree[c["from"]].append(c["to"])
    return tree

# events have a min max list of two items, calculate a value between them
def random_from_min_max(min_max):
    return int(random.random()*(min_max[1]-min_max[0]) + min_max[0])

def random_pct():
    return int(random.random()*100)

# in transit pixel handling
# pixel in transit {"assetId":"id", "location":"OAK", "destination":"SNA", "nextEventTime":10, "nextMoveTime": 20}
pixels = []
current_time_in_ticks = 0
time_per_tick = 1   # seconds

class ticker:
    def __init__(self, name):
        self.name = name
        self.current_state = "no state"

    def tick(self):
        print(f"generic tick event, you need to define this: {self.name}")

class pixel_generator(ticker):
    '''
    Class to hold an pixel generator
    '''
    def __init__(self, client, name, origin, destination, time_start, time_between_seconds_min_max, signal_strength_pct_min_max, path_tree, in_transit_pixel_list):
        ticker.__init__(self, name)
        self.client = client
        self.origin = origin
        self.destination = destination
        self.time_start = time_start
        self.time_between_seconds_min_max = time_between_seconds_min_max
        self.signal_strength_pct_min_max = signal_strength_pct_min_max
        self.in_transit_pixel_list = in_transit_pixel_list

        self.next_generation_event = self.time_start

        ## calculat the path
        self.path = BFS_SP(path_tree, self.origin, self.destination)

    # returns if change was made
    def tick(self, current_tick_count):
        if current_tick_count>=self.next_generation_event:
            asset = db_get_next_asset()
            p = pixel(self.client, asset["name"], asset["assetId"], self.path,
                        random_from_min_max(self.signal_strength_pct_min_max),
                        current_tick_count)
            print(f"{self.name} generating new pixel: {p.name}")

            self.in_transit_pixel_list.append(p)
            self.next_generation_event = int(current_tick_count + random_from_min_max(self.time_between_seconds_min_max))
            return(True)
        return(False)


class pixel(ticker):
    '''
    Class to hold an intransit pixel
    '''
    def __init__(self, client, name, assetId, path, signal_strength, current_tick_count):
        ticker.__init__(self, name)

        self.client = client
        self.assetId = assetId
        self.path = path
        self.signal_strength = signal_strength
        self.start_tick_count = current_tick_count

        self.next_arrival_location = path[0]
        self.next_arrival_tick = current_tick_count
        self.have_sent_location = False
        self.path = path[1:]

        self.transit_state = "in_transit"
        self.current_state = "off"


    def tick(self, current_tick_count):
        # return true if something changed
        ret = False
        if self.transit_state == "in_transit":
            if current_tick_count >= self.next_arrival_tick:
                print(f"{self.name} arrival event {self.next_arrival_location}")
                self.transit_state = "at_location"
                self.current_location = self.next_arrival_location

                # set the next location move to next path
                if len(self.path)>0:
                    self.next_arrival_location = self.path[0]
                    self.path = self.path[1:]
                else:
                    self.next_arrival_location = None

                self.next_leave_tick = current_tick_count + int(random_from_min_max(db_get_location(self.current_location)['lingerTimeHrsMinMax'])*60)
                ret = True
        elif self.transit_state == "at_location":
            if (self.next_leave_tick>0) and (current_tick_count >= self.next_leave_tick):
                print(f"{self.name} is leaving {self.current_location}")
                self.transit_state = "in_transit"
                self.have_sent_location = False
                self.current_state = "off"
                ret = True
            else:
                if self.current_state=="off":
                    # need to see if we turn on
                    loc = db_get_location(self.current_location)
                    if random_pct() < loc['oddsConnecting']:
                        print(f"{self.name} CONNECTED")

                        # send MQTT event
                        self.mqtt_connect()
                        self.current_state = "on"
                        self.next_telemetry_tick = current_tick_count + random_from_min_max(loc["telemetryTimeMinMax"])

                        if not self.have_sent_location:
                            print(f"{self.name} first connect at location, sending location")
                            self.mqtt_location()
                            self.have_sent_location = True
                        ret = True
                elif self.current_state == "on":
                    loc = db_get_location(self.current_location)
                    if current_tick_count > self.next_telemetry_tick:
                        print(f"{self.name} sending Telemetry")
                        # send MQTT event
                        self.mqtt_send_temperature()
                        self.next_telemetry_tick = current_tick_count + random_from_min_max(loc["telemetryTimeMinMax"])

                    if random_pct() < loc['oddsDisconnecting']:

                        # if we are at destination, remove the pixel from service
                        if self.next_arrival_location==None:
                            self.current_state = "arrived"
                            print(f"{self.name} disconnected and arrived at {self.current_location}. removing.")

                        else:
                            self.current_state = "off"
                            print(f"{self.name} disconnected")

                        ret = True
        return ret


    def mqtt_location(self):
        global topic_name

        # sample raw event
        # {"eventName": "geolocation", "value": "33.58897,-117.73657", "startTime": "1676413224918", "endTime": "0", "ownerId": "673344343533", "createdOn": "1676413255762", "assetId": "b5a8be23-52dd-447f-bea1-262072a64333", "categoryID": "Default", "confidence": "1.00", "keySet": "[(key:latitude,value:33.58897)(key:longitude,value:-117.73657)(key:distance,value:-1.0)]"}

        loc = db_get_location(self.current_location)
        packet = {  "eventName": "geolocation",
                    "value": loc["location"],
                    "startTime": "1676068290536",
                    "endTime": "0",
                    "ownerId": "673344343533",
                    "createdOn": "1676068343274",
                    "assetId": self.assetId,
                    "categoryID": "Default",
                    "confidence": "1",
                    "keySet": "[(key:latitude,value:33.58897)(key:longitude,value:-117.73657)(key:distance,value:-1.0)]"
                }
        # TODO: keySet location is hard coded, needs to come from the pixel
        packet["startTime"] = time.time()
        packet["createdOn"] = time.time()
        self.client.publish(topic_name, json.dumps(packet))

    def mqtt_connect(self):
        global topic_name

        # sample raw event
        # {"eventName": "active", "value": "0", "startTime": "1676413224918", "endTime": "1676413241310", "ownerId": "673344343533", "createdOn": "1676413263442", "assetId": "b5a8be23-52dd-447f-bea1-262072a64333", "categoryID": "Default", "confidence": "0.15", "keySet": "[(key:active,value:0)]"}
        packet = {  "eventName": "active",
                    "value": "0",
                    "startTime": "1676068290536",
                    "endTime": "0",
                    "ownerId": "673344343533",
                    "createdOn": "1676068343274",
                    "assetId": self.assetId,
                    "categoryID": "Default",
                    "confidence": "0.15",
                    "keySet": "[(key:active,value:0)]"
                }

        packet["startTime"] = time.time()
        packet["createdOn"] = time.time()
        self.client.publish(topic_name, json.dumps(packet))

    def mqtt_send_temperature(self):
        global topic_name
        packet = {  "eventName": "temperature",
                    "value": "21.0",
                    "startTime": "1676068290536",
                    "endTime": "0",
                    "ownerId": "673344343533",
                    "createdOn": "1676068343274",
                    "assetId": self.assetId,
                    "categoryID": "Default",
                    "confidence": "1.00",
                    "keySet": "[(key:temperature,value:21.0)]"}

        # TODO: currently this temperature doesn't change. should go up and down over time
        packet["value"] = 22.0
        packet["startTime"] = time.time()
        packet["createdOn"] = time.time()
        self.client.publish(topic_name, json.dumps(packet))


def world_tick(things_that_tick):
    global current_time_in_ticks
    current_time_in_ticks += time_per_tick

    bDone = False
    bRemovePixels = False
    while not bDone:
        bDone = True
        for t in things_that_tick:
            if t.tick(current_time_in_ticks):
                # something changed, go through the list again
                bDone = False
            if t.current_state == "arrived":
                bRemovePixels = True

    if bRemovePixels:
        things_that_tick = [t for t in things_that_tick if t.current_state != "arrived"]

    return

# MQTT handler
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
        '-d', '--db_folder',
        help='The folder containing the json files that make up the mock database [default=./sim_db]',
        default='./sim_db'
    )

    parser.add_argument(
        '-m', '--mqtt_host',
        help='The hostname of the MQTT server. default = test.mosquitto.org ',
        default = "test.mosquitto.org"
        )
    args = parser.parse_args()

    global topic_name
    topic_name = args.topic_name

    db_init(args.db_folder)

    tree = build_traversal_tree(db_connections)
    print("tree: {}".format(json.dumps(tree)))

    client = mqtt.Client("pixel sim")  # Create instance of client with client ID
    client.on_connect = on_connect  # Define callback function for successful connection

    things_that_tick = []
    for t in db_shipping_events:
        pg = pixel_generator(client, t["name"], t["origin"], t["destination"], t["time_start"], t["time_between_seconds_min_max"], t["signal_strength_pct_min_max"], tree, things_that_tick)
        things_that_tick.append(pg)


    while (True):
        world_tick(things_that_tick)
        time.sleep(1)

    #path = BFS_SP(tree, "OAK", "PHX")
    #print("path: {}".format(json.dumps(path)))


    '''
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
    '''

if __name__ == '__main__':
    main()