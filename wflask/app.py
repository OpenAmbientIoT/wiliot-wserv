from flask import Flask, make_response, request
from flask_cors import CORS, cross_origin
import json

# Wiliot Hackathon entry for William Wood Harter
# (c) copyright 2023 - William Wood Harter
#
# License: MIT License

# for now we are just going to maintain the data since the last start of flask
# I could db this, but for now this seems like a good starting place.
data = []
pixel_last_temp = {}
pixel_names = {}

# These are wrapper functions that will go into a database at some point
def db_connect():
    # load the device names db
    global pixel_names
    pixel_names = json.load(open('./db_pixel_names.json'))
    print(f"pixel_names={json.dumps(pixel_names)}")

    # eventually this will be a real db. for now just load all the past
    # data into memory
    db_read_raw_data()



def db_store_raw_data(new_entry):
    data.append(new_entry)

    # hack to store the stream in a text file
    txt_db = open("raw_data_received.txt","a")
    txt_db.write(json.dumps(new_entry)+"\n")
    txt_db.close()

def db_read_raw_data():
    txt_db =  open("raw_data_received.txt","r")
    for line in txt_db.readlines():
        new_entry = json.loads(line)
        data.append(new_entry)
        db_store_last_pixel_temp(new_entry)

    print(f"db ready: {len(data)} entries")

def db_store_last_pixel_temp(new_entry):
    if (new_entry) and ('eventName' in new_entry) and (new_entry['eventName']=='temperature'):
        pixel_last_temp[new_entry['assetId']] = {
            'temp': new_entry['value'],
            'time': new_entry['startTime'],
            'pixelname': db_get_pixel_name(new_entry['assetId'])
        }

def db_get_last_pixel_temps():
    return pixel_last_temp

def db_get_pixel_name(assetId):
    if assetId in pixel_names:
        return pixel_names[assetId]
    print(f"assetId {assetId} got no name from:\n{json.dumps(pixel_names)}")
    return 'no name'


app = Flask(__name__)
db_connect()
CORS(app, support_credentials=True)

@app.route('/')
@cross_origin(supports_credentials=True)
def hello():
    return '<h1>William Wood Harter - Wiliot Hackathon</h1>'


# the wserv MQTT listener will post data here
@app.route('/api/add_data', methods=['POST'])
@cross_origin(supports_credentials=True)
def add_data():
    content = request.get_json(silent=True)
    db_store_raw_data(content)
    db_store_last_pixel_temp(content)


    # print(json.dumps(content))
    print(f"datapoints: {len(data)}")
    return make_response(json.dumps({"status":"success",}),200)


# curl http://localhost:5000/api/pixel
# get the list of pixels and their last date/temperature
@app.route('/api/pixels', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_pixels():
    return make_response(json.dumps(
        {
            "status":"success",
            "pixels": db_get_last_pixel_temps()
        }), 200)
