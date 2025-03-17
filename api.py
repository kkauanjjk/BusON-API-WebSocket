from flask import Flask, request, jsonify
from pymongo import MongoClient
from constants import API_HOST
import time
import pytz
from datetime import datetime

app = Flask(__name__)

# Conexão com o MongoDB
connection_string = "mongodb+srv://QuadCore:AViuL9s9QSgkCBX7@buson.rhgqz.mongodb.net/transport_data?retryWrites=true&w=majority"
client = MongoClient(connection_string)
db = client["BusON_Crowdsourcing"]

def get_bus_collection(ssid):
    return db[f"line_{ssid}"]

def get_brazil_timestamp():
    brasil_tz = pytz.timezone("America/Sao_Paulo")
    return datetime.now(brasil_tz).strftime('%Y-%m-%d %H:%M:%S %z')

def create_or_update_user(bus_ssid, user_id, latitude, longitude, speed, rssi, heading, timestamp):
    collection = get_bus_collection(bus_ssid)

    existing_user = collection.find_one({"_id": user_id})
    frame_data = {
        "timestamp": timestamp,
        "latitude": latitude,
        "longitude": longitude,
        "speed": speed,
        "RSSI": rssi,
        "heading": heading
    }

    if not existing_user:
        collection.insert_one({
            "_id": user_id,
            "ssid": bus_ssid,
            "last_update": frame_data,
            "user_movimentation": {
                "time_frame_1": frame_data
            }
        })
    else:
        movement_key = f"time_frame_{len(existing_user['user_movimentation']) + 1}"
        collection.update_one(
            {"_id": user_id},
            {
                "$set": {
                    "last_update": frame_data,
                    f"user_movimentation.{movement_key}": frame_data
                }
            }
        )

def remove_user(bus_ssid, user_id):
    collection = get_bus_collection(bus_ssid)
    collection.delete_one({"_id": user_id})

@app.route("/api/v1/movements", methods=["POST"])
def create_or_update_movement():
    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON inválido"}), 400

    try:
        print("Dados recebidos:", data)

        required_fields = ["bus_ssid", "user_id", "latitude", "longitude", "speed", "rssi", "heading"]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Campo obrigatório faltando: {field}"}), 400

        bus_ssid = data["bus_ssid"]
        user_id = data["user_id"]
        latitude = data["latitude"]
        longitude = data["longitude"]
        speed = data["speed"]
        rssi = data["rssi"]
        heading = data["heading"]
        
        timestamp = get_brazil_timestamp()

        create_or_update_user(bus_ssid, user_id, latitude, longitude, speed, rssi, heading, timestamp)

        return jsonify({"status": "success"}), 200

    except KeyError as e:
        return jsonify({"error": f"Chave faltando: {e}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/movements", methods=["DELETE"])
def remove_movement():
    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON inválido"}), 400

    try:
        bus_ssid = data["bus_ssid"]
        user_id = data["user_id"]

        remove_user(bus_ssid, user_id)
        return jsonify({"status": "success"}), 200

    except KeyError as e:
        return jsonify({"error": f"Chave faltando: {e}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/v1/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == "__main__":
    app.run(host=API_HOST, debug=True, port=5000)
