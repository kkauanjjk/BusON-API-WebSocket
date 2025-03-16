from flask import Flask, request, jsonify
from pymongo import MongoClient
from constants import API_HOST
import time  

app = Flask(__name__)

# client = MongoClient("mongodb://localhost:27017")
# db = client["transport_data"]

connection_string = "mongodb+srv://QuadCore:AViuL9s9QSgkCBX7@buson.rhgqz.mongodb.net/transport_data?retryWrites=true&w=majority"
client = MongoClient(connection_string)
db = client["BusON_Crowdsourcing"]

# Verifique as coleções disponíveis
print(db.list_collection_names())

def get_bus_collection(ssid):
    return db[f"bus_{ssid}"]

def create_or_update_user(bus_ssid, user_id, latitude, longitude, speed, rssi, heading, timestamp):
    collection = get_bus_collection(bus_ssid)

    existing_user = collection.find_one({"_id": user_id})
    frame_data = {
        "timestamp": timestamp,  # Coloca timestamp dentro do frame
        "latitude": latitude,
        "longitude": longitude,
        "speed": speed,  # Substituído de 'velocidade' para 'speed'
        "RSSI": rssi,
        "heading": heading  # Adiciona o campo heading
    }

    if not existing_user:
        collection.insert_one({
            "_id": user_id,
            "ssid": bus_ssid,
            "last_update": {
                "timestamp": timestamp,  
                "latitude": latitude,
                "longitude": longitude,
                "speed": speed,  
                "RSSI": rssi,
                "heading": heading  
            },
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
                    "last_update": {
                        "timestamp": timestamp,  
                        "latitude": latitude,
                        "longitude": longitude,
                        "speed": speed,  
                        "RSSI": rssi,
                        "heading": heading
                    },
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
        # Log para depuração
        print("Dados recebidos:", data)

        # Verifica se todos os campos obrigatórios estão presentes
        required_fields = ["bus_ssid", "user_id", "latitude", "longitude", "speed", "rssi", "heading"]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Campo obrigatório faltando: {field}"}), 400

        bus_ssid = data["bus_ssid"]
        user_id = data["user_id"]
        latitude = data["latitude"]
        longitude = data["longitude"]
        speed = data["speed"]  # Alterado de 'velocidade' para 'speed'
        rssi = data["rssi"]
        heading = data["heading"]  # Novo campo
        
        # Obtém o tempo atual com fuso horário
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S %z', time.localtime())  # Formato: '2025-03-14 14:25:30 +0000'

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
