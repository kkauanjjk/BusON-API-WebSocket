import asyncio
import websockets
import json
import os
import http
from pymongo import MongoClient

# Definições de conexão com MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://QuadCore:AViuL9s9QSgkCBX7@buson.rhgqz.mongodb.net/transport_data?retryWrites=true&w=majority")
client = MongoClient(MONGO_URI)
db = client["BusON_Crowdsourcing"]
collection = "buses_locations"

# Porta do servidor
PORT = int(os.getenv("PORT", 8080))

async def health_check(path, request_headers):
    if path == "/healthz":
        return http.HTTPStatus.OK, [], b"OK\n"

async def send_locations(websocket):
    try:
        active_buses_ssid = await websocket.recv()
        active_buses_ssid = json.loads(active_buses_ssid) 

        print(f"Ônibus ativos recebidos: {active_buses_ssid}")

        while True:
            updated_locations = []
            for bus_ssid in active_buses_ssid:
                document = db[collection].find_one({"_id": bus_ssid})
                if document:
                    updated_position = {
                        "_id": bus_ssid,
                        "location": document.get("last_update", {})
                    }
                    updated_locations.append(updated_position)

            await websocket.send(json.dumps(updated_locations))
            print(f"Localizações enviadas: {updated_locations}")

            await asyncio.sleep(1)

    except websockets.ConnectionClosed:
        print("Conexão com o cliente fechada.")
    except Exception as e:
        print(f"Erro: {e}")

async def main():
    async with websockets.serve(send_locations, "0.0.0.0", PORT, process_request=health_check):
        print(f"Servidor WebSocket rodando na porta {PORT}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
