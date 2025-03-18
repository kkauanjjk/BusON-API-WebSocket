import asyncio
import websockets
import json
import os
from pymongo import MongoClient
from http import HTTPStatus
from websockets.http import Headers

# Conexão com MongoDB
connection_string = "mongodb+srv://QuadCore:AViuL9s9QSgkCBX7@buson.rhgqz.mongodb.net/transport_data?retryWrites=true&w=majority"
client = MongoClient(connection_string)
db = client["BusON_Crowdsourcing"]
collection = "buses_locations"

# Porta definida pelo Render
PORT = int(os.getenv("PORT", 10000))

# Função para responder ao Health Check
async def health_check(path, request_headers):
    if path == "/health":
        return HTTPStatus.OK, Headers(), b"OK"
    return None  # Continua para o WebSocket

# Servidor WebSocket
async def send_locations(websocket):
    try:
        active_buses_ssid = await websocket.recv()
        active_buses_ssid = json.loads(active_buses_ssid)

        print(f"Ônibus ativos recebidos: {active_buses_ssid}")

        while True:
            updated_locations = []
            for bus_ssid in active_buses_ssid:
                document = db["buses_locations"].find_one({"_id": bus_ssid})
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

async def start_websocket_server():
    async with websockets.serve(
        send_locations,
        "0.0.0.0",
        PORT,
        process_request=health_check  # Adiciona o health check na mesma porta
    ):
        print(f"Servidor WebSocket rodando na porta {PORT}")
        await asyncio.Future()  # Mantém o servidor rodando

# Inicia o servidor WebSocket
asyncio.run(start_websocket_server())
