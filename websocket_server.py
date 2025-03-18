import asyncio
import websockets
import json
import os
import http
from http.server import BaseHTTPRequestHandler, HTTPServer
from pymongo import MongoClient
from constants import API_WEBSOCKET

# Configuração do MongoDB
connection_string = "mongodb+srv://QuadCore:AViuL9s9QSgkCBX7@buson.rhgqz.mongodb.net/transport_data?retryWrites=true&w=majority"
client = MongoClient(connection_string)
db = client["BusON_Crowdsourcing"]
collection = "buses_locations"

PORT = int(os.getenv("PORT", 10000))  # Porta usada pelo Render

async def send_locations(websocket):
    try:
        active_buses_ssid = await websocket.recv()
        active_buses_ssid = json.loads(active_buses_ssid)

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
            await asyncio.sleep(1)

    except websockets.ConnectionClosed:
        print("Conexão fechada.")
    except Exception as e:
        print(f"Erro: {e}")

async def websocket_server():
    async with websockets.serve(send_locations, "0.0.0.0", PORT):
        print(f"Servidor WebSocket rodando em ws://0.0.0.0:{PORT}")
        await asyncio.Future()  # Mantém o servidor rodando

# Servidor HTTP separado para Health Check
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK\n")
        else:
            self.send_response(404)
            self.end_headers()

def run_http_server():
    http_port = 8080  # Porta diferente do WebSocket
    server = HTTPServer(("0.0.0.0", http_port), HealthCheckHandler)
    print(f"Servidor HTTP rodando em http://0.0.0.0:{http_port}")
    server.serve_forever()

# Rodar WebSocket e HTTP Server juntos
loop = asyncio.get_event_loop()
loop.create_task(websocket_server())

# Rodar HTTP server no mesmo loop
from threading import Thread
http_thread = Thread(target=run_http_server)
http_thread.start()

loop.run_forever()
