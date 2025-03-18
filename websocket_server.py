import asyncio
import websockets
import json
import os
import threading
from pymongo import MongoClient
from http.server import BaseHTTPRequestHandler, HTTPServer

# Conexão com MongoDB
connection_string = "mongodb+srv://QuadCore:AViuL9s9QSgkCBX7@buson.rhgqz.mongodb.net/transport_data?retryWrites=true&w=majority"
client = MongoClient(connection_string)
db = client["BusON_Crowdsourcing"]
collection = "buses_locations"

# Porta padrão do Render
PORT = int(os.getenv("PORT", 10000))

# Servidor HTTP para responder ao health check do Render
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

def run_health_check_server():
    httpd = HTTPServer(("0.0.0.0", PORT), HealthCheckHandler)
    print(f"Servidor HTTP rodando na porta {PORT} para health checks...")
    httpd.serve_forever()

# Servidor WebSocket
async def send_locations(websocket, path):
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

async def start_websocket_server():
    async with websockets.serve(send_locations, "0.0.0.0", PORT):
        print(f"Servidor WebSocket rodando na porta {PORT}")
        await asyncio.Future()  # Mantém o WebSocket rodando

# Inicia o servidor HTTP em uma thread separada
threading.Thread(target=run_health_check_server, daemon=True).start()

# Inicia o servidor WebSocket
asyncio.run(start_websocket_server())
