import asyncio
import websockets
import json
from pymongo import MongoClient
from constants import API_WEBSOCKET
import os
# Conecta ao MongoDB
# client = MongoClient("mongodb://localhost:27017")
# db = client["transport_data"]
# collection = 'buses_locations'

connection_string = "mongodb+srv://QuadCore:AViuL9s9QSgkCBX7@buson.rhgqz.mongodb.net/transport_data?retryWrites=true&w=majority"
client = MongoClient(connection_string)
db = client["BusON_Crowdsourcing"]
collection = "buses_locations"

async def send_locations(websocket):
    try:
        # Recebe a lista de _ssid dos ônibus ativos
        active_buses_ssid = await websocket.recv()
        active_buses_ssid = json.loads(active_buses_ssid)  # Converte de JSON para lista

        print(f"Ônibus ativos recebidos: {active_buses_ssid}")

        while True:
            # Busca as localizações atualizadas de todos os ônibus ativos
            updated_locations = []
            for bus_ssid in active_buses_ssid:
                document = db[collection].find_one({"_id": bus_ssid})
                if document:
                    updated_position = {
                        "_id": bus_ssid,
                        "location": document.get("last_update", {})
                    }
                    updated_locations.append(updated_position)

            # Envia as localizações atualizadas para o cliente
            await websocket.send(json.dumps(updated_locations))
            print(f"Localizações enviadas: {updated_locations}")

            # Aguarda 1 segundo antes de enviar a próxima atualização
            await asyncio.sleep(1)

    except websockets.ConnectionClosed:
        print("Conexão com o cliente fechada.")
    except Exception as e:
        print(f"Erro: {e}")

async def main():
    PORT = int(os.getenv("PORT", 8765))
    async with websockets.serve(send_locations, '0.0.0.0', PORT):
        print(f"Servidor WebSocket rodando em ws://{API_WEBSOCKET}:8765")
        await asyncio.Future()  # Mantém o servidor rodando

# Inicia o servidor
asyncio.run(main())