"""Producteur : écoute le flux temps réel Binance et dépose chaque trade dans Kafka."""
import json
import os

import websocket                       # librairie websocket-client
from confluent_kafka import Producer

# Réglages lus dans les variables d'environnement (définies dans docker-compose.yml)
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "binance.aggtrades")
WS_URL = os.environ.get("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws/btcusdt@aggTrade")

# L'émetteur Kafka : l'équivalent de ta fenêtre de gauche
producer = Producer({"bootstrap.servers": BOOTSTRAP})
compteur = 0


def rapport_livraison(err, msg):
    """Appelée par Kafka pour chaque message : on n'affiche que les échecs."""
    if err is not None:
        print(f"Échec d'envoi : {err}", flush=True)


def on_open(ws):
    print(f"Connecté à {WS_URL}, envoi vers le topic {TOPIC}", flush=True)


def on_message(ws, message):
    """Appelée à chaque trade reçu de Binance."""
    global compteur
    # On dépose le message tel quel (texte JSON -> octets), sans clé
    producer.produce(TOPIC, value=message.encode("utf-8"), callback=rapport_livraison)
    producer.poll(0)                   # laisse Kafka traiter les accusés de réception
    compteur += 1
    if compteur % 100 == 0:
        trade = json.loads(message)
        print(f"{compteur} trades envoyés - dernier prix : {trade['p']}", flush=True)


def on_error(ws, error):
    print(f"Erreur WebSocket : {error}", flush=True)


def on_close(ws, code, raison):
    print(f"WebSocket fermé ({code}) : {raison}", flush=True)
    producer.flush()                   # envoie ce qui reste en attente avant de quitter


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(reconnect=5)        # se reconnecte tout seul après 5 s en cas de coupure