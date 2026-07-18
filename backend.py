import json
import os

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.management import ServiceBusAdministrationClient
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request


load_dotenv()

app = Flask(__name__)

CONNECTION_STRING = os.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING", "").strip().strip('"')

QUEUES = {
    "technical-support": ["customer", "issue", "priority"],
    "sales-chat": ["customer", "product", "contact"],
    "customer-feedback": ["customer", "feedback", "rating"],
}


def get_client():
    if not CONNECTION_STRING:
        raise RuntimeError(
            "Set AZURE_SERVICE_BUS_CONNECTION_STRING before using Azure Service Bus."
        )
    required_parts = ("Endpoint=sb://", "SharedAccessKeyName=", "SharedAccessKey=")
    if not all(part in CONNECTION_STRING for part in required_parts):
        raise RuntimeError(
            "AZURE_SERVICE_BUS_CONNECTION_STRING must be the full Service Bus connection "
            "string: Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
        )
    return ServiceBusClient.from_connection_string(CONNECTION_STRING)


def get_admin_client():
    if not CONNECTION_STRING:
        raise RuntimeError(
            "Set AZURE_SERVICE_BUS_CONNECTION_STRING before using Azure Service Bus."
        )
    return ServiceBusAdministrationClient.from_connection_string(CONNECTION_STRING)


def validate_queue(queue_name):
    if queue_name not in QUEUES:
        raise ValueError("Unknown queue selected.")


def parse_message(message):
    body = b"".join(message.body).decode("utf-8")
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return body


@app.route("/")
def index():
    return render_template("index.html", queues=QUEUES)


@app.get("/metrics")
def metrics():
    queues = []

    with get_admin_client() as admin_client:
        for queue_name in QUEUES:
            properties = admin_client.get_queue_runtime_properties(queue_name)
            queues.append(
                {
                    "name": queue_name,
                    "active": properties.active_message_count,
                    "dead_letter": properties.dead_letter_message_count,
                    "total": properties.total_message_count,
                }
            )

    return jsonify(
        {
            "queues": queues,
            "total_active": sum(queue["active"] for queue in queues),
            "total_dead_letter": sum(queue["dead_letter"] for queue in queues),
            "total_messages": sum(queue["total"] for queue in queues),
        }
    )


@app.post("/submit")
def submit_message():
    data = request.get_json(force=True)
    queue_name = data.get("queue")
    payload = data.get("payload", {})

    validate_queue(queue_name)
    missing = [field for field in QUEUES[queue_name] if not payload.get(field)]
    if missing:
        return jsonify({"error": f"Missing field(s): {', '.join(missing)}"}), 400

    with get_client() as client:
        sender = client.get_queue_sender(queue_name=queue_name)
        with sender:
            sender.send_messages(ServiceBusMessage(json.dumps(payload)))

    return jsonify({"message": f"Message sent to {queue_name}.", "payload": payload})


@app.post("/receive-one")
def receive_one():
    queue_name = request.get_json(force=True).get("queue")
    validate_queue(queue_name)

    with get_client() as client:
        receiver = client.get_queue_receiver(queue_name=queue_name, max_wait_time=5)
        with receiver:
            messages = receiver.receive_messages(max_message_count=1, max_wait_time=5)
            if not messages:
                return jsonify({"message": "No messages found.", "items": []})
            item = parse_message(messages[0])
            receiver.complete_message(messages[0])

    return jsonify({"message": "Received one message.", "items": [item]})


@app.post("/receive-all")
def receive_all():
    queue_name = request.get_json(force=True).get("queue")
    validate_queue(queue_name)
    items = []

    with get_client() as client:
        receiver = client.get_queue_receiver(queue_name=queue_name, max_wait_time=5)
        with receiver:
            while True:
                messages = receiver.receive_messages(max_message_count=10, max_wait_time=2)
                if not messages:
                    break
                for message in messages:
                    items.append(parse_message(message))
                    receiver.complete_message(message)

    return jsonify({"message": f"Received {len(items)} message(s).", "items": items})


@app.post("/peek-one")
def peek_one():
    queue_name = request.get_json(force=True).get("queue")
    validate_queue(queue_name)

    with get_client() as client:
        receiver = client.get_queue_receiver(queue_name=queue_name, max_wait_time=5)
        with receiver:
            messages = receiver.peek_messages(max_message_count=1)

    items = [parse_message(message) for message in messages]
    return jsonify({"message": f"Peeked {len(items)} message(s).", "items": items})


@app.post("/peek-all")
def peek_all():
    queue_name = request.get_json(force=True).get("queue")
    validate_queue(queue_name)

    with get_client() as client:
        receiver = client.get_queue_receiver(queue_name=queue_name, max_wait_time=5)
        with receiver:
            messages = receiver.peek_messages(max_message_count=50)

    items = [parse_message(message) for message in messages]
    return jsonify({"message": f"Peeked {len(items)} message(s).", "items": items})


@app.errorhandler(ValueError)
def handle_value_error(error):
    return jsonify({"error": str(error)}), 400


@app.errorhandler(RuntimeError)
def handle_runtime_error(error):
    return jsonify({"error": str(error)}), 500


@app.errorhandler(Exception)
def handle_unexpected_error(error):
    return jsonify({"error": str(error)}), 500


if __name__ == "__main__":
    app.run(debug=True)
