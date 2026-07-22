from google.cloud import pubsub_v1
import json

# Inicializa el cliente de Pub/Sub una vez
publisher = pubsub_v1.PublisherClient()
project_id = "project-9d01163c-da4a-4e93-98e"
topic_id = "registros"
topic_path = publisher.topic_path(project_id, topic_id)


def main(request):
    try:
        # Obtener el JSON desde el body del request
        data = request.get_json()
        if not data:
            return "Solicitud sin JSON válido", 400

        # Convertir a bytes para Pub/Sub
        message_bytes = json.dumps(data).encode("utf-8")

        # Publicar mensaje en Pub/Sub
        # Si es un array, publicar cada uno individualmente
        if isinstance(data, list):
            for item in data:
                message_bytes = json.dumps(item).encode("utf-8")
                future = publisher.publish(topic_path, message_bytes)
                future.result()  # Esperar a que se complete la publicación
        else:
            message_bytes = json.dumps(data).encode("utf-8")
            future = publisher.publish(topic_path, message_bytes)
            future.result()
        return "Completado", 200
    except Exception as e:
        return f"Error al procesar la solicitud: {e}", 500
