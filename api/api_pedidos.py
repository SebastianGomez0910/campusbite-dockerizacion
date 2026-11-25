
import os
import json
import uuid
import threading
import time

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer, KafkaConsumer


app = Flask(__name__)
CORS(app)

@app.route("/health", methods=["GET"])
def health():
    return {
        "status": "healthy",
        "service": "api_pedidos"
    }, 200


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPICO_PEDIDO_CREADO = os.getenv("TOPICO_PEDIDO_CREADO", "pedido-creado")
TOPICO_PEDIDO_LISTO = os.getenv("TOPICO_PEDIDO_LISTO", "pedido-listo")

PUERTO = int(os.getenv("API_INTERNAL_PORT", 5000))

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "campusbite_db")
DB_USER = os.getenv("DB_USER", "campusbite_user")
DB_PASSWORD = os.getenv("DB_PASSWORD")



def obtener_conexion():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        row_factory=dict_row
    )


def inicializar_base_datos():
    while True:
        try:
            with obtener_conexion() as conexion:
                with conexion.cursor() as cursor:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS pedidos (
                            id TEXT PRIMARY KEY,
                            estudiante TEXT NOT NULL,
                            items JSONB NOT NULL,
                            total NUMERIC(10, 2) NOT NULL,
                            estado TEXT NOT NULL,
                            idempotency_key TEXT UNIQUE NOT NULL,
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        );
                    """)

            print("[API-PEDIDOS] Base de datos PostgreSQL inicializada.")
            return

        except Exception as e:
            print(f"[API-PEDIDOS] Esperando a PostgreSQL... ({e})")
            time.sleep(3)


def convertir_pedido(pedido):
    if pedido is None:
        return None

    return {
        "id": pedido["id"],
        "estudiante": pedido["estudiante"],
        "items": pedido["items"],
        "total": float(pedido["total"]),
        "estado": pedido["estado"]
    }



def crear_productor():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )

            print("[API-PEDIDOS] Conectado al productor de Kafka.")
            return producer

        except Exception as e:
            print(f"[API-PEDIDOS] Esperando a Kafka para productor... ({e})")
            time.sleep(3)


inicializar_base_datos()


producer = crear_productor()



def escuchar_pedidos_listos():
    while True:
        consumer = None

        try:
            consumer = KafkaConsumer(
                TOPICO_PEDIDO_LISTO,
                bootstrap_servers=KAFKA_BROKER,
                group_id="grupo-api-pedidos",
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )

            print("[API-PEDIDOS] Escuchando tópico 'pedido-listo'...")

            for mensaje in consumer:
                evento = mensaje.value
                pedido_id = evento.get("id")

                if not pedido_id:
                    continue

                with obtener_conexion() as conexion:
                    with conexion.cursor() as cursor:
                        cursor.execute(
                            """
                            UPDATE pedidos
                            SET estado = 'LISTO'
                            WHERE id = %s
                            """,
                            (pedido_id,)
                        )

                        if cursor.rowcount > 0:
                            print(
                                f"[API-PEDIDOS] Estado actualizado en PostgreSQL "
                                f"-> {pedido_id}: LISTO"
                            )

        except Exception as e:
            print(
                f"[API-PEDIDOS] Reconectando consumidor 'pedido-listo'... ({e})"
            )
            time.sleep(3)

        finally:
            if consumer is not None:
                consumer.close()


threading.Thread(
    target=escuchar_pedidos_listos,
    daemon=True
).start()




@app.route("/pedidos", methods=["POST"])
def crear_pedido():

    idempotency_key = request.headers.get("Idempotency-Key")

    if not idempotency_key:
        return jsonify({
            "error": "La cabecera 'Idempotency-Key' es obligatoria"
        }), 400

    datos = request.get_json(silent=True) or {}

    estudiante = datos.get("estudiante")
    items = datos.get("items")
    total = datos.get("total", 0.0)

    if not estudiante or not items:
        return jsonify({
            "error": "Los campos 'estudiante' e 'items' son obligatorios"
        }), 400

    try:
        pedido_id = f"PED-{uuid.uuid4().hex[:6].upper()}"

        with obtener_conexion() as conexion:
            with conexion.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO pedidos (
                        id,
                        estudiante,
                        items,
                        total,
                        estado,
                        idempotency_key
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING *
                    """,
                    (
                        pedido_id,
                        estudiante,
                        Jsonb(items),
                        total,
                        "EN_PREPARACION",
                        idempotency_key
                    )
                )

                pedido = cursor.fetchone()
                duplicado = pedido is None

                if duplicado:
                    cursor.execute(
                        """
                        SELECT *
                        FROM pedidos
                        WHERE idempotency_key = %s
                        """,
                        (idempotency_key,)
                    )

                    pedido = cursor.fetchone()

        pedido_respuesta = convertir_pedido(pedido)

        if duplicado:
            print(
                f"[API-PEDIDOS] Reintento detectado "
                f"(Key: {idempotency_key}). Pedido: {pedido['id']}"
            )

            return jsonify({
                "mensaje": "Pedido ya procesado previamente (idempotencia aplicada)",
                "duplicado": True,
                "pedido": pedido_respuesta
            }), 200

        producer.send(
            TOPICO_PEDIDO_CREADO,
            value=pedido_respuesta
        )

        producer.flush()

        print(
            f"[API-PEDIDOS] Nuevo pedido registrado en PostgreSQL "
            f"y publicado en Kafka -> {pedido['id']}"
        )

        return jsonify({
            "mensaje": "Pedido creado exitosamente",
            "duplicado": False,
            "pedido": pedido_respuesta
        }), 201

    except Exception as e:
        print(f"[API-PEDIDOS] Error al crear pedido: {e}")

        return jsonify({
            "error": "No se pudo procesar el pedido"
        }), 500


@app.route("/pedidos/<pedido_id>", methods=["GET"])
def consultar_pedido(pedido_id):

    try:
        with obtener_conexion() as conexion:
            with conexion.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM pedidos
                    WHERE id = %s
                    """,
                    (pedido_id,)
                )

                pedido = cursor.fetchone()

        if not pedido:
            return jsonify({
                "error": f"No se encontró el pedido {pedido_id}"
            }), 404

        return jsonify(convertir_pedido(pedido)), 200

    except Exception as e:
        print(f"[API-PEDIDOS] Error al consultar pedido: {e}")

        return jsonify({
            "error": "No se pudo consultar el pedido"
        }), 500



if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=PUERTO,
        debug=False
    )
