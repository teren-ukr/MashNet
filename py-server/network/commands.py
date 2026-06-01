import json
from rsocket.payload import Payload

def handle_load_schema(payload: Payload, node_id: str) -> Payload:
    schema_json = payload.metadata.decode('utf-8')
    try:
        schema = json.loads(schema_json)
        print(f"[{node_id}] Схему завантажено: {schema.get('operation')} -> {schema.get('output_sink')}")
        return Payload(b"SCHEMA_LOADED_OK")
    except Exception as e:
        print(f"[{node_id}] Помилка JSON: {e}")
        return Payload(b"ERROR: BAD_JSON")


def handle_reset(node_id: str) -> Payload:
    print(f"[{node_id}] Очищення конфігурації (RESET)...")
    return Payload(b"RESET_OK")

