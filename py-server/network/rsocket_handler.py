from typing import Awaitable

from rsocket.request_handler import BaseRequestHandler
from rsocket.payload import Payload
from rsocket.helpers import create_future
from reactivestreams.publisher import Publisher

from streams.publisher import SensorDataPublisher
from streams.data_source import IDataSource
from . import commands

class PythonNodeHandler(BaseRequestHandler):
    # -----------------------------------------------------------------------------------------------------------------
    def __init__(self, node_id: str, data_source: IDataSource):
        self.node_id = node_id
        self.data_source = data_source

    # -----------------------------------------------------------------------------------------------------------------
    async def request_response(self, payload: Payload):
        command = payload.data.decode('utf-8')

        if command == "LOAD_SCHEMA":
            result = commands.handle_load_schema(payload, self.node_id)
        elif command == "WHO_ARE_YOU":
            result = Payload(self.node_id.encode("utf-8"))
        elif command == "RESET":
            result = commands.handle_reset(self.node_id)
        else:
            result = Payload(b"ERROR: UNKNOWN_COMMAND")

        return create_future(result)

    # -----------------------------------------------------------------------------------------------------------------
    async def request_stream(self, payload: Payload) -> Publisher:
        command = payload.data.decode('utf-8')

        if command == "START_SENSOR":
            print(f"\n[{self.node_id}] Відкриття потоку даних...")
            # Передаємо джерело даних у видавця
            return SensorDataPublisher(self.node_id, self.data_source)

        return await super().request_stream(payload)
