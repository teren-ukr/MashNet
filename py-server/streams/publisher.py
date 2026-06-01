import asyncio
from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.payload import Payload
from .data_source import IDataSource

class SensorDataPublisher(Publisher, Subscription):

    def __init__(self, node_id: str, data_source: IDataSource):
        self.node_id = node_id
        self.data_source = data_source
        self.subscriber: Subscriber | None = None
        self._is_running = False
        self._task = None


    def subscribe(self, subscriber: Subscriber):
        self.subscriber = subscriber
        subscriber.on_subscribe(self)

    def request(self, n: int):
        if not self._is_running:
            self._is_running = True
            print(f"[{self.node_id}] Потік запущено. Відправка даних...")
            self._task = asyncio.create_task(self._generate_data())

    def cancel(self):
        print(f"[{self.node_id}] Потік зупинено.")
        self._is_running = False
        if self._task:
            self._task.cancel()


    async def _generate_data(self):
        try:
            while self._is_running and self.subscriber:
                # Отримуємо дані з незалежного джерела (DIP)
                raw_bytes = self.data_source.get_data()
                self.subscriber.on_next(Payload(raw_bytes))
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if self.subscriber:
                self.subscriber.on_error(e)