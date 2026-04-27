import asyncio
import ssl
import json
import uuid
import random # Для генерації температури

from rsocket.rsocket_client import RSocketClient
from rsocket.payload import Payload
from rsocket.transports.tcp import TransportTCP
from rsocket.request_handler import BaseRequestHandler
from rsocket.helpers import single_transport_provider, create_future

# Імпорти для створення потоку даних
from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription


class SensorDataPublisher(Publisher, Subscription):
    def __init__(self, node_id):
        self.node_id = node_id
        self.subscriber = None
        self._is_running = False
        self._task = None

    def subscribe(self, subscriber: Subscriber):
        # Зберігаємо підписника (того, хто буде слухати наші дані)
        self.subscriber = subscriber
        # Кажемо підписнику що підписка готова"
        subscriber.on_subscribe(self)

    def request(self, n: int):
        # Цей метод викликається, коли Java каже: "Дай мені N нових значень"
        if not self._is_running:
            self._is_running = True
            print(f"[{self.node_id}] Потік запущено: генеруємо {n} значень...")
            # Запускаємо фоновий процес генерації даних
            self._task = asyncio.create_task(self._generate_data())

    def cancel(self):
        # Цей метод викликається, коли Java відключається або зупиняє потік
        print(f"[{self.node_id}] Сервер відключився, зупиняємо датчик.")
        self._is_running = False
        if self._task:
            self._task.cancel()

    async def _generate_data(self):
        try:
            while self._is_running:
                # 1. Генеруємо випадкову температуру
                temp = round(random.uniform(20.0, 30.0), 2)

                # 2. Пакуємо її в Payload RSocket
                payload = Payload(data=str(temp).encode('utf-8'))

                # 3. Відправляємо підписнику (Java)
                self.subscriber.on_next(payload)

                # 4. Спимо 0.01 сек (імітуємо 100 значень на секунду)
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass  # Це нормально, потік просто зупинили
        except Exception as e:
            self.subscriber.on_error(e)


class PythonNodeHandler(BaseRequestHandler):
    def __init__(self, node_id):
        self.node_id = node_id

    # ---------------------------------------------------------
    # КАНАЛ КЕРУВАННЯ (Request-Response)
    # ---------------------------------------------------------
    async def request_response(self, payload: Payload):
        command = payload.data.decode('utf-8')

        if command == "LOAD_SCHEMA":
            schema_json = payload.metadata.decode('utf-8')
            print(f"\n[{self.node_id}] Отримано команду на керуючому каналі: {command}")

            try:
                schema = json.loads(schema_json)
                print(f"[{self.node_id}] Успішно розпарсено JSON схему!")
                print(f"   -> ID: {schema.get('schema_id')}")
                print(f"   -> Операція: {schema.get('operation')}")
                print(f"   -> Джерело: {schema.get('input_source')}")

                return create_future(Payload(b"SCHEMA_LOADED_OK"))
            except Exception as e:
                print(f"[{self.node_id}] Помилка парсингу JSON: {e}")
                return create_future(Payload(b"ERROR: BAD_JSON"))

        elif command == "WHO_ARE_YOU":
            return create_future(Payload(self.node_id.encode('utf-8')))

        elif command == "RESET":
            print(f"\n[{self.node_id}] Отримано команду RESET: Очищення конфігурації...")
            # Тут Зроблю щось на зразок self.current_schema = None
            return create_future(Payload(b"RESET_OK"))

        return create_future(Payload(b"ERROR: UNKNOWN_COMMAND"))

    # ---------------------------------------------------------
    # КАНАЛ ДАНИХ (Request-Stream)
    # ---------------------------------------------------------
    async def request_stream(self, payload: Payload) -> Publisher:
        command = payload.data.decode('utf-8')

        if command == "START_SENSOR":
            print(f"\n[{self.node_id}] Отримано команду на запуск потоку: {command}")
            # Створюємо і повертаємо наш генератор даних!
            return SensorDataPublisher(self.node_id)

        # Якщо прийшла невідома команда, повертаємо стандартну помилку
        return super().request_stream(payload)


async def main():
    # Генеруємо унікальний ID та задаємо порти
    node_id = f"NodePy-{str(uuid.uuid4())[:8]}"
    my_port = 8005
    seed_port = 7000

    print(f"=== Python Mesh Node [{node_id}] ===")
    print(f"Підключення до Java Seed Node (Порт {seed_port})...")

    # 2. Налаштовуємо SSL (ігноруємо самопідписаний сертифікат для тестів)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # 3. Встановлюємо з'єднання
    try:
        connection = await asyncio.open_connection('localhost', seed_port, ssl=ssl_context)
        transport = TransportTCP(*connection)

        # Створюємо Setup Payload
        setup_payload = Payload(
            data=node_id.encode('utf-8'),
            metadata=str(my_port).encode('utf-8')
        )

        # 4. Запускаємо RSocket клієнт (ОБГОРТАЄМО ТРАНСПОРТ У PROVIDER!)
        async with RSocketClient(single_transport_provider(transport),
                                 setup_payload=setup_payload,
                                 handler_factory=lambda: PythonNodeHandler(node_id)) as client:

            print("[OK] SSL та RSocket з'єднання встановлено!")

            # 5. Відправляємо GREETING
            greeting_payload = Payload(
                data=b"GREETING",
                metadata=str(my_port).encode('utf-8')
            )
            await client.fire_and_forget(greeting_payload)

            print("Очікування команд від сервера...\n")
            await asyncio.Future()  # Нескінченне очікування

    except Exception as e:
        print(f"[ERROR] Помилка підключення: {e}")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[OK] Роботу ноди успішно завершено користувачем (Ctrl+C).")