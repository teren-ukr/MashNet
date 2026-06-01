import asyncio
import ssl
from rsocket.rsocket_client import RSocketClient
from rsocket.payload import Payload
from rsocket.transports.tcp import TransportTCP
from rsocket.helpers import single_transport_provider

from config import NodeConfig
from network.rsocket_handler import PythonNodeHandler
from streams.data_source import IDataSource

import os


class MeshSensorAPI:
    # -----------------------------------------------------------------------------------------------------------------
    def __init__(self, config: NodeConfig, data_source: IDataSource):
        self.config = config
        self.data_source = data_source

    # -----------------------------------------------------------------------------------------------------------------
    def _create_ssl_context(self) -> ssl.SSLContext:
        print("[SECURITY] Ініціалізація mTLS контексту...")

        # 1. Визначаємо АБСОЛЮТНИЙ шлях до кореневої папки проєкту
        # __file__ вказує на core/node_api.py. Ми піднімаємось на один рівень вгору.
        current_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.dirname(current_dir)

        # 2. Формуємо точні шляхи до сертифікатів
        ca_path = os.path.join(base_dir, "certs", "ca.crt")
        cert_path = os.path.join(base_dir, "certs", "sensor1.crt")
        key_path = os.path.join(base_dir, "certs", "sensor1.key")

        # Додамо перевірку для зручності (якщо файлу нема, воно скаже де саме шукало)
        if not os.path.exists(ca_path):
            raise FileNotFoundError(f"Файл не знайдено за шляхом: {ca_path}")

        # Створюємо контекст
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

        # Завантажуємо сертифікати за абсолютними шляхами
        context.load_verify_locations(cafile=ca_path)
        context.load_cert_chain(certfile=cert_path, keyfile=key_path)

        # Налаштування суворої перевірки
        context.check_hostname = False
        context.verify_mode = ssl.CERT_REQUIRED

        return context

    # -----------------------------------------------------------------------------------------------------------------
    async def _run(self):
        print(f"=== Ініціалізація Mesh Node [{self.config.node_id}] ===")

        try:
            ssl_ctx = self._create_ssl_context()
        except Exception as e:
            print(f"[FATAL] Не вдалося завантажити сертифікати: {e}")
            return  # Якщо немає сертифікатів, працювати неможливо

        # Головний цикл живучості (Resilience Loop)
        while True:
            try:
                print(f"\n[NETWORK] Спроба підключення до {self.config.host}:{self.config.port}...")

                # TCP + mTLS Handshake
                connection = await asyncio.open_connection(self.config.host, self.config.port, ssl=ssl_ctx)
                transport = TransportTCP(*connection)

                setup_payload = Payload(
                    data=self.config.node_id.encode('utf-8'),
                    metadata=str(self.config.my_port).encode('utf-8')
                )

                # RSocket Handshake
                async with RSocketClient(
                        single_transport_provider(transport),
                        setup_payload=setup_payload,
                        handler_factory=lambda: PythonNodeHandler(self.config.node_id, self.data_source)
                ) as client:

                    print("[OK] Захищене mTLS та RSocket з'єднання встановлено!")

                    await client.fire_and_forget(Payload(b"GREETING", str(self.config.my_port).encode('utf-8')))
                    print("Очікування команд від сервера...")

                    # Чекаємо безкінечно, поки з'єднання живе
                    await asyncio.Future()

            except ConnectionRefusedError:
                print(f"[WARN] Сервер недоступний. Повторна спроба через 5 секунд...")
            except ssl.SSLError as e:
                print(f"[FATAL] Помилка mTLS (можливо, чужий сервер або відкликаний сертифікат): {e}")
                # При криптографічній помилці краще зупинитись, щоб не скомпрометувати себе
                break
            except Exception as e:
                print(f"[ERROR] Зв'язок розірвано: {e}. Перепідключення через 5 секунд...")

            # Пауза перед наступною спробою, щоб не перевантажувати процесор (Backoff)
            await asyncio.sleep(5)

    # -----------------------------------------------------------------------------------------------------------------
    def start(self):
        """Головний публічний метод для запуску ноди."""
        try:
            asyncio.run(self._run())
        except KeyboardInterrupt:
            print("\n[OK] Роботу ноди завершено (Ctrl+C).")
        except Exception as e:
            print(f"\n[ERROR] Мережева помилка: {e}")