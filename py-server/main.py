from config import NodeConfig
from streams.data_source import MockTemperatureSource
from core.node_api import MeshSensorAPI

if __name__ == '__main__':
    # 1. Задаємо налаштування
    config = NodeConfig(host='192.168.50.11', port=7000)

    # 2. Обираємо джерело даних (може бути що завгодно, що імплементує IDataSource)
    sensor_source = MockTemperatureSource()

    # 3. Створюємо та запускаємо ноду
    node = MeshSensorAPI(config=config, data_source=sensor_source)
    node.start()