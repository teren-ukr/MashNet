import random
from abc import ABC, abstractmethod

class IDataSource(ABC):
    """Інтерфейс для будь-якого джерела даних сенсора."""
    @abstractmethod
    def get_data(self) -> bytes:
        pass


class MockTemperatureSource(IDataSource):
    """Конкретна реалізація джерела даних (генератор температури)."""
    def get_data(self) -> bytes:
        temp = round(random.uniform(20.0, 30.0), 2)
        return str(temp).encode('utf-8')