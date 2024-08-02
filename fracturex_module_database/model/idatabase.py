from abc import ABC, abstractmethod
from typing import Any
from pydantic import (
    StrictStr, 
    PositiveInt
)

class IDatabase(ABC):
    @abstractmethod
    def get_connection(self):
        pass

    @abstractmethod
    def select(self, id: PositiveInt | None = None) -> list[dict]:
        pass
    
    @abstractmethod
    def insert(self, data: dict[StrictStr, Any]) -> bool:
        pass

    @abstractmethod
    def update(self, data: dict[StrictStr, Any], id: PositiveInt) -> bool:
        pass

    @abstractmethod
    def delete(self, id: PositiveInt) -> bool:
        pass
