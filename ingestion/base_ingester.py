from abc import ABC, abstractmethod
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class BaseIngester(ABC):
    @abstractmethod
    def ingest(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def validate(self, data: pd.DataFrame) -> bool:
        pass

    
    def get_info(self) -> str:

        return f"{self.__class__.__name__} | config: {self.__dict__}"
    
    def __str__(self) -> str:
        return self.get_info()
    
    def __repr__(self):
        return f"{self.__class__.__name__}(config={self.__dict__})"
    
