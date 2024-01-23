from abc import ABC, abstractclassmethod
import pandas as pd

class Oppositions(ABC):
    @abstractclassmethod
    def build_oppositions(self) -> pd.DataFrame:
        pass
    
    @abstractclassmethod
    def build_matrix(self) -> pd.DataFrame:
        pass