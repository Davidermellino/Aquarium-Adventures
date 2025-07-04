from abc import ABC, abstractmethod
import polars as pl


class BaseAquariumAnalyzer(ABC):
    @abstractmethod
    def analyze_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Analyze the aquarium data and return a DataFrame with the results.

        :param data: A Polars DataFrame containing aquarium data.
        :return: A Polars DataFrame with the analysis results.
        """
        pass
