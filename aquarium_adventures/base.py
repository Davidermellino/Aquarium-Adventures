from abc import ABC, abstractmethod
import polars as pl


class BaseAquariumAnalyzer(ABC):
    @abstractmethod
    def analyze_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Analyze the aquarium data and return a DataFrame with the results.


        Args:
            data (pl.DataFrame): The input aquarium data to analyze.
        Returns:
            pl.DataFrame: A DataFrame containing the analysis results.

        """
        pass
