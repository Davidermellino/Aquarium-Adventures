import numba
import numpy as np
import polars as pl
from aquarium_adventures.base import BaseAquariumAnalyzer
from aquarium_adventures.transformations import AquariumTransformer


class AquariumHPCComputations(BaseAquariumAnalyzer):
    def __init__(self):
        pass

    def analyze_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Analyze the aquarium data to compute stress score based on pH, temperature, and quantity of water.

        Args:
            df (pl.DataFrame): The input DataFrame containing columns "pH", "temp", and "quantity_liters".
        Returns:
            pl.DataFrame: A DataFrame with an additional column "stress_score" containing the computed stress score.
        """

        pH_vals = df["pH"].to_numpy()
        temp_vals = df["temp"].to_numpy()
        quantity_vals = df["quantity_liters"].to_numpy()

        stress_score = pairwise_stress_function(pH_vals, temp_vals, quantity_vals)

        result_df = df.with_columns(pl.lit(stress_score).alias("stress_score"))

        return result_df


@numba.njit(parallel=True)
def pairwise_stress_function(
    pH_vals: np.array, temp_vals: np.array, quantity_vals: np.array
) -> float:
    """
    Compute the stress score based on pairwise differences in pH, temperature, and quantity of water.

    Args:
        pH_vals (np.array): Array of pH values.
        temp_vals (np.array): Array of temperature values.
        quantity_vals (np.array): Array of water quantities in liters.
    Returns:
        float: The computed stress score.

    """

    n = len(pH_vals)

    if n == 0:
        return 0.0

    stress_sum = 0.0

    for i in numba.prange(0, n):
        for j in numba.prange(0, n):
            if (
                np.isnan(pH_vals[i])
                or np.isnan(pH_vals[j])
                or np.isnan(temp_vals[i])
                or np.isnan(temp_vals[j])
                or np.isnan(quantity_vals[i])
                or np.isnan(quantity_vals[j])
            ):
                continue
            pH_dev = abs(pH_vals[i] - pH_vals[j])
            t_dev = abs(temp_vals[i] - temp_vals[j]) * 2
            quantity_factor = (500.0 / (quantity_vals[i])) + (
                500.0 / (quantity_vals[j])
            )
            stress_sum += (pH_dev + t_dev) * quantity_factor
    final_stress = stress_sum / (n * n)

    return final_stress
