import numba
import numpy as np
import polars as pl
from aquarium_adventures.base import BaseAquariumAnalyzer
from aquarium_adventures.transformations import AquariumTransformer


class AquariumHPCComputations(BaseAquariumAnalyzer):
    def __init__(self):
        pass

    def analyze_data(self, df):
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
    n = len(pH_vals)

    if n == 0:
        return 0.0

    stress_sum = 0.0

    for i in numba.prange(0, n):
        for j in numba.prange(0, n):
            if (
            np.isnan(pH_vals[i]) or np.isnan(pH_vals[j]) or
            np.isnan(temp_vals[i]) or np.isnan(temp_vals[j]) or
            np.isnan(quantity_vals[i]) or np.isnan(quantity_vals[j])
            ):
                continue
            pH_dev = abs(pH_vals[i] - pH_vals[j])
            t_dev = abs(temp_vals[i] - temp_vals[j]) * 2
            quantity_factor = (500.0 / (quantity_vals[i])) + (500.0 / (quantity_vals[j]))
            stress_sum += (pH_dev + t_dev) * quantity_factor
    final_stress = stress_sum / (n * n)
    
    return final_stress


