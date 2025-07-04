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
        quantity_vals = df["capacity_liters"].to_numpy()

        stress_score = pairwise_stress_function(pH_vals, temp_vals, quantity_vals)

        result_df = df.with_columns(pl.lit(stress_score).alias("stress_score"))

        return result_df


@numba.njit()
def pairwise_stress_function(
    pH_vals: np.array, temp_vals: np.array, quantity_vals: np.array
) -> float:
    n = len(pH_vals)

    if n == 0:
        return 0.0

    stress_sum = 0.0

    for i in range(0, n):
        for j in range(0, n):
            pH_dev = abs(pH_vals[i] - pH_vals[j])
            t_dev = abs(temp_vals[i] - temp_vals[j]) * 2
            quantity_factor = (500.0 / quantity_vals[i]) + (500.0 / quantity_vals[j])
            stress_sum += (pH_dev + t_dev) * quantity_factor

    final_stress = stress_sum / (n * n)

    return final_stress


if __name__ == "__main__":
    tank_info_df_fish_species_split = pl.read_csv(
        "data/tank_info_sample.tsv", separator="\t"
    ).with_columns(pl.col("fish_species").str.split(","))
    sensor_df = pl.read_csv("data/sensors_sample.tsv", separator="\t")
    trasformer = AquariumTransformer(tank_info_df_fish_species_split)
    out_df = trasformer.add_num_readings_per_fish_species(sensors_df=sensor_df)
    print(out_df.columns)
