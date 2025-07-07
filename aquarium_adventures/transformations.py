import polars as pl
from aquarium_adventures.base import BaseAquariumAnalyzer
from joblib import Parallel, delayed


class AquariumTransformer(BaseAquariumAnalyzer):
    STANDARD_TEMPERATURE = 26.0

    def __init__(self, tank_info_df_fish_species_split=None):
        self.tank_info_df_fish_species_split = tank_info_df_fish_species_split
    
    def analyze_data(self, sensors_df: pl.DataFrame) -> pl.DataFrame:
        """
        Analyzes the sensor data and adds various calculated columns.
        """

        sensors_df = self.add_num_readings_per_tank(sensors_df)
        sensors_df = self.add_avg_ph_per_tank(sensors_df)
        sensors_df = self.add_temperature_deviation(sensors_df)
        
        if self.tank_info_df_fish_species_split is not None:
            sensors_df = self.add_num_readings_per_fish_species(sensors_df)

        return sensors_df

    def add_num_readings_per_tank(self, sensors_df: pl.DataFrame) -> pl.DataFrame:
        """
        Adds a column with the number of readings per tank.
        """
        tank_num_readings_table = sensors_df.group_by("tank_id").agg(
            pl.len().alias("tank_num_readings")
        )
        return sensors_df.join(tank_num_readings_table, on="tank_id")

    def add_avg_ph_per_tank(self, sensors_df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculates the average pH per tank and adds it to the DataFrame
        """
        avg_ph = sensors_df.group_by("tank_id").agg(
            pl.col("pH").mean().alias("avg_pH_per_tank")
        )
        return sensors_df.join(avg_ph, on="tank_id")

    def add_temperature_deviation(self, sensors_df: pl.DataFrame) -> pl.DataFrame:
        """
        Adds a column with the temperature deviation from the standard temperature.
        """

        if "quantity_liters" in sensors_df.columns:
            return sensors_df.with_columns(
                (
                    abs(pl.col("temp") - self.STANDARD_TEMPERATURE)
                    * 1000
                    / pl.col("quantity_liters")
                ).alias("temperature_deviation_scaled")
            )

        else:
            return sensors_df.with_columns(
                (abs(pl.col("temp") - self.STANDARD_TEMPERATURE)).alias(
                    "temperature_deviation"
                )
            )

    def add_num_readings_per_fish_species(
        self, sensors_df: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Adds a column with the number of readings per fish species.
        """
        # Check if tank_info data exists and has fish_species column
        if self.tank_info_df_fish_species_split is None:
            raise AttributeError("tank_info_df_fish_species_split is not available")

        if "fish_species" not in self.tank_info_df_fish_species_split.columns:
            raise ValueError(
                "fish_species column not found in tank_info_df_fish_species_split"
            )

        # Check if tank_num_readings column exists, if not add it
        if "tank_num_readings" not in sensors_df.columns:
            sensors_df = self.add_num_readings_per_tank(sensors_df)

        # Explode fish_species if it's a list column
        tank_info_exploded = self.tank_info_df_fish_species_split
        if self.tank_info_df_fish_species_split["fish_species"].dtype == pl.List:
            tank_info_exploded = self.tank_info_df_fish_species_split.explode(
                "fish_species"
            )

        # Function to process a chunk of data
        def process_chunk(chunk):
            chunk_readings = (
                chunk.join(
                    sensors_df.select(["tank_id", "tank_num_readings"]).unique(),
                    on="tank_id",
                )
                .group_by("fish_species")
                .agg(pl.col("tank_num_readings").sum().alias("fish_species_num_readings"))
            )
            return chunk_readings

        # Split tank_info_exploded into chunks
        chunks = tank_info_exploded.partition_by("fish_species", as_dict=False)

        # Process chunks in parallel
        fish_species_readings_list = Parallel(n_jobs=-1)(
            delayed(process_chunk)(chunk) for chunk in chunks
        )

        # Combine results
        fish_species_readings = pl.concat(fish_species_readings_list)

        # Join back to sensors_df via tank_info
        sensors_with_species = sensors_df.join(tank_info_exploded, on="tank_id")
        result = sensors_with_species.join(fish_species_readings, on="fish_species")

        return result
