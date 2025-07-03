import polars as pl
from aquarium_adventures.base import BaseAquariumAnalyzer

class AquariumTransformer(BaseAquariumAnalyzer):
    
    def __init__(self, tank_info_df_fish_species_split=None):
        pass
    
    def analyze_data(self):
        pass
    
    def add_num_readings_per_tank(self, sensors_df):
        """
        Adds a column with the number of readings per tank.
        """
        tank_num_readings_table = sensors_df.group_by('tank_id').agg(pl.len().alias('tank_num_readings'))
        return pl.DataFrame.join(sensors_df, tank_num_readings_table, on='tank_id') 

    def add_avg_ph_per_tank(self, df: pl.DataFrame) -> pl.DataFrame:
        """ 
        Calculates the average pH per tank and adds it to the DataFrame 
        """

        avg_ph = df.group_by("tank_id").agg(pl.col("pH").mean().alias("avg_pH"))
        return df.join(avg_ph, on="tank_id") 