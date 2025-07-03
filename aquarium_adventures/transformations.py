import polars as pl
from aquarium_adventures.base import BaseAquariumAnalyzer

class AquariumTransformer(BaseAquariumAnalyzer):
    STANDARD_TEMPERATURE = 26.0  
    
    def __init__(self, tank_info_df_fish_species_split=None):
        self.tank_info_df_fish_species_split = tank_info_df_fish_species_split
        
        
    def analyze_data(self, sensors_df: pl.DataFrame) -> pl.DataFrame:
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
        avg_ph = df.group_by("tank_id").agg(pl.col("pH").mean().alias("avg_pH_per_tank"))
        return df.join(avg_ph, on="tank_id") 
    
    def add_temperature_deviation(self, sensors_df: pl.DataFrame) -> pl.DataFrame:
        """
        Adds a column with the temperature deviation from the standard temperature.
        """
        
        if 'quantity_liters' in sensors_df.columns:
        
            return sensors_df.with_columns(
                (abs(pl.col('temp') - self.STANDARD_TEMPERATURE) * 1000/pl.col('quantity_liters')).alias('temperature_deviation_scaled')
            )
        
        else: 
            
            return sensors_df.with_columns(
                (abs(pl.col('temp') - self.STANDARD_TEMPERATURE)).alias('temperature_deviation')
            )
        
        
        
        
      
   
        
