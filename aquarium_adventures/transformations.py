from aquarium_adventures.base import BaseAquariumAnalyzer
import polars as pl

class AquariumTransformer(BaseAquariumAnalyzer):
    
    def add_avg_ph_per_tank(self, df: pl.DataFrame) -> pl.DataFrame:
        """ Calcola il pH medio per ogni vasca e lo aggiunge al DataFrame """
        
        avg_ph = df.group_by("tank_id").agg(
            pl.col("ph").mean().alias("avg_ph")
        )
        return df.join(avg_ph, on="tank_id", how="right")