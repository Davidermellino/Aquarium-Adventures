import polars as pl
from aquarium_adventures.transformations import AquariumTransformer
from aquarium_adventures.computations import AquariumHPCComputations
from aquarium_adventures.pipeline import AquariumPipeline


def run_full_pipeline(
    input_csv, tank_info_csv=None, output_csv=None, project_name="AquariumProject"
):
    """
    Runs the full aquarium data processing pipeline.

    Args:
        input_csv (str): Path to the input sensor data CSV file.
        tank_info_csv (str, optional): Path to the tank info CSV file. Defaults to None.
        output_csv (str, optional): Path to save the output CSV file. Defaults to None.
        project_name (str, optional): Name of the Weights & Biases project. Defaults to "AquariumProject".

    Returns:
        pl.DataFrame: The final processed DataFrame.
    """

    # LOAD DATA
    sensors_df = pl.read_csv(input_csv, separator="\t")

    tank_info_df_fish_species_split = None
    if tank_info_csv:
        tank_info_df_fish_species_split = pl.read_csv(
            tank_info_csv, separator="\t"
        ).with_columns(pl.col("fish_species").str.split(","))

    # INITIALIZE TRANSFORMER AND COMPUTATIONS
    transformer = AquariumTransformer(tank_info_df_fish_species_split)
    hpc_computations = AquariumHPCComputations()

    # RUN PIPELINE
    pipeline = AquariumPipeline(
        analyzers=[transformer, hpc_computations], project_name=project_name
    )
    result_df = pipeline.run(sensors_df, log_to_wandb=True)

    if output_csv:
        result_df.write_csv(output_csv, separator="\t")

    return result_df
