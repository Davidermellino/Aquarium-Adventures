import wandb


class AquariumPipeline:
    def __init__(self, analyzers, project_name=None):
        self.analyzers = analyzers
        self.project_name = project_name

    def run(self, sensors_df, log_to_wandb=False):
        """
        Executes the pipeline on the provided sensor data.

        Args:
            sensors_df (pl.DataFrame): The input sensor data.
            log_to_wandb (bool): Whether to log results to Weights & Biases.
        Returns:
            pl.DataFrame: The processed DataFrame after all analyzers have been applied.
        """

        transformed_df = self.analyzers[0].analyze_data(sensors_df)

        out_df = self.analyzers[1].analyze_data(transformed_df)

        if log_to_wandb:
            self.log_to_wandb(out_df)

        return out_df

    def log_to_wandb(self, df):
        """
        Logs the results to Weights & Biases.

        Args:
            df (pl.DataFrame): The DataFrame containing the results to log.
        Returns:
            None
        """

        wandb.init(project=self.project_name)
        wandb.log({"stress_score": df["stress_score"]})
