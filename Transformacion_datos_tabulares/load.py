import pandas as pd


class Load:
    """
    Class to load data from a source and transform it into a pandas DataFrame.
    """

    def __init__(self, data_source):
        self.data_source = data_source
        self.df = None

    def dict_to_pandas(self):
        """
        Convert a dictionary to a pandas DataFrame then transform every column
        to the correct type.
        """
        # Convert the dictionary to a pandas DataFrame
        df = pd.DataFrame(self.data_source)

        # Convert the columns to the correct types
        df['flight_date'] = pd.to_datetime(
            df['flight_date'], format='%Y-%m-%d'
            )
        df['departure_delay'] = pd.to_numeric(
            df['departure_delay'], errors='coerce'
            )
        df['departure_delay'] = df['departure_delay'].astype(pd.Int16Dtype())
        df['arrival_delay'] = pd.to_numeric(
            df['arrival_delay'], errors='coerce'
            )
        df['arrival_delay'] = df['arrival_delay'].astype(pd.Int16Dtype())
        df['air_time_minutes'] = pd.to_numeric(
            df['air_time_minutes'], errors='coerce'
            )
        df['air_time_minutes'] = df['air_time_minutes'].astype(pd.Int16Dtype())
        df['distance_miles'] = pd.to_numeric(
            df['distance_miles'], errors='coerce'
            )
        df['distance_miles'] = df['distance_miles'].astype(pd.Int32Dtype())
        df['departure_time_decimal'] = df['departure_time_decimal'].astype(
            "string"
            )
        df['arrival_time_decimal'] = df['arrival_time_decimal'].astype(
            "string"
            )
        df['flight_datetime'] = pd.to_datetime(
            df['flight_datetime'], format='%Y-%m-%d %H:%M'
            )
        df['average_speed'] = df['average_speed'].astype("float64")
        df['total_delay'] = pd.to_numeric(df['total_delay'], errors='coerce')
        df['total_delay'] = df['total_delay'].astype(pd.Int16Dtype())
        df['on_time'] = df['on_time'].astype("bool")
        df['day_of_week'] = pd.to_numeric(df['day_of_week'], errors='coerce')
        df['day_of_week'] = df['day_of_week'].astype(pd.Int16Dtype())
        df['day_of_month'] = pd.to_numeric(df['day_of_month'], errors='coerce')
        df['day_of_month'] = df['day_of_month'].astype(pd.Int16Dtype())
        df['month'] = pd.to_numeric(df['month'], errors='coerce')
        df['month'] = df['month'].astype(pd.Int16Dtype())
        self.df = df

    def df_to_parquet(self, output_path):
        """
        Save the DataFrame to a parquet file.
        """
        # Save the DataFrame to a parquet file
        self.df.to_parquet(output_path, index=False)
        print(f"Data saved to {output_path}")
