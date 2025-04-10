from pathlib import Path
from extraction import Extraction
from transform import Transform
from load import Load

base_dir = Path(__file__).parent
file_path = base_dir / 'src' / 'flights.json'


ext = Extraction(file_path)
# Extract the data from the JSON file
data = ext.extract()
# Create a Transform object
transform = Transform(data)
# Transform FL_DATE to datetime format
transform.transform_datetime("FL_DATE")
# Transform DEP_TIME and ARR_TIME to time format
transform.transform_time("DEP_TIME")
transform.transform_time("ARR_TIME")
# Add a new column with the flight datetime
transform.add_flight_datetime_column(
    "FL_DATE", "DEP_TIME", "flight_datetime"
    )
# Add a new column with the average speed of the flight
transform.add_average_speed_column(
    "DISTANCE", "AIR_TIME", "average_speed"
    )
# Add a new column with the total delay of the flight
transform.add_total_delay_column(
    "DEP_DELAY", "ARR_DELAY", "total_delay"
    )
# Add a new column with the on time status of the flight
transform.add_on_time_column("on_time", "total_delay")
# Add a new column with the day of the week
transform.add_day_of_week_column("FL_DATE", "day_of_week")
# Add a new column with the day of the month
transform.add_day_of_month_column("FL_DATE", "day_of_month")
# Add a new column with the month
transform.add_month_of_year_column("FL_DATE", "month")
# Rename the columns
transform.rename_columns()
# Create a Load object
load = Load(transform.data_source)
# Convert the data to a pandas DataFrame
load.dict_to_pandas()
# Save the data to a parquet file
load.df_to_parquet(str(base_dir / 'output' / 'flights.parquet'))
