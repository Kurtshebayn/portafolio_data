from datetime import datetime
from dateutil.parser import parse


class Transform:

    """
    Class to transform data from a source into a pandas DataFrame.
    """

    def __init__(self, data_source):
        self.data_source = data_source
        self.rename_map = {
            "FL_DATE": "flight_date",
            "DEP_DELAY": "departure_delay",
            "ARR_DELAY": "arrival_delay",
            "AIR_TIME": "air_time_minutes",
            "DISTANCE": "distance_miles",
            "DEP_TIME": "departure_time_decimal",
            "ARR_TIME": "arrival_time_decimal",
            }

    def transform_datetime(self, key):
        """
        Transforms the date data from a string format (YYYY-MM-DD) to a date
        object. The date is represented as a string in the format YYYY-MM-DD.
        """
        # Transform the data
        for record in self.data_source:
            date_str = record.get(key)
            if date_str:
                try:
                    record[key] = parse(date_str).date()
                except ValueError:
                    print(f"Error parsing date: {date_str}")
                    record[key] = None

    def transform_time(self, key):
        """
        Transforms the time data from a decimal format to a time format
        (HH:MM).
        The time is represented as a decimal number where the integer part
        represents the hours and the decimal part represents the minutes.
        """
        # Transform the data
        for record in self.data_source:
            time = record.get(key)
            if time:
                try:
                    hours = int(time)
                    minutes = int((time - hours) * 60)
                    time_str = f"{hours:02d}:{minutes:02d}"
                    time_format = "%H:%M"
                    record[key] = datetime.strptime(
                        time_str, time_format
                        ).time()
                except ValueError:
                    print(f"Error parsing time: {time}")
                    record[key] = None

    def add_flight_datetime_column(self, date_key, time_key, new_key):
        """
        Adds a new column to the data with the flight datetime, combining the
        date and time columns. The new column will be in the format
        YYYY-MM-DD HH:MM.
        """
        # Add a new column with the flight datetime
        for record in self.data_source:
            date = record.get(date_key)
            time = record.get(time_key)
            if date and time:
                try:
                    flight_datetime_str = f"{date} {time}"
                    flight_datetime = datetime.strptime(
                        flight_datetime_str, "%Y-%m-%d %H:%M:%S"
                    )
                    record[new_key] = flight_datetime
                except ValueError:
                    print(f"Error parsing datetime: {flight_datetime_str}")
                    record[new_key] = None

    def add_average_speed_column(self, distance_key, air_time_key, new_key):
        """
        Adds a new column to the data with the average speed of the flight.
        The average speed is calculated as distance / time.
        """
        # Add a new column with the average speed
        for record in self.data_source:
            distance = record.get(distance_key)
            time = record.get(air_time_key)
            if distance and time:
                try:
                    time = int(time) / 60
                    average_speed = int(distance) / time
                    record[new_key] = average_speed
                except ZeroDivisionError:
                    print("Error calculating average speed: division by zero")
                    record[new_key] = None

    def add_total_delay_column(self, dep_delay_keys, arr_delay_keys, new_key):
        """
        Adds a new column to the data with the total delay of the flight.
        The total delay is calculated as the sum of the arrive and departure
        delays in the specified keys.
        """
        # Add a new column with the total delay
        for record in self.data_source:
            if record[dep_delay_keys] is None:
                record[dep_delay_keys] = 0
            if record[arr_delay_keys] is None:
                record[arr_delay_keys] = 0
            try:
                record[new_key] = (
                    int(record[dep_delay_keys]) + int(record[arr_delay_keys])
                )
            except TypeError:
                print(
                    f"Error calculating total delay: "
                    f"{record[dep_delay_keys]} + "
                    f"{record[arr_delay_keys]}"
                )
                record[new_key] = None

    def add_on_time_column(self, new_key, delay_key):
        """
        Adds a new column to the data with the on time status of the flight.
        The on time status is calculated as 1 if the flight is on time, and 0
        otherwise.
        """
        # Add a new column with the on time status
        for record in self.data_source:
            if record[delay_key] is None:
                record[delay_key] = 0
            try:
                if record[delay_key] > 0:
                    record[new_key] = 0
                else:
                    record[new_key] = 1
            except TypeError:
                print(f"Error calculating on time status: {record[delay_key]}")
                record[new_key] = None

    def add_day_of_week_column(self, date_key, new_key):
        """
        Adds a new column to the data with the day of the week of the flight.
        The day of the week is represented as an integer from 0 (Monday) to 6
        (Sunday).
        """
        # Add a new column with the day of the week
        for record in self.data_source:
            date = record.get(date_key)
            if date:
                try:
                    day_of_week = date.weekday()
                    record[new_key] = day_of_week
                except AttributeError:
                    print(f"Error getting day of week: {date}")
                    record[new_key] = None

    def add_day_of_month_column(self, date_key, new_key):
        """
        Adds a new column to the data with the day of the month of the flight.
        The day of the month is represented as an integer from 1 to 31.
        """
        # Add a new column with the day of the month
        for record in self.data_source:
            date = record.get(date_key)
            if date:
                try:
                    day_of_month = date.day
                    record[new_key] = day_of_month
                except AttributeError:
                    print(f"Error getting day of month: {date}")
                    record[new_key] = None

    def add_month_of_year_column(self, date_key, new_key):
        """
        Adds a new column to the data with the month of the year of the flight.
        The month of the year is represented as an integer from 1 (January) to
        12 (December).
        """
        # Add a new column with the month of the year
        for record in self.data_source:
            date = record.get(date_key)
            if date:
                try:
                    month_of_year = date.month
                    record[new_key] = month_of_year
                except AttributeError:
                    print(f"Error getting month of year: {date}")
                    record[new_key] = None

    def rename_columns(self):
        """
        Renames the columns in the data according to the specified mapping.
        The mapping is a dictionary where the keys are the old column names and
        the values are the new column names.
        """
        # Rename the columns
        for record in self.data_source:
            for old, new in self.rename_map.items():
                if old in record:
                    record[new] = record.pop(old)
