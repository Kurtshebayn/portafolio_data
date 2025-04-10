import json


class Extraction:
    """
    This class is responsible for extracting data from a JSON newlines
    delimited file and converting it into a list of dictionaries.
    """

    def __init__(self, file_path):
        """
        Initializes the Extraction class with the file path.

        :param file_path: The path to the JSON newlines delimited file.
        """
        self.file_path = file_path

    def extract(self):
        """
        Extracts data from the JSON newlines delimited file and converts it
        into a list of dictionaries.

        :return: A list of dictionaries representing the data in the JSON
        newlines delimited file.
        """
        data = []
        with open(self.file_path, 'r') as f:
            for line in f:
                data.append(json.loads(line))
        return data
