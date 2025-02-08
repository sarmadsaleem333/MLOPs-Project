import sys
import pandas as pd
import numpy as np
from typing import Optional
import pymongo
from src.configuration.mongo_db_connection import MongoDBClient
from src.constants import DATABASE_NAME, MONGODB_URL_KEY
from src.exception import MyException

class Proj1Data:
    """
    A class to export MongoDB records as a pandas DataFrame.
    """

    def __init__(self) -> None:
        """
        Initializes the MongoDB client connection.
        """
        try:
            self.mongo_client = MongoDBClient(database_name=DATABASE_NAME)
        except Exception as e:
            raise MyException(e, sys)

    def export_collection_as_dataframe(self, collection_name: str, database_name: Optional[str] = None) -> pd.DataFrame:
        """
        Exports an entire MongoDB collection as a pandas DataFrame.

        Parameters:
        ----------
        collection_name : str
            The name of the MongoDB collection to export.
        database_name : Optional[str]
            Name of the database (optional). Defaults to DATABASE_NAME.

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the collection data, with '_id' column removed and 'na' values replaced with NaN.
        """
        try:


            # Access specified collection from the default or specified database
            # if database_name is None:
            #     collection = self.mongo_client.database[collection_name]
            # else:
            #     collection = self.mongo_client[database_name][collection_name]
            
            # Use the shared MongoClient for this instance
            # client = pymongo.MongoClient(CONNECTION_URL)


            client = pymongo.MongoClient(MONGODB_URL_KEY)
            data_base=client["Proj1"]
            collection=data_base["Proj1-data"]
            entries = list(collection.find())
            print(f"Data fecthed with len: {len(entries)}")
            # entries = list(self.mongo_client["Proj1"][collection_name].find())
            # Convert collection data to DataFrame and preprocess
            print("Fetching data from mongoDB")
            df = pd.DataFrame(entries)
            print(f"Data fecthed with len: {len(df)}")
            if "id" in df.columns.to_list():
                df = df.drop(columns=["id"], axis=1)
            df.replace({"na":np.nan},inplace=True)
            return df

        except Exception as e:
            raise MyException(e, sys)



