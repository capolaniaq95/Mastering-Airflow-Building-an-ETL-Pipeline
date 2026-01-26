import json
from typing import Dict, Union, List
import pandas as pd


def save_data(file_content: Union[List[Dict], Dict, List, str, pd.DataFrame], file_name: str, 
             zone: str = "raw", context:str = "books", file_type:str = "csv"):
    """
    Function that receives the dictionary containing the contents of a book and saves it in a JSON file

    Args: 
        file_content: Union[List[Dict], Dict, List, str, dataframe]
            The content to be saved in the file. It can be a list of dictionaries, a single dictionary, a list, a string, or a pandas DataFrame.
        file_name: str
            The name of the file where the content will be saved.
        zone: str
            The zone where the file will be saved. Default is "raw".
        context: str
            The context of the data being saved. Default is "books".
        file_type: str
            The type of file to save. Default is "csv".
    """
    file_path = f"../data_lake/{zone}/{file_type}/{context}/{file_name}"
    if file_content is None:
        return
    if file_type == "json":
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(file_content, file, ensure_ascii=False, indent=4)
    elif file_type == "csv":
        with open(file_path, "w", encoding="utf-8") as file:
            df = pd.DataFrame(file_content)
            df.to_csv(file, index=False)
    elif file_type == "parquet":
        with open(file_path, "wb") as file:
            df = pd.DataFrame(file_content)
            df.to_parquet(file, index=False)
