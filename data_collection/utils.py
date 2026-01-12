import json
from typing import Dict, Union, List

def save_data(file_content: Union[List[Dict], Dict, List, str], file_name: str, 
             zone: str = "raw", context:str = "books", file_type:str = "csv"):
    """
    Function that receives the dictionary containing the contents of a book and saves it in a JSON file

    Args: 
        file_content: Union[List[Dict], Dict, List, str]
            The content to be saved in the file. It can be a list of dictionaries, a single dictionary, a list, or a string.
        file_name: str
            The name of the file where the content will be saved.
        zone: str
            The zone where the file will be saved. Default is "raw".
        context: str
            The context of the data being saved. Default is "books".
        file_type: str
            The type of file to save. Default is "csv".
    """
    directory = f"../data_lake/{zone}/{file_type}/{context}/"

    with open(f"{directory}{file_name}", "w", encoding="utf-8") as file:
        json.dump(file_content, file, ensure_ascii=False, indent=4)


