#!/usr/bin/env python3

import requests
from typing import Dict
from pprint import pprint
from datetime import datetime
from utils import save_data
from argparse import ArgumentParser

url = "https://openlibrary.org/works/"

ids = [
        "OL47317227M",
        "OL38631342M",
        "OL46057997M",
        "OL26974419M",
        "OL10737970M",
        "OL25642803M",
        "OL20541993M",
]


def collect_single_book_data(base_url: str, open_library_id: str, file_format: str = "json") -> Dict:
    full_url = f"{base_url}{open_library_id}.{file_format}"

    try:
        response = requests.get(full_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data for ID {open_library_id}: {e}")
        return {}


if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument("--data_lake_path", required=True, 
                        help="Path to the data lake directory")
    
    args = parser.parse_args()

    path = args.data_lake_path

    for book_id in ids:
        book_data = collect_single_book_data(url, book_id)
        current_date = datetime.now().strftime("%Y%m%d")
        file_name = f"{current_date}_book_{book_id}.json"
        save_data(book_data, file_name, zone="raw", context="books", file_type="json", base_path=path)
    
