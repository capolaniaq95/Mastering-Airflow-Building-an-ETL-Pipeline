#!/usr/bin/env python3

import json
import pandas as pd
import os
from typing import Dict, Union, List
from utils import save_data
from datetime import datetime



list_books = os.listdir("../data_lake/raw/json/books")

books_collected = []

for book_file in list_books:
    book_collect = {}
    id = book_file.split("_")[2].split(".")[0]
    collected_date_raw = book_file.split("_")[0]
    
    try:
        collected_date = datetime.strptime(collected_date_raw, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        collected_date = collected_date_raw 
    with open(f"../data_lake/raw/json/books/{book_file}") as f:
        book_content = json.load(f)

    title = book_content.get("title", "Unknown Title")
    subtitle = book_content.get("subtitle", "unknown subtitle")
    number_of_pages = book_content.get("number_of_pages", 0)
    publish_date = book_content.get("publish_date", "unknown publish date")
    publish_country = book_content.get("publish_country", "unknown publish country")
    by_statement = book_content.get("by_statement", "unknown by statement")
    
    try:
        publish_places = book_content.get("publish_places")
        if len(publish_places) > 1:
            publish_places = "|".join(publish_places)
        else:
            publish_places = publish_places[0].strip()
    except (TypeError, AttributeError):
        publish_places = "unknown publish place"

    try:
        publishers = book_content.get("publishers")
        if len(publishers) > 1:
            publishers = "|".join(publishers)
        else:
            publishers = publishers[0].strip()
    except (TypeError, AttributeError):
        publishers = "unknown publisher"

    try:
        authors_uri = book_content.get("authors")
        if len(authors_uri) > 1:
            authors_uri = "|".join(authors_uri)
        else:
            authors_uri = authors_uri[0]['key'].strip()
    except (TypeError, AttributeError):
        authors_uri = book_content.get("author", "unknown author")
        if isinstance(authors_uri, list):
            if len(authors_uri) > 1:
                authors_uri = "|".join(authors_uri)
            else:
                authors_uri = authors_uri[0].strip()

    book_collect = {"id": id,
                        "collected_date": collected_date,
                        "title": title,
                        "subtitle": subtitle,
                        "number_of_pages": number_of_pages,
                        "publish_date": publish_date,
                        "publish_country": publish_country,
                        "by_statement": by_statement,
                        "publish_place": publish_places,
                        "publisher": publishers,
                        "author_uri": authors_uri}
    
    books_collected.append(book_collect)

save_data(file_content=books_collected, 
          file_name="books.parquet", 
          zone="refined", context="books", 
          file_type="parquet")

    