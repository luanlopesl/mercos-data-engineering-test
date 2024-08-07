import polars as pl
from pathlib import Path
from data_results import show_data_results
from data_processing import process_books

source_directory = Path(__name__).parent / "data"


def main():
    books_df = pl.scan_csv(source_directory / 'dataset.csv')
    authors_df = pl.scan_csv(source_directory / 'authors.csv')
    categories_df = pl.scan_csv(source_directory / 'categories.csv')
    formats_df = pl.scan_csv(source_directory / 'formats.csv')

    books_df = process_books(books_df, authors_df, categories_df, formats_df)

    show_data_results(books_df)


if __name__ == "__main__":
    main()
