import ast
import polars as pl
from schema import BOOK_SCHEMA

rename_map = {
    "authors": "author_id",
    "categories": "category_id",
    "format": "format_id"
}


def convert_to_list(df: pl.LazyFrame, columns: list[str]) -> pl.LazyFrame:
    return df.with_columns(
      pl.col(columns).map_elements(ast.literal_eval, return_dtype=pl.List(pl.Int64))
    )


def replace_nulls(df: pl.LazyFrame, columns: list[str], value: int) -> pl.LazyFrame:
    return df.with_columns(pl.col(columns).fill_null(pl.lit(value)))


def clean_escape_chars(df: pl.LazyFrame, column: str) -> pl.LazyFrame:
    return df.with_columns(pl.col(column).str.replace_all(r"\s*\r\s*", " "))


def apply_schema(df: pl.DataFrame, schema_map: dict) -> pl.DataFrame:
    for col, dtype in schema_map.items():
        if dtype in (pl.Int64, pl.Float64):
            df = df.with_columns(
              pl.col(col).str.replace("", "0").cast(dtype)
            )
        elif dtype in (pl.Date, pl.Datetime):
            df = df.with_columns(
              pl.col(col).str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S", strict=False)
            )

    return df


def process_books(df: pl.LazyFrame, authors_df: pl.LazyFrame, categories_df: pl.LazyFrame, formats_df: pl.LazyFrame) -> pl.DataFrame:
    books_df = (
        df
        .pipe(convert_to_list, ["authors", "categories"])
        .pipe(clean_escape_chars, "description")
        .explode("authors")
        .explode("categories")
        .join(authors_df, left_on="authors", right_on="author_id", how='left')
        .join(categories_df, left_on="categories", right_on="category_id", how='left')
        .join(formats_df, left_on="format", right_on="format_id", how='left')
        .pipe(replace_nulls, ["authors", "categories"], -1)
        .pipe(apply_schema, BOOK_SCHEMA)
        .rename(rename_map)
        .collect()
    )   # Manter as operações em cadeia permite ao Polars usar lazy evaluation para planejar a ordem das operações de forma eficiente.

    return books_df
