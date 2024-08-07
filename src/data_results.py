import polars as pl
from datetime import datetime


def transform_to_lazyframe(df: pl.DataFrame) -> pl.LazyFrame:
    return df.lazy()


def get_distinct_book_count(df: pl.LazyFrame, consider_columns: list[str]) -> int:
    return (
      df
      .select(consider_columns)
      .unique()
      .select(pl.count().alias("distinct_book_count"))
      .collect()
    )[0, 0]


def get_single_author_book_count(df: pl.LazyFrame) -> int:
    return (
      df
      .group_by(["id", "title"])
      .agg(pl.count("author_id").alias("author_count"))
      .filter(pl.col("author_count") <= 1)
      .select(pl.count().alias("single_author_book_count"))
      .collect()
    )[0, 0]


def get_top_five_authors(df: pl.LazyFrame) -> pl.DataFrame:
    return (
      df
      .filter(pl.col("author_id") != -1)
      .group_by(["author_id", "author_name"])
      .agg(pl.count("id").alias("book_count"))
      .sort("book_count", descending=True)
      .limit(5)
      .collect()
    )


def get_book_count_by_category(df: pl.LazyFrame) -> pl.DataFrame:
    return (
      df
      .group_by("category_name")
      .agg(pl.col("id").unique().count().alias("book_count"))
      .sort("book_count", descending=True)
      .collect()
    )


def get_most_common_book_format(df: pl.LazyFrame) -> pl.DataFrame:
    return (
      df
      .group_by(["format_id", "format_name"])
      .agg(pl.col("id").unique().count().alias("book_count"))
      .sort("book_count", descending=True)
      .limit(1)
      .collect()
    )


def get_top_ten_bestsellers(df: pl.LazyFrame) -> pl.DataFrame:
    return (
      df
      .select(["id", "title", "bestsellers-rank"])  # Seleciona as colunas relevantes
      .unique(subset=["id"], keep='first')  # Mantém apenas a primeira ocorrência de cada ID
      .sort("bestsellers-rank", descending=True)  # Ordena pelo ranking
      .limit(10)  # Limita aos 10 primeiros
      .collect()
    )


def get_top_ten_rating_avg(df: pl.LazyFrame) -> pl.DataFrame:
    return (
      df
      .with_columns(
          pl.col("rating-avg").rank(method="ordinal", descending=True).over("title").alias("rank")
      )
      .filter(pl.col("rating-avg").is_not_null() & (pl.col("rank") == 1))
      .sort(["rating-avg", "publication-date"], descending=[True, True])
      .select(["id", "title", "rating-avg", "publication-date"])
      .limit(10)
      .collect()
    )


def get_book_count_with_greater_rating(df: pl.LazyFrame, greater_than_value: float | int) -> int:
    return (
      df
      .filter(pl.col("rating-avg") > greater_than_value)
      .select(pl.col("id").unique().count())
      .collect()
    )[0, 0]


def get_book_count_after_date(df: pl.LazyFrame, year: int, month: int, day: int) -> int:
    return (
      df
      .filter(pl.col('publication-date') > pl.lit(datetime(year, month, day)))
      .select(pl.col(["id", "title", "publication-date"]).unique().count())
      .collect()
    )[0, 0]


def show_data_results(df: pl.DataFrame) -> None:
    df = transform_to_lazyframe(df)

    distinct_book_count = get_distinct_book_count(df, ['id'])
    top_five_categories_df = get_book_count_by_category(df).head(5)
    rating_greater_than_book_count = get_book_count_with_greater_rating(df, 3.5)
    after_2020_book_count = get_book_count_after_date(df, 2020, 1, 1)

    print(f"Quantidade total de livros da base: {distinct_book_count}\n")

    print(f"Quantidade de livros com apenas 1 autor: {get_single_author_book_count(df)}\n")

    print(f"Top 5 autores por quantidade de livros: {get_top_five_authors(df)}\n")

    print(f"Quantidade de livros por categoria: {get_book_count_by_category(df)}\n")

    print(f"Top 5 categorias com a maior quantidade de livros: {top_five_categories_df}\n")

    print(f"Formato com a maior quantidade de livros: {get_most_common_book_format(df)}\n")

    with pl.Config(tbl_rows=10, fmt_str_lengths=50):
        print(f"Top 10 livros baseando-se na coluna 'bestsellers-rank': {get_top_ten_bestsellers(df)}\n")
        print(f"Top 10 livros baseando-se na coluna 'rating-avg': {get_top_ten_rating_avg(df)}\n")

    print(f"Quantidade de livros com 'rating-avg' maior que 3,5: {rating_greater_than_book_count}\n")
    print(f"Quantidade de livros com data de publicação maior que 01/01/2020: {after_2020_book_count}")
