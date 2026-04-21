import duckdb
import pandas as pd

conn = duckdb.connect()
df = conn.execute("""
    SELECT * 
    FROM read_csv_auto('streamdbi_ranking_data_master.csv', header=true, all_varchar=true) 
    WHERE CAST("対象仕訳数" AS VARCHAR) = 'receipt' OR "対象仕訳数" NOT SIMILAR TO '[0-9]*'
""").fetchdf()

print("Bad rows count:", len(df))
print(df)
