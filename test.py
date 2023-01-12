from pyspark_pg import PySparkPG

conn = PySparkPG("localhost", "9999", "postgres", "example", "postgres")

df = conn.get_sparkDataFrame("test.bd_peak_index_song_feature_lib")

print(df.show())
