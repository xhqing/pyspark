# pyspark
Modified PySpark. Based on `pyspark.__version__==3.3.0`

1. change pyspark/sql/dataframe.py `DataFrame` class `show` method arg `truncate` default value to `False`
2. change pyspark/sql/dataframe.py `DataFrame` class `show` method arg `n` default value to `3`, means default show 3 records
3. add pyspark/jars/postgresql-42.4.0.jar
4. add pyspark/jars/clickhouse-native-jdbc-shaded-2.5.4.jar
