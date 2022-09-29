from pyspark.sql import SparkSession


sparkClassPath: str = "pyspark/jars/clickhouse-native-jdbc-shaded-2.5.4.jar"
spark = SparkSession.builder.master("local").appName("Connect To clickhouse - via JDBC").config("spark.driver.extraClassPath", sparkClassPath).getOrCreate()

class PySparkCH:
    """pyspark connect clickhouse"""

    def __init__(self, host: str, port: str, username: str, password: str, dbname: str):

        url = f"jdbc:clickhouse://{host}:{port}"
        self.driver = "com.github.housepower.jdbc.ClickHouseDriver"
        self.url = url
        self.username = username
        self.password = password
        self.dbname = dbname

    def get_sparkDataFrame(self, tablename: str):

        dbtable = self.dbname + "." + tablename
        sparkDataFrame = (
            spark.read.format("jdbc")
            .option("driver", self.driver)
            .option("url", self.url)
            .option("user", self.username)
            .option("password", self.password)
            .option("dbtable", dbtable)
            .load()
        )
        return sparkDataFrame

