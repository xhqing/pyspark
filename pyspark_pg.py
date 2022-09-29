from pyspark.sql import SparkSession


sparkClassPath: str = "pyspark/jars/postgresql-42.4.0.jar"
spark = SparkSession.builder.appName("pyspark-pg").master("local").config("spark.driver.extraClassPath", sparkClassPath).getOrCreate()

class PySparkPG:
    """pyspark connect pg"""

    def __init__(self, host: str, port: str, username: str, password: str, dbname: str):

        self.dbname = dbname
        self.username = username
        self.password = password
        self.host = host
        self.port = port

    def get_sparkDataFrame(self, tablename: str):

        sparkDataFrame = spark.read.format("jdbc").option("url", f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}").option("driver", "org.postgresql.Driver")\
            .option("dbtable", tablename).option("user", self.username).option("password", self.password).load()

        return sparkDataFrame
