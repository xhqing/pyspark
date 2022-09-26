from pyspark.sql import SparkSession

class PySparkPG:
    """pyspark connect pg"""

    def __init__(self, dbname, tablename, username, password, host, port):
        self.dbname = dbname
        self.tablename = tablename
        self.username = username
        self.password = password
        self.host = host
        self.port = port

    def get_sparkDataFrame(self):
        sparkClassPath = "pyspark/jars/postgresql-42.4.0.jar"
        spark = SparkSession.builder.appName("pyspark pg app").master("local").config("spark.driver.extraClassPath", sparkClassPath).getOrCreate()

        sparkDataFrame = spark.read.format("jdbc").option("url", f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}").option("driver", "org.postgresql.Driver")\
            .option("dbtable", self.tablename).option("user", self.username).option("password", self.password).load()

        return sparkDataFrame
