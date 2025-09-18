from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("intro")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
df.show()
print("Spark version:", spark.version)
# Spark UI: http://localhost:4040
spark.stop()
