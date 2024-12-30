from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_date, row_number, to_timestamp, hour, minute, when, countDistinct, lit

spark = SparkSession.builder \
    .appName("Filter and Join CSVs") \
    .getOrCreate()

# Read the first CSV with columns: ticker, Date, gap_percent, avg_volume_30, Close
df1 = spark.read.csv("../downloader/data/", header=True, inferSchema=True)
# print("df1 without filter ")
# print(df1.count())
# Filter rows where gap_percent is between -8 and -2
df1_filtered = df1.filter((col("gap_percent") >= -8) & (col("gap_percent") <= -2))
# print("df1 with filter ")
# print(df1_filtered.count())
# print("df1 with filter SHOW")
# df1_filtered.show()
# Read the second CSV with columns: Ticker, Date, Open, Close, Volume, Market Cap, Sector, Company Short Name
df2 = spark.read.csv("../downloader/nse_data_per_ticker/", header=True,
                     inferSchema=False)
# print("df2 count")
# print(df2.count())

df2 = ((((df2.withColumnRenamed("Ticker", "ticker")
          .withColumnRenamed("Date", "5minsTickerTime"))
         .withColumn("5minsTickerTime", col("5minsTickerTime").substr(1, 19))
         .withColumn("Date", to_date(col("5minsTickerTime"), "yyyy-MM-dd HH:mm:ss")))
        .withColumnRenamed("Close", "5minsClose").withColumnRenamed("Open", "5minsOpen"))
       .withColumn("timestamp", to_timestamp(col("5minsTickerTime"), "yyyy-MM-dd HH:mm:ss")))

# Filter rows where the time is below 12:30
filtered_df2 = df2.filter(
    (hour(col("timestamp")) < 12) | ((hour(col("timestamp")) == 12) & (minute(col("timestamp")) < 30)))

# print("df2 show")
# df2.printSchema()
# df2.show()
joined_df = df1_filtered.join(filtered_df2, on=["ticker", "Date"], how="inner")

# Show the resulting DataFrame
# print("joinedDF")
# joined_df.show()
# print(joined_df.count())
# print("GroupBy")
# grpBy = joined_df.groupby("ticker").count()
#

dfWithWinLose = joined_df.withColumn("Outcome",
                                     when(col("gap_target") < col("5minsClose").cast("float"), "win").otherwise("lose"))

winLoseGroupBy = dfWithWinLose.groupby("ticker", "Date", "Outcome").count()

grouped_df = winLoseGroupBy.groupBy("ticker", "Date").agg(
    countDistinct("Outcome").alias("distinct_outcomes")
)

# Join back with the original DataFrame to determine the final decision
result_df = winLoseGroupBy.join(grouped_df, on=["ticker", "Date"], how="inner") \
    .withColumn(
    "final_outcome",
    when(col("distinct_outcomes") == 2, "Win")  # If two distinct outcomes, consider as Win
    .otherwise(col("Outcome"))  # Otherwise, retain the original Outcome
).drop("distinct_outcomes").select("ticker", "Date", "final_outcome").distinct()

finalDf = result_df.join(joined_df, on=["ticker", "Date"], how="inner")
# finalDf.show(100)
# filterBasedOnGapTarget = joined_df.filter(col("gap_target") < col("5minsClose").cast("float"))

# filterBasedOnGapTarget.show()
#
# window_spec = Window.partitionBy("ticker", "Date").orderBy(col("5minsTickerTime").asc())
#
# # Use row_number to identify the row with the least 5minsTickerTime for each ticker and Date
# df_with_row_number = filterBasedOnGapTarget.withColumn("row_num", row_number().over(window_spec))
#
# # Filter to keep only rows with row_num = 1
# result_df = df_with_row_number.filter(col("row_num") == 1).drop("row_num").orderBy("Date")
# result_df.coalesce(1).write.mode("overwrite").option("header","true").csv("output.csv")
# result_df.show()
# Stop the Spark session
# finalDf.printSchema()
loseOutcome = finalDf.withColumn("5minsClose", col("5minsClose").cast("double")).filter(
    col("final_outcome") == lit("lose")).filter(col("gap_target") > col("5minsClose"))
winOutcome = finalDf.filter(col("final_outcome") == lit("Win")).filter(
    col("gap_target") < col("5minsClose").cast("double"))

window_spec = Window.partitionBy("ticker", "Date").orderBy(col("5minsTickerTime").asc())
df_with_row_number = winOutcome.withColumn("row_num", row_number().over(window_spec))
winOutcomeResult_df = df_with_row_number.filter(col("row_num") == 1).drop("row_num").orderBy("Date")

window_spec2 = Window.partitionBy("ticker", "Date").orderBy(col("5minsClose").desc())
df_with_row_number2 = loseOutcome.withColumn("row_num", row_number().over(window_spec2))
loseOutcomeResult_df = df_with_row_number2.filter(col("row_num") == 1).drop("row_num").orderBy("Date")
# winOutcomeResult_df.show(20)
# loseOutcomeResult_df.show(20)

# finalDf.orderBy("ticker", "Date").filter(col("ticker") == lit("21STCENMGM.NS")).filter(
#     col("Date") == lit("2024-11-14")).show()
# df_with_row_number2.show(30)
# joined_df.filter(col("ticker") == lit("21STCENMGM.NS")).filter(col("Date") == lit("2024-11-14")).show()

theLastDF= loseOutcomeResult_df.unionByName(winOutcomeResult_df).orderBy( "Date")

theLastDF.coalesce(1).write.mode("overwrite").option("header","true").csv("trail1Output.csv")

spark.stop()
