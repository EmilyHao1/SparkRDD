import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

val spark = SparkSession.builder().appName("BDProject4 Question 1").config("spark.some.config", "some-value").getOrCreate()
import spark.implicits._

import org.apache.spark.sql.types._
val dfSchema = StructType(Array(StructField("TransID", IntegerType, true), StructField("CustID", IntegerType, true), StructField("TransTotal", FloatType, true), StructField("TransNumItems", IntegerType, true), StructField("TransDesc", StringType, true)))

val df = spark.read.format("csv").option("sep", ",").option("header","true").schema(dfSchema).load("transactions.txt")
df.printSchema()

val T1 = df.filter($"TransTotal">200)

val T2 = T1.groupBy($"TransNumItems").agg(functions.sum($"TransTotal"), functions.avg($"TransTotal"), functions.max($"TransTotal"), functions.min($"TransTotal"))

T2.show()

val T3 = T1.groupBy($"CustID").count()


val T4 = df.filter($"TransTotal">600)

val T5 = T4.groupBy($"CustID").count().alias("Count") 

val T6_temp = T5.select($"CustID", col("Count").multiply(5).alias("Count5"))
Val T6_Join = T6_temp.join(T3, "CustID")
Val T6 = T6_Join.filter($"Count5" > $"Count")
