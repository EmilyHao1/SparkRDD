import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions
import org.apache.spark.sql.Row

import org.apache.spark.sql.types._

val spark = SparkSession.builder.appName("SparkPageRank").getOrCreate()
val lines = spark.read.textFile("question3Data.txt").rdd
// i got rid of the first line because it is not dataset 
val junk = lines.first()
val validLines = lines.filter(k => k != junk)
// split the dataset into two columns (space before, and space after)
val linksRDD = validLines.map{ column => 
	(column.split("\\s+")(0), column.split("\\s+")(1))}
// distinct value only to make Rank table R0, inititalize to 1.0 for ranks 
val ranks = linksRDD.distinct().map(ranked => (ranked._1, 1.0))
// group by key to prepare for the join in the loop 
val link = linksRDD.groupByKey()

for (w <- 1 to 50){
	// join link and rank table into [source, destination, rank]
	// used distinct to remove the duplicates 
	val joined = links.leftOuterJoin(ranks).distinct()	// left outer join to handle the out of edge cases 
	// i was trying to calculate the new rank at this step, so the new table should be [source, destination, updated-rank]
	// my VM crushed when I am in this part, so I cannot test the codes below. I also did not have the chance to run the loop yet
	val updateRanks = joined.values.flatMap{case (sources, dest, rank) => sources.map(source => (source, dest, (rank /dest.count)))}
	//this code below is to update rank table to [dest, rank], the rank of each dest is added together for the new rank
	ranks = updateRanks.groupByKey(dest).sum(rank)
}
// this is the produce the output (the first 100 row)
val output = ranks.collect().take(100).foreach(println)

