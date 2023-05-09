package com.labs1904.spark

import org.apache.hadoop.hbase.client.Get
import org.apache.kafka.common.serialization.Serdes.Bytes
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}


/**
 * Spark Structured Streaming app
 *
 */
object StreamingPipeline {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "StreamingPipeline"

  val hdfsUrl = "CHANGEME"
  val bootstrapServers = "CHANGE ME"
  val username = "CHANGE ME"
  val password = "CHANGE ME"
  val hdfsUsername = "slashley" // TODO: set this to your handle

  //Use this for Windows
  val trustStore: String = "src\\main\\resources\\kafka.client.truststore.jks"
  //Use this for Mac
  //val trustStore: String = "src/main/resources/kafka.client.truststore.jks"

  case class Review(
                     marketplace:String,
                     customer_id:String,
                     review_id:String,
                     product_id:String,
                     product_parent:String,
                     product_title:String,
                     product_category:String,
                     star_rating:String,
                     helpful_votes:String,
                     total_votes:String,
                     vine:String,
                     verified_purchase:String,
                     review_headline:String,
                     review_body:String,
                     review_date:String
                   )


  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder()
        .config("spark.sql.shuffle.partitions", "3")
        .appName(jobName)
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._

      val ds = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "20")
        .option("startingOffsets","earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.ssl.truststore.location", trustStore)
        .option("kafka.sasl.jaas.config", getScramAuthString(username, password))
        .load()
        .selectExpr("CAST(value AS STRING)").as[String]

      //construct hbase get request for every review message (customer_id corresponds to hbase rowkey)
      //val get = new Get(Bytes.toBytes("rowkey") -- (get object for rowkey)
      //val result = table.get(get) -- (returns result object)
      //analyze result object
      //convert ds to Review case class

      val rawReviews = ds.map(x => {
        val split = x.split("\\t")
        val marketplace = split(0)
        val customer_id = split(1)
        val review_id = split(2)
        val product_id = split(3)
        val product_parent = split(4)
        val product_title = split(5)
        val product_category = split(6)
        Review(marketplace, customer_id...)
      })

      //map rawReviews to turn into hbase gets

      val get = new Get (Bytes.toBytes(Review))


      // TODO: implement logic here
      val result = ds

      // Write output to console
      val query = result.writeStream
        .outputMode(OutputMode.Append())
        .format("console")
        .option("truncate", false)
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      // Write output to HDFS
//      val query = result.writeStream
//        .outputMode(OutputMode.Append())
//        .format("json")
//        .option("path", s"/user/${hdfsUsername}/reviews_json")
//        .option("checkpointLocation", s"/user/${hdfsUsername}/reviews_checkpoint")
//        .trigger(Trigger.ProcessingTime("5 seconds"))
//        .start()
      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def getScramAuthString(username: String, password: String) = {
    s"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username=\"$username\"
   password=\"$password\";"""
  }
}
