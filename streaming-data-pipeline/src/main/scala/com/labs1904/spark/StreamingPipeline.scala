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


      val rawReviews = ds.map(x => {   //making data set an instance of the case class
        val split = x.split("\\t")
        val marketplace = split(0)
        val customer_id = split(1)
        val review_id = split(2)
        val product_id = split(3)
        val product_parent = split(4)
        val product_title = split(5)
        val product_category = split(6)
        val star_rating = split(7)
        val helpful_votes = split(8)
        val total_votes = split(9)
        val vine = split(10)
        val verified_purchase= split(11)
        val review_headline = split(12)
        val review_body= split(13)
        val review_date = split(14)
        Review(marketplace, customer_id, review_id, product_id, product_parent,product_title, product_category, star_rating,
          helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date)
      })

      //map rawReviews to turn into hbase gets

      val reviewGets = rawReviews.map(x => {
        new Get(Bytes.toBytes())
      })
      //row-key will be customer_id

      //val get = new Get(Bytes.toBytes(rawReviews)
      //val get = new Get(Bytes.toBytes("row-key"))
      //val new_collection = collection.map(x => x * x )


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
