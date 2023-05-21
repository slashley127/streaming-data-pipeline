package com.labs1904.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
//import org.apache.kafka.common.serialization.Serdes.Bytes
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}


/**
 * Spark Structured Streaming app
 *
 */

case class Review(
                   marketplace: String,
                   customer_id: String,
                   review_id: String,
                   product_id: String,
                   product_parent: String,
                   product_title: String,
                   product_category: String,
                   star_rating: String,
                   helpful_votes: String,
                   total_votes: String,
                   vine: String,
                   verified_purchase: String,
                   review_headline: String,
                   review_body: String,
                   review_date: String
                 )


object StreamingPipeline {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "StreamingPipeline"

  val hdfsUrl = "CHANGE ME"
  val bootstrapServers = "CHANGE ME"
  val username = "CHANGE ME"
  val password = "CHANGE ME"
  val hdfsUsername = "slashley" // TODO: set this to your handle

  //Use this for Windows
  val trustStore: String = "src\\main\\resources\\kafka.client.truststore.jks"
  //Use this for Mac
  //val trustStore: String = "src/main/resources/kafka.client.truststore.jks"


  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder()
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.fs.defaultFS", hdfsUrl)
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
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.ssl.truststore.location", trustStore)
        .option("kafka.sasl.jaas.config", getScramAuthString(username, password))
        .load()
        .selectExpr("CAST(value AS STRING)").as[String]

      //splitting reviews (tab separated in raw) to Review case class
      val rawReviews = ds.map(reviewCategory => { //making data set an instance of the case class
        val reviewSplit = reviewCategory.split("\\t")
        val marketplace = reviewSplit(0)
        val customer_id = reviewSplit(1)
        val review_id = reviewSplit(2)
        val product_id = reviewSplit(3)
        val product_parent = reviewSplit(4)
        val product_title = reviewSplit(5)
        val product_category = reviewSplit(6)
        val star_rating = reviewSplit(7)
        val helpful_votes = reviewSplit(8)
        val total_votes = reviewSplit(9)
        val vine = reviewSplit(10)
        val verified_purchase = reviewSplit(11)
        val review_headline = reviewSplit(12)
        val review_body = reviewSplit(13)
        val review_date = reviewSplit(14)
        Review(marketplace, customer_id, review_id, product_id, product_parent, product_title, product_category, star_rating,
          helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date)
      })



      val customers = rawReviews.mapPartitions(partition => {

        //HBase connection
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "cdh01.hourswith.expert:2181,cdh02.hourswith.expert:2181,cdh03.hourswith.expert:20181")
        val connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf("slashley:users"))

        //mapping customer_id (rowkey) to create 'gets' to retrieve customer info from HBase
        val reviewsIteration = partition.map(review => {
          val get = new Get(Bytes.toBytes(review.customer_id)).addFamily(Bytes.toBytes("f1"))
          val result = table.get(get)

          (Bytes.toBytes(review.customer_id), Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail"))))
      }).toList.iterator

      connection.close()

      reviewsIteration
    })

      // Write output to console
      val query = customers.writeStream
        .outputMode(OutputMode.Append())
        .format("console")
        .option("truncate", false)
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      //Write output to HDFS
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
