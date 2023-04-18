package com.labs1904.hwe.consumers

import com.labs1904.hwe.util.Constants._
import com.labs1904.hwe.util.Util
import net.liftweb.json.DefaultFormats
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Arrays

case class RawUser(id: Int, username: String, name: String, email: String, date: String)
case class EnrichedUser(rawUser: RawUser, numberAsWord: String, hweDeveloper: String)

object HweConsumer {
  private val logger = LoggerFactory.getLogger(getClass)

  val consumerTopic: String = "question-1"
  val producerTopic: String = "question-1-output"

  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {

    // Create the KafkaConsumer
    val consumerProperties = Util.getConsumerProperties(BOOTSTRAP_SERVER)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProperties)

    // Create the KafkaProducer
    val producerProperties = Util.getProperties(BOOTSTRAP_SERVER)
    val producer = new KafkaProducer[String, String](producerProperties)

    // Subscribe to the topic
    consumer.subscribe(Arrays.asList(consumerTopic))

    while ( {
      true
    }) {
      // poll for new data
      val duration: Duration = Duration.ofMillis(100)
      val records: ConsumerRecords[String, String] = consumer.poll(duration)

      records.forEach((record: ConsumerRecord[String, String]) => {

        // Retrieve the message from each record
        val message: Array[String] = record.value().split("\t")
        val user = RawUser(message(0).toInt, message(1), message(2), message(3), message(4) )
        val utilMethod = Util.mapNumberToWord(user.id)
        val hweDeveloper = "Slash"
        val enrichedUser= EnrichedUser(user, utilMethod, hweDeveloper)

        val fields: List[String] = List(user.date, user.name, user.email, user.username, user.id.toString, utilMethod, hweDeveloper)
        val output: String = fields.mkString(",")
        logger.info(s"Message Received: $output")
        // TODO: Add business logic here!

      })
    }
  }
}