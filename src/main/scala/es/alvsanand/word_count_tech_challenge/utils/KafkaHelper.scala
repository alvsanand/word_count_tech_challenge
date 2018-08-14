package es.alvsanand.word_count_tech_challenge.utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.Row

case class SparkKafkaRecord[A, B](key: A,
                                  value: B,
                                  topic: String,
                                  partition: Int,
                                  offset: Long,
                                  timestamp: Long,
                                  timestampType: Int)

object KafkaHelper {
  def parseSparkSQLRecord[A,B](record: Row): Option[SparkKafkaRecord[A,B]] = if(record!=null) record match {
    case Row(key: A,
    value: B,
    topic: String,
    partition: Int,
    offset: Long,
    timestamp: Long,
    timestampType: Int) if record != null =>
      Option(SparkKafkaRecord(key, value, topic, partition, offset, timestamp, timestampType))
  }
  else {
    None
  }

  def parseSparkRecord[A,B](record: ConsumerRecord[A,B]): Option[SparkKafkaRecord[A,B]] = if(record!=null) record match {
    case r if record != null =>
      Option(SparkKafkaRecord(r.key, r.value, r.topic, r.partition, r.offset, r.timestamp, r.timestampType.id))
  }
  else {
    None
  }
}
