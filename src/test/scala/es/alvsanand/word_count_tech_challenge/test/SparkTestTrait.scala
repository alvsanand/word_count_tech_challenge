package es.alvsanand.word_count_tech_challenge.test

import java.util.{Properties, UUID}

import es.alvsanand.word_count_tech_challenge.SparkJob
import es.alvsanand.word_count_tech_challenge.utils.{Config, Logging}
import org.apache.ignite.spark.IgniteContext
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalactic.source
import org.scalatest._
import scala.collection.JavaConverters._

import scala.util.Try

trait SparkTestTrait extends FeatureSpec with Matchers with BeforeAndAfter with GivenWhenThen with Logging {
  protected val config = () => Config()

  override protected def before(fun: => Any)(implicit pos: source.Position): Unit = {
    super[BeforeAndAfter].before({
      fun
    })
  }

  override protected def after(fun: => Any)(implicit pos: source.Position): Unit = {
    super[BeforeAndAfter].after({
      fun
    })
  }

  protected def withSpark(fun: (SparkSession) => Any): Unit = {
    val conf = new SparkConf()
      .setAppName("DEFAULT_APP")
      .setMaster("local[2]")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
    val sparkSession =  SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    SparkJob.setSparkSession(sparkSession)

    val result = Try({fun(sparkSession)})

    if(sparkSession!=null){
      sparkSession.stop()
    }

    if(result.isFailure) throw result.failed.get
  }

  private def createKafkaConsumer(group: String, autoOffset: String): KafkaConsumer[String, String] = {
    val config = new Properties()
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config().get("word_count_tech_challenge.kafka").get)
    config.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset)

    new KafkaConsumer[String, String](config)
  }

  private def createKafkaProducer(client: String): KafkaProducer[String, String] = {
    val config = new Properties()
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config().get("word_count_tech_challenge.kafka").get)
    config.put(ProducerConfig.CLIENT_ID_CONFIG, client)
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    new KafkaProducer[String, String](config)
  }

  protected def withKafka(group: String = UUID.randomUUID().toString(), client: String = UUID.randomUUID().toString(), autoOffset: String = "earliest", fun: (KafkaProducer[String, String], KafkaConsumer[String, String]) => Any) = {
    val result = Try({fun(createKafkaProducer(group), createKafkaConsumer(client, autoOffset))})

    if(result.isFailure) throw result.failed.get
  }

  protected val igniteConfigFile = "ignite_config.xml"

  protected def withIgnite(cacheNames: List[String], sparkSession: SparkSession)(fun: (IgniteContext) => Any) = {
    val igniteContext = new IgniteContext(sparkSession.sparkContext, igniteConfigFile)

    igniteContext.ignite().destroyCaches(cacheNames.asJava)

    val result = Try({fun(igniteContext)})

    if(result.isFailure) throw result.failed.get
  }
}
