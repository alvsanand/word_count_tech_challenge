package es.alvsanand.word_count_tech_challenge

import es.alvsanand.word_count_tech_challenge.utils.{Config, Logging}
import org.apache.ignite.spark.IgniteContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait SparkJobArguments {
  def toMap =
    (getClass.getDeclaredFields).filterNot(_.getName.startsWith("$")).map { f =>
      f.setAccessible(true)
      (f.getName -> f.get(this).toString)
    }.toMap
}

trait SparkJob[A <: SparkJobArguments, B] extends Logging{

  protected val igniteConfigFile = "ignite_config.xml"

  protected def getName(): String = {
    val fullName = this.getClass.getSimpleName

    fullName.replaceAll("\\$.*", "")
  }

  def validateArguments(args: A): Unit

  protected def doRun(args: A)(sparkSession: SparkSession): B

  def run(args: A): Try[B] = {
    validateArguments(args)

    val sparkSession = SparkJob.sparkSession()

    implicit val jobId = sparkSession.sparkContext.applicationId

    val initTime = System.currentTimeMillis()

    val result = logTime[B](s"Execution of ${getName()}[$args]", quiet = true) {
      doRun(args)(SparkJob.sparkSession())
    }

    result.result match {
      case Failure(e) => {
        val error = s"Error executing ${getName()}: ${e.getMessage()}"
        logError(error, e)
      }
      case Success(v) =>
    }

    result.result
  }

  protected def config(): Config = SparkJob._config

  protected def createKafkaDirectStream[K, V, K2 <: Deserializer[K], V2 <: Deserializer[V]](topics: Array[String])
                                                                                           (sparkSession: SparkSession, streamingContext: StreamingContext)
                                                                                           (implicit tagK2: ClassTag[K2],  tagV2: ClassTag[V2]): InputDStream[ConsumerRecord[K, V]] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> config.get("word_count_tech_challenge.kafka").get,
      "key.deserializer" -> tagK2.runtimeClass,
      "value.deserializer" -> tagV2.runtimeClass,
      "group.id" -> s"${getName()}_GROUP",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val subscribe: ConsumerStrategy[K, V] =  ConsumerStrategies.Subscribe[K, V](topics, kafkaParams)

    KafkaUtils.createDirectStream[K, V](
      streamingContext,
      PreferConsistent,
      subscribe
    )
  }

  protected def getIgniteContext(sparkSession: SparkSession) = new IgniteContext(sparkSession.sparkContext, igniteConfigFile)
}

object SparkJob {
  private var _config: Config = Config()

  private var _sparkSession: SparkSession = _

  private def sparkSession(): SparkSession = {
    this.synchronized {
      if(_sparkSession==null) {
        val conf = new SparkConf()
          .setMaster(_config.get("word_count_tech_challenge.sparkMaster").get)

        _sparkSession = SparkSession
          .builder()
          .config(conf)
          .getOrCreate()
      }
    }
    _sparkSession
  }

  def setSparkSession(sparkSession: SparkSession) = {
    this.synchronized {
      _sparkSession = sparkSession
    }
  }
}
