package es.alvsanand.word_count_tech_challenge

import es.alvsanand.word_count_tech_challenge.utils.{Config, Logging}
import org.apache.spark.streaming.Milliseconds
import scopt.OptionParser

object WordCountStreamingJobMain extends Logging{

  private val config = Config()

  def main(args: Array[String]) {
    val defaultParams = scala.collection.mutable.Map[String, Any]()
    defaultParams += "topic" -> config.get("word_count_tech_challenge.topic").get
    defaultParams += "batchDuration" -> config.get("word_count_tech_challenge.batchDuration").get.toLong

    val parser = new OptionParser[scala.collection.mutable.Map[String, Any]]("WordCountStreamingJobMain") {
      head("Word Count Streaming Job")
      opt[String]("topic")
        .text("Kafka topic used to read the text")
        .action((x, c) => {
          c += "topic" -> x
        })
      opt[Long]("batchDuration")
        .text("Batch duration of the job in ms")
        .action((x, c) => {
          c += "batchDuration" -> x
        })
      help("help") text ("prints this usage text")
    }
    parser.parse(args, defaultParams).map { params =>
      run(params.toMap)
    } getOrElse {
      System.exit(1)
    }
  }

  private def run(params: Map[String, Any]): Unit = {
    val topic = params("topic").asInstanceOf[String]
    val batchDuration = Milliseconds(params("batchDuration").asInstanceOf[Long])

    val args = WordCountStreamingJobArguments(topic, batchDuration)

    try {
      logInfo("Init WordCountStreamingJobMain")

      val result = WordCountStreamingJob.run(args)

      if(result.isSuccess){
        sys.addShutdownHook({
          result.get.stop()
        })

        result.get.awaitTermination()

        logInfo("End WordCountStreamingJobMain")
      }
      else {
        logError("Error executing WordCountStreamingJobMain: WordCountStreamingJob is failure")

        sys.exit(1)
      }
    }
    catch {
      case e: Exception =>
        logError("Error executing WordCountStreamingJobMain", e)

        sys.exit(1)
    }
  }
}
