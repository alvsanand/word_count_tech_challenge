package es.alvsanand.word_count_tech_challenge

import es.alvsanand.word_count_tech_challenge.utils.{Config, Logging}
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser

object WordCountJobMain extends Logging{

  private val config = Config()

  def main(args: Array[String]) {
    val defaultParams = scala.collection.mutable.Map[String, Any]()
    defaultParams += "filesPath" -> config.get("word_count_tech_challenge.filesPath").get
    defaultParams += "topSize" -> config.get("word_count_tech_challenge.topSize").get.toInt
    defaultParams += "cacheLevel" -> config.get("word_count_tech_challenge.cacheLevel").getOrElse("")

    val parser = new OptionParser[scala.collection.mutable.Map[String, Any]]("WordCountJobMain") {
      head("Word Count Streaming Job")
      opt[String]("filesPath")
        .text("Path to the files")
        .action((x, c) => {
          c += "filesPath" -> x
        })
      opt[Int]("topSize")
        .text("Numbers of entries of the top")
        .action((x, c) => {
          c += "topSize" -> x
        })
      opt[String]("cacheLevel")
        .text("Type of cache to use. See org.apache.spark.storage.StorageLevel for types")
        .action((x, c) => {
          c += "cacheLevel" -> x
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
    val filesPath = params("filesPath").asInstanceOf[String]
    val topSize = params("topSize").asInstanceOf[Int]
    val cacheLevel = params.get("cacheLevel").map(c => StorageLevel.fromString(c.asInstanceOf[String])).headOption

    val args = WordCountJobArguments(filesPath, topSize, cacheLevel)

    try {
      logInfo("Init WordCountJobMain")

      val result = WordCountJob.run(args)

      if(result.isSuccess){
        val top = result.get

        logInfo(s"Top $topSize longest phrases")
        top._1.longestPhrases.foreach{ case (p: String, s: Int) => s"$p: $s characters" }

        logInfo(s"Top $topSize longest words")
        top._1.longestWords.foreach{ case (w: String) => s"$w: ${w.length} characters" }

        logInfo(s"Top $topSize common words")
        top._1.commonWords.foreach{ case (w: String, t:Int) => s"$w: $t times" }

        logInfo(s"Text stats: filesProcessed=${top._2.filesProcessed} and processedLines=${top._2.processedLines}")
        top._1.commonWords.foreach{ case (w: String, t:Int) => s"$w: $t times" }

        logInfo("End WordCountJobMain")
      }
      else {
        logError("Error executing WordCountJobMain: WordCountJob is failure")

        sys.exit(1)
      }
    }
    catch {
      case e: Exception =>
        logError("Error executing WordCountJobMain", e)

        sys.exit(1)
    }
  }
}
