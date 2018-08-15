package es.alvsanand.word_count_tech_challenge


import es.alvsanand.word_count_tech_challenge.WordCountStreamingJob.igniteConfigFile
import es.alvsanand.word_count_tech_challenge.utils.StringUtils
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

case class WordCountJobArguments(filesPath:String, topSize: Int = 5, cache: Option[StorageLevel] = Option(StorageLevel.MEMORY_ONLY_SER)) extends SparkJobArguments

case class WordCountTop(longestPhrases: Array[(String, Int)],
                        longestWords: Array[String],
                        commonWords: Array[(String, Int)])
case class WordCountStats(filesProcessed: Long,
                          processedLines: Long,
                          wordCount: Long)


object WordCountJob extends SparkJob[WordCountJobArguments, WordCountStats] {
  protected def doRun(args: WordCountJobArguments)(sparkSession: SparkSession): WordCountStats = {
    val filesPath = args.filesPath
    val topSize = args.topSize
    val cacheType = args.cache

    val linesRDD = sparkSession.sparkContext.textFile(filesPath)
    val filesProcessed = sparkSession.sparkContext.wholeTextFiles(filesPath).count

    import sparkSession.implicits._

    val filteredLinesRDD = if(cacheType.isDefined) {
      getFilteredRDD(linesRDD).persist(cacheType.get)
    }
    else{
      getFilteredRDD(linesRDD)
    }
    val processedLines = filteredLinesRDD.count()

    getPhraseSizesRDD(filteredLinesRDD)
      .toDF("Phrase", "Size")
      .write.format(FORMAT_IGNITE)
      .option(OPTION_CONFIG_FILE, igniteConfigFile)
      .option(OPTION_TABLE, "PhraseSize")
      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "Phrase")
      .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1")
      .option(OPTION_STREAMER_ALLOW_OVERWRITE, "true")
      .mode(SaveMode.Append)
      .save()

    val words = if(cacheType.isDefined) {
      val tmp = getWords(filteredLinesRDD).persist(cacheType.get)

      filteredLinesRDD.unpersist()

      tmp
    }
    else {
      getWords(filteredLinesRDD)
    }
    val wordCount = words.count()

    getLongestWordsRDD(words)
      .toDF("Word", "Size")
      .write.format(FORMAT_IGNITE)
      .option(OPTION_CONFIG_FILE, igniteConfigFile)
      .option(OPTION_TABLE, "WordSize")
      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "Word")
      .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1")
      .option(OPTION_STREAMER_ALLOW_OVERWRITE, "true")
      .mode(SaveMode.Append)
      .save()

    getWordsCountRDD(words)
      .toDF("Word", "Count")
      .write.format(FORMAT_IGNITE)
      .option(OPTION_CONFIG_FILE, igniteConfigFile)
      .option(OPTION_TABLE, "WordCount")
      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "Word")
      .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1")
      .option(OPTION_STREAMER_ALLOW_OVERWRITE, "true")
      .mode(SaveMode.Append)
      .save()

    WordCountStats(filesProcessed, processedLines, wordCount)
  }

  def getFilteredRDD(rdd: RDD[String]) = rdd.filter(l => l.length != 0 && !l.startsWith("<"))
    .map(_.trim)

  def getWords(rdd: RDD[String]) = rdd.flatMap(StringUtils.getWords(_)).map(_.toLowerCase)

  def getPhraseSizesRDD(rdd: RDD[String]) = rdd.map(p => (p, p.length))
    .distinct()

  def getLongestWordsRDD(rdd: RDD[String]) = rdd.distinct()
    .map(w => (w, w.length))

  def getWordsCountRDD(rdd: RDD[String]) = rdd.map(w => (w, 1))
    .reduceByKey(_+_)

  def validateArguments(args: WordCountJobArguments): Unit = {
    if (args == null || StringUtils.isEmpty(args.filesPath)) {
      throw new IllegalArgumentException("filesPath cannot be empty")
    }
  }
}

