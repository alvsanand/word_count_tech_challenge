package es.alvsanand.word_count_tech_challenge


import es.alvsanand.word_count_tech_challenge.utils.StringUtils
import es.alvsanand.word_count_tech_challenge.{SparkJob, SparkJobArguments}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

case class WordCountJobArguments(filesPath:String, topSize: Int = 5, cache: Option[StorageLevel] = Option(StorageLevel.MEMORY_ONLY_SER)) extends SparkJobArguments

case class WordCountTop(longestPhrases: Array[(String, Int)],
                        longestWords: Array[String],
                        commonWords: Array[(String, Int)])
case class WordCountStats(filesProcessed: Long,
                          processedLines: Long,
                          wordCount: Long)


object WordCountJob extends SparkJob[WordCountJobArguments, (WordCountTop, WordCountStats)] {
  protected def doRun(args: WordCountJobArguments)(sparkSession: SparkSession): (WordCountTop, WordCountStats) = {
    val filesPath = args.filesPath
    val topSize = args.topSize
    val cacheType = args.cache

    val linesRDD = sparkSession.sparkContext.textFile(filesPath)
    val filesProcessed = sparkSession.sparkContext.wholeTextFiles(filesPath).count

    val filteredLinesRDD = if(cacheType.isDefined) {
      getFilteredRDD(linesRDD).persist(cacheType.get)
    }
    else{
      getFilteredRDD(linesRDD)
    }
    val processedLines = filteredLinesRDD.count()

    val phrasesSizesRDD = getPhraseSizesRDD(filteredLinesRDD)

    val words = if(cacheType.isDefined) {
      val tmp = getWords(filteredLinesRDD).persist(cacheType.get)

      filteredLinesRDD.unpersist()

      tmp
    }
    else {
      getWords(filteredLinesRDD)
    }
    val wordCount = words.count()

    val longestWordsRDD = getLongestWordsRDD(words)

    val wordsCountRDD = getWordsCountRDD(words)

    (WordCountTop(phrasesSizesRDD.take(topSize), longestWordsRDD.take(topSize), wordsCountRDD.take(topSize)), WordCountStats(filesProcessed, processedLines, wordCount))
  }

  def getFilteredRDD(rdd: RDD[String]) = rdd.filter(l => l.length != 0 && !l.startsWith("<"))
    .map(_.trim)

  def getWords(rdd: RDD[String]) = rdd.flatMap(StringUtils.getWords(_)).map(_.toLowerCase)

  def getPhraseSizesRDD(rdd: RDD[String]) = rdd.map(p => (p, p.length))
    .distinct()
    .sortBy(_._2, false)

  def getLongestWordsRDD(rdd: RDD[String]) = rdd.distinct()
    .sortBy(_.length, false)

  def getWordsCountRDD(rdd: RDD[String]) = rdd.map(w => (w, 1))
    .reduceByKey(_+_)
    .sortBy(_._2, false)

  def validateArguments(args: WordCountJobArguments): Unit = {
    if (args == null || StringUtils.isEmpty(args.filesPath)) {
      throw new IllegalArgumentException("filesPath cannot be empty")
    }
  }
}

