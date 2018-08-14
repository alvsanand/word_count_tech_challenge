package es.alvsanand.word_count_tech_challenge

import es.alvsanand.word_count_tech_challenge.utils.{KafkaHelper, StringUtils}
import org.apache.ignite.spark.IgniteContext
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

case class WordCountStreamingJobArguments(topic:String, batchDuration: org.apache.spark.streaming.Duration) extends SparkJobArguments

object WordCountStreamingJob extends SparkJob[WordCountStreamingJobArguments, StreamingContext] {
  protected def doRun(args: WordCountStreamingJobArguments)(sparkSession: SparkSession): StreamingContext = {
    val streamingContext = new StreamingContext(sparkSession.sparkContext, args.batchDuration)

    val topic = args.topic

    val stream = createKafkaDirectStream[String, String, StringDeserializer, StringDeserializer](Array(topic))(sparkSession, streamingContext)

    val igniteContext = new IgniteContext(sparkSession.sparkContext, "ignite_config.xml")

    stream.foreachRDD(rdd => {
      val linesRDD = rdd.flatMap(r => KafkaHelper.parseSparkRecord[String, String](r)).map(_.value)

      val filteredLinesRDD = WordCountJob.getFilteredRDD(linesRDD)

      val phrasesSizesRDD = WordCountJob.getPhraseSizesRDD(filteredLinesRDD)
      igniteContext.fromCache("longestPhrases").savePairs(phrasesSizesRDD)

      val words = WordCountJob.getWords(filteredLinesRDD)

      val longestWordsRDD = WordCountJob.getLongestWordsRDD(words).map(w=>(w, w.length))
      igniteContext.fromCache("longestWords").savePairs(longestWordsRDD)

      val wordsCountRDD = WordCountJob.getWordsCountRDD(words)
      wordsCountRDD.foreach(w => {
        val previousCount: Option[Integer] = Option(igniteContext.ignite().cache[String, Integer]("commonWords").get(w._1))
        val newCount = previousCount.getOrElse(0).asInstanceOf[Integer] + w._2
        igniteContext.ignite().cache[String, Integer]("commonWords").put(w._1, newCount)
      })
    })

    streamingContext.start()

    streamingContext
  }

  def validateArguments(args: WordCountStreamingJobArguments): Unit = {
    if (args == null || StringUtils.isEmpty(args.topic)) {
      throw new IllegalArgumentException("topic cannot be empty")
    }
    if (args == null || args.batchDuration == null) {
      throw new IllegalArgumentException("batchDuration cannot be empty")
    }
  }
}

