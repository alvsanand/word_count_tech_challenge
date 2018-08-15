package es.alvsanand.word_count_tech_challenge

import es.alvsanand.word_count_tech_challenge.utils.{KafkaHelper, StringUtils}
import org.apache.ignite.spark.IgniteDataFrameSettings
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.StreamingContext
import scala.collection.JavaConverters._

case class WordCountStreamingJobArguments(topic:String, batchDuration: org.apache.spark.streaming.Duration) extends SparkJobArguments

object WordCountStreamingJob extends SparkJob[WordCountStreamingJobArguments, StreamingContext] {

  protected def doRun(args: WordCountStreamingJobArguments)(sparkSession: SparkSession): StreamingContext = {
    val streamingContext = new StreamingContext(sparkSession.sparkContext, args.batchDuration)

    val topic = args.topic

    val stream = createKafkaDirectStream[String, String, StringDeserializer, StringDeserializer](Array(topic))(sparkSession, streamingContext)

    stream.foreachRDD(rdd => {
      import sparkSession.implicits._

      val linesRDD = rdd.flatMap(r => KafkaHelper.parseSparkRecord[String, String](r)).map(_.value)

      val filteredLinesRDD = WordCountJob.getFilteredRDD(linesRDD)

      WordCountJob.getPhraseSizesRDD(filteredLinesRDD)
        .toDF("Phrase", "Size")
        .write.format(FORMAT_IGNITE)
        .option(OPTION_CONFIG_FILE, igniteConfigFile)
        .option(OPTION_TABLE, "PhraseSize")
        .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "Phrase")
        .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1")
        .option(OPTION_STREAMER_ALLOW_OVERWRITE, "true")
        .mode(SaveMode.Append)
        .save()

      val words = WordCountJob.getWords(filteredLinesRDD)

      WordCountJob.getLongestWordsRDD(words)
        .toDF("Word", "Size")
        .write.format(FORMAT_IGNITE)
        .option(OPTION_CONFIG_FILE, igniteConfigFile)
        .option(OPTION_TABLE, "WordSize")
        .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "Word")
        .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1")
        .option(OPTION_STREAMER_ALLOW_OVERWRITE, "true")
        .mode(SaveMode.Append)
        .save()


      val currentWordCount = WordCountJob.getWordsCountRDD(words)
        .toDF("Word", "Count")

      val igniteContext = getIgniteContext(sparkSession)

      val wordCountDF = if(igniteContext.ignite().cacheNames().asScala.filter(_.endsWith("WordCount".toUpperCase)).size>0){
        val previousWordCount = sparkSession.read
          .format(IgniteDataFrameSettings.FORMAT_IGNITE)
          .option(IgniteDataFrameSettings.OPTION_TABLE, "WordCount")
          .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, igniteConfigFile)
          .load()

        currentWordCount.join(previousWordCount, currentWordCount("Word") === previousWordCount("Word"), "left_outer")
          .select(currentWordCount("Word") as "Word", (currentWordCount("Count") + previousWordCount("Count")) as "Count")
          .select($"Word", $"Count")
      }
      else{
        currentWordCount
      }

      wordCountDF.write.format(FORMAT_IGNITE)
        .option(OPTION_CONFIG_FILE, igniteConfigFile)
        .option(OPTION_TABLE, "WordCount")
        .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "Word")
        .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1")
        .option(OPTION_STREAMER_ALLOW_OVERWRITE, "true")
        .mode(SaveMode.Append)
        .save()
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

