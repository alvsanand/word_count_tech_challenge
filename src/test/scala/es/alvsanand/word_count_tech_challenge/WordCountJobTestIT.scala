package es.alvsanand.word_count_tech_challenge

import es.alvsanand.word_count_tech_challenge.test.SparkTestTrait
import org.apache.ignite.spark.{IgniteContext, IgniteDataFrameSettings}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith

@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class WordCountJobTestIT extends SparkTestTrait {
  before {
  }
  after {
  }

  private val FILE_PATH = getClass.getResource("/test_files").getFile + "/*"
  private val TOP_SIZE = 5

  feature("WordCountJobTestIT") {
    scenario("Simple") {
      withSpark((sparkSession: SparkSession) => {
        withIgnite (List("PhraseSize", "WordSize", "WordCount"), sparkSession)((igniteContext: IgniteContext) => {
          Given("Config")
          val args = WordCountJobArguments(FILE_PATH, TOP_SIZE)

          When("Running job")
          val result = WordCountJob.run(args)

          Then("Match expected values")
          result.isSuccess should be(true)

          import sparkSession.implicits._

          sparkSession.read
            .format(IgniteDataFrameSettings.FORMAT_IGNITE)
            .option(IgniteDataFrameSettings.OPTION_TABLE, "PhraseSize")
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, igniteConfigFile)
            .load()
            .sort($"Size".desc)
            .take(TOP_SIZE)
            .map(r => (r.getAs[String]("PHRASE") -> r.getAs[Int]("SIZE"))) should contain theSameElementsAs (Array[(String, Integer)](
            "With bracelets of thy hair, rings, gawds, conceits," -> 51,
            "With cunning hast thou filch'd my daughter's heart;" -> 51,
            "Knacks, trifles, nosegays, sweetmeats, messengers" -> 49,
            "Thou, thou, Lysander, thou hast given her rimes," -> 48,
            "This man hath bewitch'd the bosom of my child:" -> 46
          ))

          sparkSession.read
            .format(IgniteDataFrameSettings.FORMAT_IGNITE)
            .option(IgniteDataFrameSettings.OPTION_TABLE, "WordSize")
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, igniteConfigFile)
            .load()
            .sort($"Size".desc, $"Word".asc)
            .take(TOP_SIZE)
            .map(_.getAs[String]("WORD")) should contain theSameElementsAs (Array[(String)]("interchang'd", "immediately", "love-tokens", "prevailment", "daughter's"))

          sparkSession.read
            .format(IgniteDataFrameSettings.FORMAT_IGNITE)
            .option(IgniteDataFrameSettings.OPTION_TABLE, "WordCount")
            .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, igniteConfigFile)
            .load()
            .sort($"Count".desc, $"Word".asc)
            .take(TOP_SIZE)
            .map(r=> (r.getAs[String]("WORD")->r.getAs[Int]("COUNT"))) should contain theSameElementsAs (Array[(String, Int)]("to" -> 10, "my" -> 9, "of" -> 8, "her" -> 7, "and" -> 6))

          result.get.filesProcessed should be (2L)
          result.get.processedLines should be (31L)
          result.get.wordCount should be (226L)
        })
      })
    }
  }
}


