package es.alvsanand.word_count_tech_challenge

import es.alvsanand.word_count_tech_challenge.test.SparkTestTrait
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
        Given("Config")
        val args = WordCountJobArguments(FILE_PATH, TOP_SIZE)

        When("Running job")
        val result = WordCountJob.run(args)

        Then("Match expected values")
        result.isSuccess should be(true)

        result.get._1.longestPhrases should contain theSameElementsAs (Array[(String, Int)](
          "With bracelets of thy hair, rings, gawds, conceits," -> 51,
          "With cunning hast thou filch'd my daughter's heart;" -> 51,
          "Knacks, trifles, nosegays, sweetmeats, messengers" -> 49,
          "Thou, thou, Lysander, thou hast given her rimes," -> 48,
          "This man hath bewitch'd the bosom of my child:" -> 46
        ))
        result.get._1.longestWords should contain theSameElementsAs (Array[String]("interchang'd", "love-tokens", "prevailment", "immediately", "impression"))
        result.get._1.commonWords should contain theSameElementsAs (Array[(String, Int)]("to" -> 10, "my" -> 9, "of" -> 8, "her" -> 7, "with" -> 6))

        result.get._2.filesProcessed should be (2L)
        result.get._2.processedLines should be (31L)
        result.get._2.wordCount should be (226L)
      })
    }
  }
}


