package es.alvsanand.word_count_tech_challenge

import es.alvsanand.word_count_tech_challenge.test.SparkTestTrait
import es.alvsanand.word_count_tech_challenge.utils.StringUtils
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith

@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class WordCountJobTest extends SparkTestTrait {
  before {
  }
  after {
  }

  private val ALERT_THRESHOLD: Double = 0.1

  feature("Test RDD methods") {
    scenario("getFilteredRDD") {
      withSpark((sparkSession: SparkSession) => {
        Given("RDD")
        val lines = Array(
          "<THESEUS>	<1%>",
          "\tNow, fair Hippolyta, our nuptial hour",
          "\tDraws on apace: four happy days bring in",
          "\tAnother moon; but O! methinks how slow",
          "\tThis old moon wanes; she lingers my desires,",
          "\tLike to a step dame, or a dowager",
          "\tLong withering out a young man's revenue.",
          "</THESEUS>",
          ""
        )
        val filteredLines = Array(
          "Now, fair Hippolyta, our nuptial hour",
          "Draws on apace: four happy days bring in",
          "Another moon; but O! methinks how slow",
          "This old moon wanes; she lingers my desires,",
          "Like to a step dame, or a dowager",
          "Long withering out a young man's revenue."
        )

        val rdd = sparkSession.sparkContext.parallelize(lines)

        When("Running getFilteredRDD")
        val result = WordCountJob.getFilteredRDD(rdd)

        Then("Match expected values")

        result.collect should contain theSameElementsAs (filteredLines)
      })
    }
    scenario("getWords") {
      withSpark((sparkSession: SparkSession) => {
        Given("RDD")
        val lines = Array(
          "Now, fair Hippolyta, our nuptial hour",
          "Draws on apace: four happy days bring in"
        )
        val words = lines.flatMap(StringUtils.getWords(_)).map(_.toLowerCase)

        val rdd = sparkSession.sparkContext.parallelize(lines)

        When("Running getWords")
        val result = WordCountJob.getWords(rdd)

        Then("Match expected values")

        result.collect should contain theSameElementsAs (words)
      })
    }
    scenario("getPhraseSizesRDD") {
      withSpark((sparkSession: SparkSession) => {
        Given("RDD")
        val lines = Array(
          "Now, fair Hippolyta, our nuptial hour",
          "Draws on apace: four happy days bring in",
          "Another moon; but O! methinks how slow",
          "This old moon wanes; she lingers my desires,",
          "Like to a step dame, or a dowager",
          "Long withering out a young man's revenue."
        )
        val sizes = lines.map(l=> (l, l.length)).sortWith(_._2 > _._2)

        val rdd = sparkSession.sparkContext.parallelize(lines)

        When("Running getPhraseSizesRDD")
        val result = WordCountJob.getPhraseSizesRDD(rdd)

        Then("Match expected values")

        result.collect should contain theSameElementsAs (sizes)
      })
    }
    scenario("getLongestWordsRDD") {
      withSpark((sparkSession: SparkSession) => {
        Given("RDD")
        val lines = Array(
          "Now, fair Hippolyta, our nuptial hour",
          "Draws on apace: four happy days bring in"
        ).flatMap(StringUtils.getWords(_))
          .map(_.toLowerCase)
        val words = Array("hippolyta", "nuptial", "bring", "apace", "happy", "draws", "hour", "fair", "four", "days", "our", "now", "on", "in")
          .map(w => w.toLowerCase -> w.length)

        val rdd = sparkSession.sparkContext.parallelize(lines)

        When("Running getLongestWordsRDD")
        val result = WordCountJob.getLongestWordsRDD(rdd)

        Then("Match expected values")

        result.collect should contain theSameElementsAs (words)
      })
    }
    scenario("getWordsCountRDD") {
      withSpark((sparkSession: SparkSession) => {
        Given("RDD")
        val lines = Array(
          "Now, fair Hippolyta",
          "now, fair",
          "Now"
        ).flatMap(StringUtils.getWords(_)).map(_.toLowerCase)
        val words = Array("now" -> 3, "fair" -> 2, "hippolyta" -> 1)

        val rdd = sparkSession.sparkContext.parallelize(lines)

        When("Running getWordsCountRDD")
        val result = WordCountJob.getWordsCountRDD(rdd)

        Then("Match expected values")

        result.collect should contain theSameElementsAs (words)
      })
    }
  }
}


