package es.alvsanand.word_count_tech_challenge

import es.alvsanand.word_count_tech_challenge.test.SparkTestTrait
import org.apache.ignite.spark.IgniteContext
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Milliseconds
import org.junit.runner.RunWith

@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class WordCountStreamingJobStreamingTestIT extends SparkTestTrait {
  before {
  }
  after {
  }

  private val TOPIC = "word-count-topic"
  private val BATCH_DURATION = Milliseconds(1000)
  private val TOP_SIZE = 5

  feature("WordCountStreamingJobTestIT") {
    scenario("Simple") {
      withKafka(fun = (producer, consumer) => {
        withSpark((sparkSession: SparkSession) => {
          withIgnite (sparkSession)((igniteContext: IgniteContext) => {
            Given("Config")
            val args = WordCountStreamingJobArguments(TOPIC, BATCH_DURATION)
            val lines = Array(
              "<THESEUS>	<3%>",
              "	What say you, Hermia? be advis'd, fair maid.",
              "	To you, your father should be as a god;",
              "	One that compos'd your beauties, yea, and one",
              "	To whom you are but as a form in wax",
              "	By him imprinted, and within his power",
              "	To leave the figure or disfigure it.",
              "	Demetrius is a worthy gentleman.",
              "</THESEUS>",
              "<EGEUS>	<2%>",
              "	Full of vexation come I, with complaint",
              "	Against my child, my daughter Hermia.",
              "	Stand forth, Demetrius. My noble lord,",
              "	This man hath my consent to marry her.",
              "	Stand forth, Lysander: and, my gracious duke,",
              "	This man hath bewitch'd the bosom of my child:",
              "	Thou, thou, Lysander, thou hast given her rimes,",
              "	And interchang'd love-tokens with my child;",
              "	Thou hast by moonlight at her window sung,",
              "	With feigning voice, verses of feigning love;",
              "	And stol'n the impression of her fantasy",
              "	With bracelets of thy hair, rings, gawds, conceits,",
              "	Knacks, trifles, nosegays, sweetmeats, messengers",
              "	Of strong prevailment in unharden'd youth;",
              "	With cunning hast thou filch'd my daughter's heart;",
              "	Turn'd her obedience, which is due to me,",
              "	To stubborn harshness. And, my gracious duke,",
              "	Be it so she will not here before your Grace",
              "	Consent to marry with Demetrius,",
              "	I beg the ancient privilege of Athens,",
              "	As she is mine, I may dispose of her;",
              "	Which shall be either to this gentleman,",
              "	Or to her death, according to our law",
              "	Immediately provided in that case.",
              "</EGEUS>"
            )

            igniteContext.ignite().cache[String, Long]("longestPhrases").clear
            igniteContext.ignite().cache[String, Integer]("longestWords").clear
            igniteContext.ignite().cache[String, Long]("commonWords").clear

            When("Running job")
            val result = WordCountStreamingJob.run(args)

            Thread.sleep(1000L)

            for (l <- lines) {
              producer.send(new ProducerRecord[String, String](TOPIC, l))
            }

            Thread.sleep(10000L)

            Then("Match expected values")
            result.isSuccess should be(true)

            result.get.stop(false)
            Thread.sleep(5000L)

            igniteContext.fromCache[String, Integer]("longestPhrases").sortBy(_._2, false).take(TOP_SIZE) should contain theSameElementsAs (Array[(String, Integer)](
              "With bracelets of thy hair, rings, gawds, conceits," -> 51,
              "With cunning hast thou filch'd my daughter's heart;" -> 51,
              "Knacks, trifles, nosegays, sweetmeats, messengers" -> 49,
              "Thou, thou, Lysander, thou hast given her rimes," -> 48,
              "This man hath bewitch'd the bosom of my child:" -> 46
            ))

            igniteContext.fromCache[String, Integer]("longestWords").sortBy(_._2, false).take(TOP_SIZE).map(_._1) should contain theSameElementsAs (Array[(String)]("interchang'd", "love-tokens", "prevailment", "immediately", "impression"))
            igniteContext.fromCache[String, Integer]("commonWords").sortBy(_._2, false).take(TOP_SIZE) should contain theSameElementsAs (Array[(String, Int)]("to" -> 10, "my" -> 9, "of" -> 8, "her" -> 7, "with" -> 6))
          })
        })
      })
    }
    scenario("Multiple Batches") {
      withKafka(fun = (producer, consumer) => {
        withSpark((sparkSession: SparkSession) => {
          withIgnite (sparkSession)((igniteContext: IgniteContext) => {
            Given("Config")
            val args = WordCountStreamingJobArguments(TOPIC, BATCH_DURATION)
            val lines = Array(
              "<THESEUS>	<3%>",
              "	What say you, Hermia? be advis'd, fair maid.",
              "	To you, your father should be as a god;",
              "	One that compos'd your beauties, yea, and one",
              "	To whom you are but as a form in wax",
              "	By him imprinted, and within his power",
              "	To leave the figure or disfigure it.",
              "	Demetrius is a worthy gentleman.",
              "</THESEUS>",
              "<EGEUS>	<2%>",
              "	Full of vexation come I, with complaint",
              "	Against my child, my daughter Hermia.",
              "	Stand forth, Demetrius. My noble lord,",
              "	This man hath my consent to marry her.",
              "	Stand forth, Lysander: and, my gracious duke,",
              "	This man hath bewitch'd the bosom of my child:",
              "	Thou, thou, Lysander, thou hast given her rimes,",
              "	And interchang'd love-tokens with my child;",
              "	Thou hast by moonlight at her window sung,",
              "	With feigning voice, verses of feigning love;",
              "	And stol'n the impression of her fantasy",
              "	With bracelets of thy hair, rings, gawds, conceits,",
              "	Knacks, trifles, nosegays, sweetmeats, messengers",
              "	Of strong prevailment in unharden'd youth;",
              "	With cunning hast thou filch'd my daughter's heart;",
              "	Turn'd her obedience, which is due to me,",
              "	To stubborn harshness. And, my gracious duke,",
              "	Be it so she will not here before your Grace",
              "	Consent to marry with Demetrius,",
              "	I beg the ancient privilege of Athens,",
              "	As she is mine, I may dispose of her;",
              "	Which shall be either to this gentleman,",
              "	Or to her death, according to our law",
              "	Immediately provided in that case.",
              "</EGEUS>"
            )

            igniteContext.ignite().cache[String, Long]("longestPhrases").clear
            igniteContext.ignite().cache[String, Integer]("longestWords").clear
            igniteContext.ignite().cache[String, Long]("commonWords").clear

            When("Running job")
            val result = WordCountStreamingJob.run(args)

            Thread.sleep(1000L)

            for (l <- lines) {
              producer.send(new ProducerRecord[String, String](TOPIC, l))
            }

            Thread.sleep(5000L)

            for (l <- lines) {
              producer.send(new ProducerRecord[String, String](TOPIC, l))
            }

            Thread.sleep(10000L)

            Then("Match expected values")
            result.isSuccess should be(true)

            result.get.stop(false)
            Thread.sleep(5000L)

            igniteContext.fromCache[String, Integer]("longestPhrases").sortBy(_._2, false).take(TOP_SIZE) should contain theSameElementsAs (Array[(String, Integer)](
              "With bracelets of thy hair, rings, gawds, conceits," -> 51,
              "With cunning hast thou filch'd my daughter's heart;" -> 51,
              "Knacks, trifles, nosegays, sweetmeats, messengers" -> 49,
              "Thou, thou, Lysander, thou hast given her rimes," -> 48,
              "This man hath bewitch'd the bosom of my child:" -> 46
            ))

            val tmp = igniteContext.fromCache[String, Integer]("longestWords").sortBy(_._2, false).take(TOP_SIZE).map(_._1)
            tmp should contain theSameElementsAs (Array[(String)]("interchang'd", "love-tokens", "prevailment", "immediately", "impression"))
            igniteContext.fromCache[String, Integer]("commonWords").sortBy(_._2, false).take(TOP_SIZE) should contain theSameElementsAs (Array[(String, Int)]("to" -> 20, "my" -> 18, "of" -> 16, "her" -> 14, "with" -> 12))
          })
        })
      })
    }
  }
}


