package es.alvsanand.word_count_tech_challenge.utils

object StringUtils extends Logging {
  def isEmpty(s: String) = s == null || "".equals(s)

  def isNotEmpty(s: String) = !isEmpty(s)

  def getWords(s: String) = {
    var rs = Array(s)

    for(character <- "[]() ,!.;:") {
      rs = rs.flatMap(_.split(character))
    }

    rs
  }
}

