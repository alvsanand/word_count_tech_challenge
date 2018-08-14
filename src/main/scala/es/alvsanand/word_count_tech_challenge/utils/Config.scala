package es.alvsanand.word_count_tech_challenge.utils

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, Config => TypesafeConfig}

import scala.collection.JavaConversions._

class Config private(private val config: TypesafeConfig){
  def get(path: String): Option[String] = {
    if(config.hasPath(path)) {
      Some(config.getString(path))
    }
    else{
      None
    }
  }

  def getList(path: String): Option[List[String]] = {
    if(config.hasPath(path)) {
      Some(config.getStringList(path).toList)
    }
    else{
      None
    }
  }

  def getAnyList(path: String): Option[List[Any]] = {
    if(config.hasPath(path)) {
      Some(config.getAnyRefList(path).toList)
    }
    else{
      None
    }
  }

  private val FILTER_REGEX = ".*(pass|password)"

  override def toString: String = {
    config.entrySet()
      .map(e => (e.getKey, if (!e.getKey.matches(FILTER_REGEX)) {
        e.getValue.render(ConfigRenderOptions.concise())
      } else {
        "***"
      })).map(e => s"${e._1}: ${e._2}")
      .toSeq.sorted.mkString("\n")
  }
}

object Config extends Logging{
  private var _config = new Config(ConfigFactory.load())

  logInfo(s"Loaded Config")

  private def getConfigToPrint() = {
    _config.config.entrySet()
  }

  def apply(reload: Boolean = false): Config = {
    this.synchronized {
      if (_config == null || reload) {
        ConfigFactory.invalidateCaches()

        _config = new Config(ConfigFactory.load())
      }

      _config
    }
  }

  def apply(configFile: String): Config = {
    ConfigFactory.invalidateCaches()

    new Config(ConfigFactory.load(configFile))
  }
}

