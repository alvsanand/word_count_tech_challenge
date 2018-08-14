package es.alvsanand.word_count_tech_challenge.utils

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

case class LogTime[B](time: Long, result: Try[B])

trait Logging{
  protected def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  private val log = LoggerFactory.getLogger(logName)

  // Log methods that take only a String
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  protected def logTime[B](message: String, quiet: Boolean = false)(fun: => B): LogTime[B] = {
    logInfo(s"[INIT] $message")
    val initTime = System.currentTimeMillis()

    val result = Try(fun)

    val spentTime = System.currentTimeMillis() - initTime

    result match {
      case Success(_) => logInfo(s"[OK:$spentTime}] $message")
      case Failure(e) => {
        logError(s"[ERROR:$spentTime}] $message", e)
        if(!quiet){
          throw e
        }
      }
    }

    LogTime[B](spentTime, result)
  }
}

