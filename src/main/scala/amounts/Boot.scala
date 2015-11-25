package amounts

import akka.actor.ActorSystem
import com.typesafe.config.Config
import akka.pattern.ask
import scala.concurrent.duration._
import akka.util.Timeout
import java.io.File
import java.io.PrintWriter
import actors.AmountMapCreator._
import actors.AmountMapCreator
import actors.Master

object Boot extends scala.App {
  implicit val timeout = Timeout(20.minutes)

  override def main(args: Array[String]) {
    val argsMap = (args map { x =>
      val value = x.split("=")
      if (value.size == 2) Some(value(0) -> value(1)) else None
    } flatten).toMap

    val inputFileName = argsMap("input")
    val outputFileName = argsMap("output")
    val blockSize = argsMap("blockSize").toInt
    val numberOfWorkers = argsMap("workers").toInt

    val system = ActorSystem("Aggregator")

    implicit val executionContext = system.dispatcher

    val master = system.actorOf(Master.props(outputFileName))

    master ! Master.AggregateFromFile(inputFileName, blockSize, numberOfWorkers)
  }
}