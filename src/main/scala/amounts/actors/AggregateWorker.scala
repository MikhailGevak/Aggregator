package amounts.actors

import akka.actor.Actor
import akka.actor.Props
import scala.collection.mutable
import scala.util.Try
import scala.util.Success
import scala.util.Failure

object AggregateWorker {
  case class BlockCalculation(blockIndex: Int, amounts: Map[String, BigDecimal])
  case class AggregateLines(blockIndex: Int, lines: Array[String])
  
  def props() = Props(classOf[AggregateWorker])
}

class AggregateWorker extends Actor {
  import AggregateWorker._
  
  def receive = {
    case AggregateLines(blockIndex, lines) =>
      val map = parseLines(lines)
      sender ! BlockCalculation(blockIndex, map.toMap)
  }

  private def parseLines(lines: Array[String]) = lines.foldLeft(mutable.Map[String, BigDecimal]()) { (map, line) =>
    parseLine(line) match {
      case Right((id, amount)) =>
        map.put(id, map.get(id).getOrElse(BigDecimal(0)) + amount)
      case Left(error) => println(error) //it have to be log or smth else.
    }
    map
  }

  private def parseLine(line: String) = {
    line.split(";") match {
      case Array(id, amount) =>
        Try { BigDecimal(amount) } match {
          case Success(amount) => Right((id, amount))
          case Failure(_) => Left(s"Error in line: $line")
        }
      case other => Left(s"Error in line: $line")
    }
  }
}