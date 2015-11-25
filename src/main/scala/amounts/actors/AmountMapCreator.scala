package amounts.actors

import akka.actor.Actor
import java.io.RandomAccessFile
import scala.collection.mutable
import akka.actor.ActorRef
import akka.actor.Props
import java.util.Calendar
import java.io.File
import akka.routing.RoundRobinRouter

object AmountMapCreator {
  case class Aggregate()
  case class Busy(fromTime: Long)
  case class AggregateResult(amounts: Map[String, BigDecimal], startTime: Long, finishTime: Long)
  
  def props(fileName: String, blockSize: Int = 1024 * 1024, numberOfWorkers: Int = 1) = Props(classOf[AmountMapCreator], fileName, blockSize, numberOfWorkers)
}

class AmountMapCreator(fileName: String, blockSize: Int, numberOfWorkers: Int) extends Actor {
  import AmountMapCreator._
  private val aggregateResult = mutable.Map[String, BigDecimal]()
  private var blocksCount = 0
  private var client: ActorRef = _
  private var startTime: Long = _
  
  private def now = Calendar.getInstance.getTimeInMillis
  
  val numberOfBlocks = {
    val raf = new RandomAccessFile(fileName, "r")
    try {
      (raf.length / blockSize).toInt
    } finally {
      raf.close
    }
  }

  val worker = context.actorOf(FileReaderWorker.props(fileName, blockSize).withRouter(RoundRobinRouter(numberOfWorkers)))

  def receiveCalculating: Receive = {
    case Aggregate() =>
      sender ! Busy(startTime)
    case AggregateWorker.BlockCalculation(blockIndex, amounts) =>
      amounts foreach { case (id, amount) => aggregateResult.put(id, amount + aggregateResult.get(id).getOrElse(0)) }
      blocksCount += 1

      if (blocksCount == numberOfBlocks) {//time to return result!
        client ! AggregateResult(aggregateResult.toMap, startTime, now)
        context.stop(self)
      }

  }

  def receive = {
    case Aggregate() =>
      client = sender
      blocksCount = 0
      startTime = now
      context become receiveCalculating
      println("Sending messages to workers...")
      for (i <- 0 until numberOfBlocks) worker ! FileReaderWorker.ReadFromFile(i)
  }

}