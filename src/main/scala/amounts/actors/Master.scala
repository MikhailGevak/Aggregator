package amounts.actors

import akka.actor.Actor
import akka.actor.Props
import java.io.File
import java.io.PrintWriter

object Master {
  case class AggregateFromFile(fileName: String, blockSize: Int, numberOfWorkers: Int)

  def props(fileName: String) = Props(classOf[Master], fileName)
}

class Master(fileName: String) extends Actor {
  import Master._
  def receive = {
    case AggregateFromFile(fileName, blockSize, numberOfWorkers) =>
      println(s"Starting calculating from file ${new File(fileName).getAbsolutePath} using ${numberOfWorkers} workers by blocks by ${blockSize} bytes...")
      
      val amountMapCreator = context.actorOf(AmountMapCreator.props(fileName, blockSize, numberOfWorkers))
      amountMapCreator ! AmountMapCreator.Aggregate()
    case AmountMapCreator.Busy(startTime) =>
      println(s"A process already has been run at $startTime")
    case AmountMapCreator.AggregateResult(amounts, startTime, finishTime) =>
      println(s"Result is calculated for ${finishTime - startTime} ms")
      val file = new File(fileName)
      val fileWriter = new PrintWriter(file)
      try {
        amounts map { case (k, v) => fileWriter.println(s"$k;$v") }
      } finally {
        fileWriter.close
      }
      println(s"File ${file.getAbsolutePath} created! Thank you!")
      
      context.system.shutdown
  }
}