package amounts.actors

import akka.actor.Actor
import akka.actor.Props
import java.io.RandomAccessFile

object FileReaderWorker {
  case class ReadFromFile(blockIndex: Int)

  def props(fileName: String, blockSize: Int) = Props(classOf[FileReaderWorker], fileName, blockSize)
}

class FileReaderWorker(fileName: String, blockSize: Int) extends Actor {
  import FileReaderWorker._
  var byteBuffer = new Array[Byte](blockSize)
  val aggregateWorker = context.actorOf(AggregateWorker.props())

  def receive = {
    case ReadFromFile(blockIndex) =>
      aggregateWorker forward AggregateWorker.AggregateLines(blockIndex, readFromFile(blockIndex))
  }

  private def readFromFile(blockIndex: Int) = {
    val raf = new RandomAccessFile(fileName, "r")
    try {
      val seek = (blockIndex * blockSize - 1).max(0)//blocks should have 1 "crossing" element
      raf.seek(seek)
      raf.read(byteBuffer)
      val rawString = new String(byteBuffer) + raf.readLine() //the line should be reading until line separator
      if (blockIndex == 0) {
        rawString.split(System.getProperty("line.separator"))
      } else {
        rawString.split(System.getProperty("line.separator")).tail//remove first line. The line read by previous worker
      }
    } catch {
      case t: Throwable =>
        println(t.getLocalizedMessage)
        Array[String]()
    } finally {
      raf.close
    }
  }
}