package util

import scala.util.Random
import java.io.File
import java.io.PrintWriter
/*
object FileGenerator extends App {
  override def main(args: Array[String]) {
    val fileName = args(0)
    val lines = args(1).toInt
    val uniqueIds = args(2).toInt

    println(s"fileName: $fileName")
    println(s"lines: $lines")
    println(s"uniqueIds: $uniqueIds")

    val fileWriter = new PrintWriter(new File(fileName))
    try {
      for (i <- 0 until lines) {
        fileWriter.println(generateLine(uniqueIds))
      }
    } finally {
      fileWriter.close
    }
  }

  def generateLine(uniqueIds: Int) = s"id-${Random.nextInt(uniqueIds)};${Random.nextInt(1000000)}.${Random.nextInt(100)}"
}*/