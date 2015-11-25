package amounts.actors

import org.scalatest.BeforeAndAfterAll
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.FunSuiteLike
import org.scalatest.Matchers
import akka.actor.ActorSystem
import scala.concurrent.duration._
import AmountMapCreator._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class MapCreatorTest extends TestKit(ActorSystem()) with FunSuiteLike with BeforeAndAfterAll with ImplicitSender
  with Matchers {
  implicit val timeout = 5.seconds

  test("simplefile.txt") {
    val master = system.actorOf(AmountMapCreator.props("examples/simplefile.txt", 10))

    master ! Aggregate()

    expectMsgPF() {
      case AggregateResult(map, startTime, finishTime) =>
        println(s"Time for calculating: ${finishTime - startTime} ms")
        map should contain theSameElementsAs Map("id-3" -> 511.35, "id-2" -> 23.45, "id-1" -> 1032121.35, "id-4" -> 15.364)

    }

    system.stop(master)
  }

  test("mediumfile.txt") {
    val master = system.actorOf(AmountMapCreator.props("examples/mediumfile.txt", 10))

    master ! Aggregate()

    expectMsgPF() {
      case AggregateResult(map, startTime, finishTime) =>
        println(s"Time for calculating: ${finishTime - startTime} ms")
        map should contain theSameElementsAs Map("id-3" -> 108917.55, "id-2" -> 4994.85, "id-1" -> 219841847.55, "id-4" -> 3272.532)

    }

    system.stop(master)
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  test("errorfile.txt") {
    val master = system.actorOf(AmountMapCreator.props("examples/errorfile.txt", 10))

    master ! Aggregate()

    expectMsgPF() {
      case AggregateResult(map, startTime, finishTime) =>
        println(s"Time for calculating: ${finishTime - startTime} ms")
        map should contain theSameElementsAs Map("id-3" -> 511.35, "id-2" -> 23.45, "id-1" -> 1032121.35, "id-4" -> 15.364)

    }

    system.stop(master)
  }

  test("bigfile.txt") {
    val master = system.actorOf(AmountMapCreator.props("examples/bigfile.txt", 1024))

    master ! Aggregate()

    expectMsgPF() {
      case AggregateResult(map, startTime, finishTime) =>
        println(s"Time for calculating: ${finishTime - startTime} ms")
    }

    system.stop(master)
  }

  test("bigfile.txt and 4 workers") {
    val master = system.actorOf(AmountMapCreator.props("examples/bigfile.txt", 1024, 4))

    master ! Aggregate()

    expectMsgPF() {
      case AggregateResult(map, startTime, finishTime) =>
        println(s"Time for calculating: ${finishTime - startTime} ms")
    }

    system.stop(master)
  }

  test("bigfile.txt and 7 workers") {
    val master = system.actorOf(AmountMapCreator.props("examples/bigfile.txt", 1024, 7))

    master ! Aggregate()

    expectMsgPF() {
      case AggregateResult(map, startTime, finishTime) =>
        println(s"Time for calculating: ${finishTime - startTime} ms")
    }

    system.stop(master)
  }

  test("bigfile.txt and 9 workers") {
    val master = system.actorOf(AmountMapCreator.props("examples/bigfile.txt", 1024, 9))

    master ! Aggregate()

    expectMsgPF() {
      case AggregateResult(map, startTime, finishTime) =>
        println(s"Time for calculating: ${finishTime - startTime} ms")
    }

    system.stop(master)
  }
}