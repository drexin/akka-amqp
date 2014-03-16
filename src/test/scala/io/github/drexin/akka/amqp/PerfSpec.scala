package io.github.drexin.akka.amqp

import akka.testkit.{TestProbe, DefaultTimeout, TestKit}
import akka.actor.ActorSystem
import akka.pattern.ask
import scala.concurrent.duration._
import org.scalatest.{WordSpecLike, BeforeAndAfterAll, Matchers}
import scala.concurrent.{Future, Await}
import io.github.drexin.akka.amqp.AMQP._
import akka.io.IO
import com.typesafe.config.ConfigFactory

class PerfSpec extends TestKit(ActorSystem("TestSystem", ConfigFactory.parseString("akka.log-dead-letters = OFF"))) with DefaultTimeout with WordSpecLike with Matchers with BeforeAndAfterAll {

  val amqp = IO(AMQP)
  val connection = Await.result((amqp ? Connect("amqp://guest:guest@localhost:5672")).mapTo[Connected], 5.seconds).connection

  "A PubSub system" should {
    "should have a reasonable throughput" in {
      Await.ready(connection ? DeclareExchange(name = "test-exchange", tpe = "topic", autoDelete = true), 5.seconds)

      Await.ready(connection ? DeclareQueue(name = "test-perf-queue", autoDelete = true), 5.seconds)

      Await.ready(connection ? BindQueue("test-perf-queue", "test-exchange", "test"), 5.seconds)

      val probe = TestProbe()

      connection.tell(Subscribe(queue = "test-perf-queue", autoAck = true), probe.ref)

      import system.dispatcher

      Future {
        for (_ <- 0 until 1000000)
          connection ! Publish("test-exchange", "test", "foo".getBytes)
      }

      val start = System.currentTimeMillis()
      val res = probe.receiveWhile(10.seconds) { case x => x }
      val stop = System.currentTimeMillis()

      println(s"Received ${res.size} messages in ${stop - start} milliseconds.")
    }
  }

  override protected def afterAll(): Unit = system.shutdown()
}
