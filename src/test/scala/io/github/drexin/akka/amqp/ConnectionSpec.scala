package io.github.drexin.akka.amqp

import akka.testkit.{DefaultTimeout, TestKit}
import akka.actor.ActorSystem
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import akka.io.IO
import scala.concurrent.Await
import akka.pattern.ask
import scala.concurrent.duration._
import com.rabbitmq.client.ConnectionFactory
import java.nio.charset.Charset

class ConnectionSpec extends TestKit(ActorSystem("TestSystem")) with DefaultTimeout with WordSpecLike with Matchers with BeforeAndAfterAll {
  import AMQP._

  val amqp = IO(AMQP)
  val connection = Await.result((amqp ? Connect("amqp://guest:guest@localhost:5672")).mapTo[Connected], 5.seconds).connection

  val rawConnection = new ConnectionFactory().newConnection()
  val rawChannel = rawConnection.createChannel()

  "A connection" should {
    "declare an exchange and a queue on the server" in {
      Await.ready(connection ? DeclareExchange(name = "test-exchange", tpe = "topic", autoDelete = true), 5.seconds)

      Await.ready(connection ? DeclareQueue(name = "test-queue", autoDelete = true), 5.seconds)

      noException should be thrownBy {
        rawChannel.queueBind("test-queue", "test-exchange", "#")
      }
    }

    "publish messages to a queue in" in {
      Await.ready(connection ? BindQueue("test-queue", "test-exchange", "#"), 5.seconds)

      connection ! Publish("test-exchange", "test", "foo".getBytes)

      val res = Await.result((connection ? Subscribe("test-queue")).mapTo[Delivery], 5.seconds)

      new String(res.body, Charset.forName("utf-8")) should equal("foo")
    }
  }

  override protected def afterAll(): Unit = system.shutdown()
}
