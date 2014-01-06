/**
 *  Copyright (C) 2013-2014 Dario Rexin
 */

package io.github.drexin.akka.amqp

import akka.testkit.{TestProbe, DefaultTimeout, TestKit}
import akka.actor.ActorSystem
import org.scalatest._
import akka.io.IO
import scala.concurrent.Await
import akka.pattern.ask
import scala.concurrent.duration._
import com.rabbitmq.client.ConnectionFactory
import java.nio.charset.Charset
import java.io.IOException
import com.rabbitmq.client.Channel
import java.util.concurrent.TimeoutException

class ConnectionSpec extends TestKit(ActorSystem("TestSystem")) with DefaultTimeout with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import AMQP._

  val amqp = IO(AMQP)
  var connection = createConnection()

  val rawConnection = new ConnectionFactory().newConnection()

  "A connection" should {
    "declare an exchange and a queue on the server" in {
      withChannel { channel =>
        Await.ready(connection ? DeclareExchange(name = "test-exchange", tpe = "topic", autoDelete = false), 5.seconds)

        Await.ready(connection ? DeclareQueue(name = "test-queue", autoDelete = false), 5.seconds)

        noException should be thrownBy {
          channel.queueBind("test-queue", "test-exchange", "#")
        }
      }
    }

    "publish messages to a queue in" in {
      Await.ready(connection ? BindQueue("test-queue", "test-exchange", "#"), 5.seconds)

      connection ! Publish("test-exchange", "test", "foo".getBytes)

      val res = Await.result((connection ? Subscribe("test-queue")).mapTo[Delivery], 5.seconds)

      connection ! Ack(res.envelope.getDeliveryTag)

      new String(res.body, Charset.forName("utf-8")) should equal("foo")
    }

    "delete a queue" in {
      withChannel { channel =>

        Await.ready(connection ? DeleteQueue("test-queue"), 5.seconds)

        an [IOException] should be thrownBy {
          channel.basicGet("test-queue", true)
        }
      }
    }

    "delete an exchange" in {
      withChannel { channel =>

        Await.ready(connection ? DeleteExchange("test-exchange"), 5.seconds)

        an [IOException] should be thrownBy {
          channel.queueBind("test-queue", "test-exchange", "#")
        }
      }
    }

    "auto ack messages" in {
      withChannel { channel =>
        try {
          Await.ready(connection ? DeclareExchange(name = "test-exchange", tpe = "topic", autoDelete = true), 5.seconds)

          Await.ready(connection ? DeclareQueue(name = "test-queue", autoDelete = false), 5.seconds)

          Await.ready(connection ? BindQueue("test-queue", "test-exchange", "#"), 5.seconds)

          connection ! Publish("test-exchange", "test", "foo".getBytes)

          val res = Await.result((connection ? Subscribe(queue = "test-queue", autoAck = true)).mapTo[Delivery], 5.seconds)

          new String(res.body, Charset.forName("utf-8")) should equal("foo")

          val probe = TestProbe()

          probe.watch(connection)

          system.stop(connection)

          probe.expectTerminated(connection, 5.seconds)

          channel.basicGet("test-queue", false) should be(null)
        } finally {
          channel.queueDelete("test-queue")
        }
      }
    }

    "unbind queue" in {
      try {
        Await.ready(connection ? DeclareExchange(name = "test-exchange", tpe = "topic", autoDelete = false), 5.seconds)

        Await.ready(connection ? DeclareQueue(name = "test-queue", autoDelete = false), 5.seconds)

        Await.ready(connection ? BindQueue("test-queue", "test-exchange", "#"), 5.seconds)

        Await.ready(connection ? UnbindQueue("test-queue", "test-exchange", "#"), 5.seconds)

        connection ! Publish("test-exchange", "test", "foo".getBytes)

        an [TimeoutException] should be thrownBy {
          Await.result((connection ? Subscribe("test-queue")).mapTo[Delivery], 100.millis)
        }
      } finally {
        withChannel { channel =>
          channel.exchangeDelete("test-exchange")
          channel.queueDelete("test-queue")
        }
      }
    }
  }

  override protected def beforeEach(): Unit = {
    // make sure that a connection is always available before running a test
    try {
      val probe = TestProbe()

      probe.watch(connection)

      probe.expectTerminated(connection, 0.millis)

      connection = createConnection()
    } catch {
      case _: AssertionError =>
    }
  }

  override protected def afterAll(): Unit = {
    system.shutdown()
    system.awaitTermination()
    rawConnection.close()
  }

  private def withChannel[A](f: Channel => A) {
    val channel = rawConnection.createChannel
    try {
      f(channel)
    } finally {
      // if the channel is already closed, calling close again
      // seems to close the connection... oO
      if (channel.isOpen)
        channel.close()
    }
  }

  private def createConnection() = {
    Await.result((amqp ? Connect("amqp://guest:guest@localhost:5672")).mapTo[Connected], 5.seconds).connection
  }
}
