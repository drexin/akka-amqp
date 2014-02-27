/**
 *  Copyright (C) 2013-2014 Dario Rexin
 */

package io.github.drexin.akka.amqp

import akka.testkit.{TestProbe, DefaultTimeout, TestKit}
import akka.actor.{Props, ActorRef, Actor, ActorSystem}
import akka.io.IO
import io.github.drexin.akka.amqp.AMQP._
import io.github.drexin.akka.amqp.AMQP.Connect
import io.github.drexin.akka.amqp.AMQP.Connected
import io.github.drexin.akka.amqp.AMQP.DeclareExchange
import io.github.drexin.akka.amqp.AMQP.ExchangeDeclared
import java.nio.charset.Charset
import org.scalatest.{BeforeAndAfterAll, WordSpecLike, Matchers}
import scala.concurrent.duration._

class Publisher extends Actor {
  import context.system

  val amqp = IO(AMQP)
  var connection: ActorRef = _

  override def preStart(): Unit = {
    amqp ! Connect("amqp://guest:guest@127.0.0.1:5672")
  }

  def receive: Receive = {
    case Connected(_, _connection) =>
      connection = _connection
      connection ! DeclareExchange(name = "test-exchange", tpe = "topic", autoDelete = true)

    case ExchangeDeclared(_) =>
      context.become(publishing)
  }

  def publishing: Receive = {
    case "pub" =>
      for (i <- 0 until 10) connection ! Publish(exchange = "test-exchange", routingKey = "pubsub", body = s"test$i".getBytes(Charset.forName("utf-8")))
      for (i <- 0 until 10) connection ! Publish(exchange = "test-exchange", routingKey = "pubsub", body = s"nack$i".getBytes(Charset.forName("utf-8")))
  }
}

class Subscriber(receiver: ActorRef, prefix: String) extends Actor {
  import context.system

  val amqp = IO(AMQP)
  var connection: ActorRef = _

  override def preStart(): Unit = {
    amqp ! Connect("amqp://guest:guest@127.0.0.1:5672")
  }

  def receive: Actor.Receive = {
    case Connected(_, _connection) =>
      connection = _connection
      connection ! DeclareQueue(name = "test-queue", autoDelete = true)

    case QueueDeclared(_) =>
      connection ! BindQueue(queue = "test-queue", exchange = "test-exchange", routingKey = "pubsub")

    case QueueBound(_,_,_) =>
      connection ! Subscribe("test-queue")
      receiver ! "ready"

    case Delivery(_,envelope,_,body) =>
      val queue = sender
      val messageBody = new String(body, Charset.forName("utf-8"))
      if (messageBody.startsWith(prefix)) {
        receiver ! messageBody
        queue ! Ack(envelope.getDeliveryTag)
      } else {
        queue ! Nack(envelope.getDeliveryTag)
      }
  }
}

class PubSubSpec extends TestKit(ActorSystem("TestSystem")) with DefaultTimeout with WordSpecLike with Matchers with BeforeAndAfterAll {

  "A PubSub system" should {
    "receive all published messages" in {
      val probe = TestProbe()
      system.actorOf(Props(classOf[Subscriber], probe.ref, "test"))
      system.actorOf(Props(classOf[Subscriber], probe.ref, "nack"))
      val publisher = system.actorOf(Props(classOf[Publisher]))

      probe.expectMsg("ready")
      probe.expectMsg("ready")

      publisher ! "pub"

      probe.expectMsgAllOf(10.seconds, (0 until 10).map(i => {
        s"test$i"
      }) ++ (0 until 10).map(j => {
        s"nack$j"
      }): _*)
    }
  }

  override protected def afterAll(): Unit = system.shutdown()
}
