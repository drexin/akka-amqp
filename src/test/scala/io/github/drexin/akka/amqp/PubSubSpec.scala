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
      for (i <- 0 until 10) connection ! Publish(exchange = "test-exchange", routingKey = "test", body = s"test$i".getBytes(Charset.forName("utf-8")))
  }
}

class Subscriber(receiver: ActorRef) extends Actor {
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
      connection ! BindQueue(queue = "test-queue", exchange = "test-exchange", routingKey = "#")

    case QueueBound(_,_,_) =>
      connection ! Subscribe("test-queue")
      receiver ! "ready"

    case Delivery(_,envelope,_,body) =>
      sender ! Ack(envelope.getDeliveryTag)
      receiver ! new String(body, Charset.forName("utf-8"))
  }
}

class PubSubSpec extends TestKit(ActorSystem("TestSystem")) with DefaultTimeout with WordSpecLike with Matchers with BeforeAndAfterAll {

  "A PubSub system" should {
    "receive all published messages" in {
      val probe = TestProbe()
      system.actorOf(Props(classOf[Subscriber], probe.ref))
      val publisher = system.actorOf(Props(classOf[Publisher]))

      probe.expectMsg("ready")

      publisher ! "pub"

      probe.expectMsgAllOf((0 until 10).map(i => s"test$i"): _*)
    }
  }

  override protected def afterAll(): Unit = system.shutdown()
}
