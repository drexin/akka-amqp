package io.github.drexin.akka.amqp

import akka.actor._
import akka.io.IO
import com.rabbitmq.client.{AMQP => Rabbit, _}

import java.util.concurrent.ExecutorService
import java.net.InetAddress
import java.nio.charset.Charset
import akka.actor.SupervisorStrategy.Escalate
import akka.actor.OneForOneStrategy
import com.rabbitmq.client

object AMQP extends ExtensionId[AMQPExt] with ExtensionIdProvider {
  def lookup(): ExtensionId[_ <: Extension] = AMQP

  def createExtension(system: ExtendedActorSystem): AMQPExt = new AMQPExt(system)

  // Commands
  trait Command

  case class Connect(uri: String) extends Command

  case class DeclareExchange(name: String, tpe: String, durable: Boolean = true, autoDelete: Boolean = false, internal: Boolean = false, arguments: Map[String, AnyRef] = Map()) extends Command

  case class DeclareQueue(name: String, durable: Boolean = true, exclusive: Boolean = false, autoDelete: Boolean = false, arguments: Map[String, AnyRef] = Map()) extends Command

  case class BindQueue(queue: String, exchange: String, routingKey: String, arguments: Map[String, AnyRef] = Map()) extends Command

  case class Publish(exchange: String, routingKey: String, body: Array[Byte], mandatory: Boolean = false, immediate: Boolean = false, props: Option[Rabbit.BasicProperties] = None) extends Command

  case class Subscribe(queue: String) extends Command

  // Responses
  trait Response

  case class Connected(address: InetAddress) extends Response

  case class QueueDeclared(name: String) extends Response

  case class ExchangeDeclared(name: String) extends Response

  case class QueueBound(queue: String, exchange: String, routingKey: String) extends Response

  //
  case class Delivery(consumerTag: String, envelope: Envelope, properties: client.AMQP.BasicProperties, body: Array[Byte])
}

class AMQPExt(system: ExtendedActorSystem) extends IO.Extension {
  private [this] val config = system.settings.config.getConfig("akka.amqp")

  // necessary because the manager can not be started as system actor outside of akka library code
  private [this] val name = config.getString("manager-name")

  private [this] val _manager: ActorRef = system.actorOf(Props(classOf[AMQPManager], Option.empty[ExecutorService]).withDeploy(Deploy.local), name)
  def manager: ActorRef = _manager
}

class AMQPManager(executorOpt: Option[ExecutorService]) extends Actor with ActorLogging {
  import AMQP._


  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case e: Exception =>
      log.error("w00t", e)
      Escalate
  }

  def receive: Actor.Receive = {
    case Connect(uri: String) =>
      context.actorOf(Props(classOf[AMQPConnection], uri, executorOpt, sender).withDeploy(Deploy.local))
  }
}

class AMQPConnection(uri: String, executorOpt: Option[ExecutorService], commander: ActorRef) extends Actor {
  import AMQP._
  import scala.collection.JavaConverters._

  private [this] var connection: Connection = _
  private [this] var channel: Channel = _

  override def preStart(): Unit = {
    val factory = new ConnectionFactory()
    factory.setUri(uri)

    connection = executorOpt.fold {
      factory.newConnection()
    } { executor =>
      factory.newConnection(executor)
    }

    channel = connection.createChannel()

    commander ! Connected(connection.getAddress)
  }

  override def postStop(): Unit = {
    connection.close()
  }

  def receive: Actor.Receive = {
    case DeclareExchange(name, tpe, durable, autoDelete, internal, arguments) =>
      channel.exchangeDeclare(name, tpe, durable, autoDelete, internal, args(arguments))
      commander ! ExchangeDeclared(name)

    case DeclareQueue(name, durable, exclusive, qutoDelete, arguments) =>
      channel.queueDeclare(name, durable, exclusive, qutoDelete, args(arguments))
      commander ! QueueDeclared(name)

    case BindQueue(queue, exchange, routingKey, arguments) =>
      channel.queueBind(queue, exchange, routingKey, args(arguments))
      commander ! QueueBound(queue, exchange, routingKey)

    case Publish(msg, queue, body, mandatory, immediate, props) =>
      channel.basicPublish(msg, queue, mandatory, immediate, props.orNull, body)

    case Subscribe(queue) =>
      channel.basicConsume(queue, new ForwardingConsumer(sender))
  }

  // The rabbitmq client expects null when no properties are set... oO
  private def args(arguments: Map[String, AnyRef]): java.util.Map[String, AnyRef] = {
    if (arguments.isEmpty) {
      null
    } else {
      arguments.asJava
    }
  }

  class ForwardingConsumer(consumer: ActorRef) extends Consumer {
    def handleConsumeOk(consumerTag: String): Unit = {}

    def handleCancelOk(consumerTag: String): Unit = {}

    def handleCancel(consumerTag: String): Unit = {}

    def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit = {}

    def handleRecoverOk(consumerTag: String): Unit = {}

    def handleDelivery(consumerTag: String, envelope: Envelope, properties: client.AMQP.BasicProperties, body: Array[Byte]): Unit = {
      consumer ! Delivery(consumerTag, envelope, properties, body)
    }
  }
}
