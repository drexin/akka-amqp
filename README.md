# Akka based AMQP client (work in progress)
_________

#Usage

### Publisher
A simple actor that opens a connection to the AMQP broker, creates an exchange and then waits for messages to publish could look as follows:

```scala
import io.github.drexin.akka.amqp._

class Publisher extends Actor with Stash {
  import AMQP._

  import context.system

  val amqp = IO(AMQP)
  var connection: ActorRef = _

  override def preStart(): Unit = {
    amqp ! Connect("amqp://guest:guest@127.0.0.1:5672")
  }

  def receive: Receive = {
    case Connected(_, _connection) =>
      connection = _connection
      // declare an auto deleted topic exchange
      connection ! DeclareExchange(name = "test-exchange", tpe = "topic", autoDelete = true)

    case ExchangeDeclared(_) =>
      context.become(publishing)
      unstashAll()
    
    case x =>
      // stash all incoming messages while waiting for the exchange
      // to be declared
      stash()
  }

  def publishing: Receive = {
    case x: Publish =>
      connection ! x
  }
}
```

Instead of creating an actor as publisher, one could also just retrieve a reference the the connection actor itself by using the ask pattern and send messages to the connection directly:

```scala
val connectionFuture = (amqp ? Connect("amqp://guest:guest@localhost:5672")).mapTo[Connected]

val connection = Await.result(connectionFuture, 5.seconds).connection

//...
```

### Subscriber
A subscriber is an actor that subscribes to messages from a queue:

```scala
class Subscriber extends Actor {
  import context.system

  val amqp = IO(AMQP)
  var connection: ActorRef = _

  override def preStart(): Unit = {
    amqp ! Connect("amqp://guest:guest@127.0.0.1:5672")
  }

  def receive: Receive = {
    case Connected(_, _connection) =>
      connection = _connection
      // declare an auto deleted queue
      connection ! DeclareQueue(name = "test-queue", autoDelete = true)

    case QueueDeclared(_) =>
      // bind 'test-queue' to all messages that are published on 'test-exchange'
      connection ! BindQueue(queue = "test-queue", exchange = "test-exchange", routingKey = "#")

    case QueueBound(_,_,_) =>
      // subscribe this actor to messages from 'test-queue'
      connection ! Subscribe("test-queue")
      receiver ! "ready"
      context.become(subscribed)
  }
  
  def subscribed: Receive = {
	case Delivery(_, envelope, _, body) =>
	  // acknowledge messages received from the queue
      sender ! Ack(envelope.getDeliveryTag)
  }
}
```