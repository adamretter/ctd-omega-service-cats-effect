package uk.gov.nationalarchives.omega.api

import cats.data.NonEmptyList
import cats.effect.{ExitCode, IO, IOApp, Resource}
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.JmsClient
import jms4s.activemq.activeMQ
import jms4s.activemq.activeMQ.{ClientId, Config, Endpoint, Password, Username}
import jms4s.config.{DestinationName, QueueName}
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.jms.MessageFactory
import org.typelevel.log4cats.{Logger, LoggerFactory, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jFactory

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

object ExampleExternalApp extends IOApp {

  implicit val loggerFactory: LoggerFactory[IO] = Slf4jFactory[IO]
  implicit val logger: SelfAwareStructuredLogger[IO] = LoggerFactory[IO].getLogger

  val instanceId = new AtomicInteger() // TODO(AR) make this dynamic in future as we may have multiple instances for scalability

  val jmsClient: Resource[IO, JmsClient[IO]] = jmsClientResource // see providers section!
  val omegaRequestQueue: QueueName = QueueName("OMEGA.INPUT.QUEUE")
  val omegaResponseQueue: QueueName = QueueName("OMEGA.OUTPUT.QUEUE")

  private def jmsClientResource(implicit L: Logger[IO]): Resource[IO, JmsClient[IO]] =
    activeMQ.makeJmsClient[IO](
      Config(
        endpoints = NonEmptyList.one(Endpoint("localhost", 61616)),
        username = Some(Username("admin")),
        password = Some(Password("admin")),
        clientId = ClientId("omega.example-external-app.instance-" + instanceId.incrementAndGet())  // TODO(AR) at the moment this app will create two JMS clients due to how the IO's are composed - we should only create one!
      )
    )

  private def makeN(texts: NonEmptyList[String], destinationName: DestinationName): MessageFactory[IO] => IO[NonEmptyList[(JmsTextMessage, DestinationName)]] = { mFactory =>
    texts.traverse { text =>
      mFactory
        .makeTextMessage(text)
        .map(message => (message, destinationName))
        .flatTap{ case (message, destinationName) => IO { println(s"Sending: '${message.getText.get}' to: ${destinationName.asInstanceOf[QueueName].value}")} }
    }
  }

  private def printResponse(text: String, mf: MessageFactory[IO]): IO[AckAction[IO]] = {
    IO.delay(println(s"Received: '$text"))
      .flatMap(_ => IO.pure(AckAction.ack))
  }

  override def run(args: List[String]): IO[ExitCode] = {

    // TODO(AR) how to produce 10 requests, await 10 responses and then shutdown? -- start with any responses and then consider also adding in CorrelationID

    // producer
    val producerRes = for {
      client <- jmsClient
      producer <- client.createProducer(10)
    } yield producer

    val messageStrings: NonEmptyList[String] = NonEmptyList.fromListUnsafe((1 to 10).map(i => s"Message '$i'").toList)

    val producerIO: IO[ExitCode] = producerRes.use { producer => {
          producer.sendN(makeN(messageStrings, omegaRequestQueue))
      }.as(ExitCode.Success)
    }


    // consumerIO
    val consumerRes = for {
      client <- jmsClient
      consumer <- client.createAcknowledgerConsumer(omegaResponseQueue, 10, 100.millis)
    } yield consumer

    val consumerIO : IO[ExitCode] = consumerRes.use(_.handle { (jmsMessage, mf) =>
      for {
        text <- jmsMessage.asTextF[IO]
        res <- printResponse(text, mf)
      } yield res
    }.as(ExitCode.Success))

    IO.both(producerIO, consumerIO)
      .map { case (producerExitCode, consumerExitCode) =>
          if (producerExitCode == consumerExitCode) {
            producerExitCode
          } else if (producerExitCode == 0) {
            consumerExitCode
          } else if (consumerExitCode == 0) {
            producerExitCode
          } else {
            producerExitCode
          }
      }
  }
}
