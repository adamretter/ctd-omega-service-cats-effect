package uk.gov.nationalarchives.omega.api

import cats.data.NonEmptyList
import cats.effect.{ExitCode, IO, IOApp, Resource}
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.{JmsAcknowledgerConsumer, JmsClient, JmsProducer}
import jms4s.activemq.activeMQ
import jms4s.activemq.activeMQ.{ClientId, Config, Endpoint, Password, Username}
import jms4s.config.{DestinationName, QueueName}
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.jms.MessageFactory
import org.typelevel.log4cats.{Logger, LoggerFactory, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jFactory
import uk.gov.nationalarchives.omega.api.ApiServiceApp.{createJmsClient, createJmsInputQueueConsumer, createJmsProducer, inputQueue}

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

object ExampleExternalApp extends IOApp {

  implicit val loggerFactory: LoggerFactory[IO] = Slf4jFactory[IO]
  implicit val logger: SelfAwareStructuredLogger[IO] = LoggerFactory[IO].getLogger

  val instanceId = new AtomicInteger() // TODO(AR) make this dynamic in future as we may have multiple instances for scalability

//  val jmsClient: Resource[IO, JmsClient[IO]] = jmsClientResource // see providers section!
  val omegaRequestQueue: QueueName = QueueName("OMEGA.INPUT.QUEUE")
  val omegaResponseQueue: QueueName = QueueName("OMEGA.OUTPUT.QUEUE")

  private def createJmsClient()(implicit L: Logger[IO]): Resource[IO, JmsClient[IO]] =
    activeMQ.makeJmsClient[IO](
      Config(
        endpoints = NonEmptyList.one(Endpoint("localhost", 61616)),
        username = Some(Username("admin")),
        password = Some(Password("admin")),
        clientId = ClientId("omega.example-external-app.instance-" + UUID.randomUUID().toString)
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

    val jmsIo: Resource[IO, (JmsProducer[IO], JmsAcknowledgerConsumer[IO])] = for {
      jmsClient <- createJmsClient()
      jmsProducerAndConsumer <- Resource.both(
        jmsClient.createProducer(10),
        jmsClient.createAcknowledgerConsumer(omegaResponseQueue, 10, 100.millis)
      )
    } yield jmsProducerAndConsumer

    val messageStrings: NonEmptyList[String] = NonEmptyList.fromListUnsafe((1 to 10).map(i => s"Message '$i'").toList)

    jmsIo.use { case (jmsProducer, jmsConsumer) =>
      IO.both(
        // producer action
        jmsProducer.sendN(makeN(messageStrings, omegaRequestQueue)),

        // consumer action
        jmsConsumer.handle { case (jmsMessage, mf) =>
            for {
              text <- jmsMessage.asTextF[IO]
              res <- printResponse(text, mf)
            } yield res
        }
      )
    }.as(ExitCode.Success)
  }
}
