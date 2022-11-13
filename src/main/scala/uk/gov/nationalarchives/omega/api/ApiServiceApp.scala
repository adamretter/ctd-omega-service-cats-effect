package uk.gov.nationalarchives.omega.api

import cats.data.NonEmptyList
import cats.effect.{ExitCode, IO, IOApp, Resource}
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.{JmsAcknowledgerConsumer, JmsClient}
import jms4s.config.QueueName
import jms4s.activemq.activeMQ
import jms4s.activemq.activeMQ._
import jms4s.jms.{JmsMessage, MessageFactory}
import org.typelevel.log4cats.slf4j.Slf4jFactory
import org.typelevel.log4cats.{Logger, LoggerFactory, SelfAwareStructuredLogger}
import uk.gov.nationalarchives.omega.api.LocalMessageStore.PersistentMessageId

import java.nio.file.{Files, Paths}
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.DurationInt
import scala.util.Success

object ApiServiceApp extends IOApp {

  implicit val loggerFactory: LoggerFactory[IO] = Slf4jFactory[IO]
  implicit val logger: SelfAwareStructuredLogger[IO] = LoggerFactory[IO].getLogger

  val localMessageStore = {
    val messageStoreFolder = Paths.get("/tmp/services-message-store")  // TODO(AR) should be injected from config/args
    Files.createDirectories(messageStoreFolder)
    new LocalMessageStore(messageStoreFolder)
  }

  val instanceId = 1 // TODO(AR) make this dynamic in future as we may have multiple instances for scalability

  val contextRes: Resource[IO, JmsClient[IO]] = jmsClientResource // see providers section!
  val inputQueue: QueueName = QueueName("OMEGA.INPUT.QUEUE")
  val outputQueue: QueueName = QueueName("OMEGA.OUTPUT.QUEUE")

  private def jmsClientResource(implicit L: Logger[IO]): Resource[IO, JmsClient[IO]] =
    activeMQ.makeJmsClient[IO](
      Config(
        endpoints = NonEmptyList.one(Endpoint("localhost", 61616)),
        username = Some(Username("admin")),
        password = Some(Password("admin")),
        clientId = ClientId("omega.api-service.instance-" + instanceId)
      )
    )

  private def echoService(text: String, mf: MessageFactory[IO]): IO[AckAction[IO]] = {
//    if (text.toInt % 2 == 0)
      // NOTE(AR) Ack the incoming message, and send a message
      mf.makeTextMessage(s"Echo: $text")
        .flatTap(_ => IO { println(s"ApiServiceApp Echoing: $text") })
        .map(newMsg => AckAction.send(newMsg, outputQueue))
//    else if (text.toInt % 3 == 0) {
//      // NOTE(AR) NoAck the incoming message, no response message
//      IO.pure(AckAction.noAck)
//    } else {
//      // NOTE(AR) Ack the incoming message, no response message
//      IO.pure(AckAction.ack)
//    }
  }

  private def acknowledgeMessage(): IO[AckAction[IO]] = {
//    IO.pure(AckAction.ack)
    IO {
      println("Acknowledged message")
      AckAction.ack
    }
  }

  override def run(args: List[String]): IO[ExitCode] = {

    // TODO(AR) - one client, how to ack a consumer message after local persistence and then process it, and then produce a response
    // TODO(AR) how to wire up queues and services using a config file or DSL?

    // TODO(AR) request queue will typically be 1 (plus maybe a few more for expedited ops), response queues will be per external application

    val consumerRes: Resource[IO, JmsAcknowledgerConsumer[IO]] = for {
      client <- contextRes
      consumer <- client.createAcknowledgerConsumer(inputQueue, 10, 100.millis)
    } yield consumer

    consumerRes.use(jmsAcknowledgerConsumer =>
      jmsAcknowledgerConsumer.handle { (jmsMessage, mf) =>
        for {
  //        text <- jmsMessage.asTextF[IO]
          persistentMessageId <- localMessageStore.persistMessage(jmsMessage)
          res <- acknowledgeMessage().flatTap(_ => someBusinessService(jmsMessage, persistentMessageId))
  //        res <- echoService(text, mf)
        } yield res
      }.as(ExitCode.Success)
    )

    consumerRes.use(_.handle { (jmsMessage, mf) =>
      for {
        persistentMessageId <- localMessageStore.persistMessage(jmsMessage)
        res <- IO.pure(AckAction.ack[IO])
      } yield res
    }.as(ExitCode.Success))
  }

  def someBusinessService(jmsMessage: JmsMessage, persistentMessageId: PersistentMessageId) : IO[Unit] = {
    IO.async_[Unit] { cb =>
      val x :Either[Throwable, Unit] = jmsMessage.attemptAsText.map { text =>
        println(s"Performing some business service for id: $persistentMessageId: $text")
      }.toEither

      cb(x)
      () // TODO(AR) explicit unit return type

    }
  }
}
