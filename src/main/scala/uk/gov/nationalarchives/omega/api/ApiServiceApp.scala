package uk.gov.nationalarchives.omega.api

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated, ValidatedNec}
import cats.effect.std.Queue
import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.implicits.catsSyntaxParallelTraverse_
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.{JmsAcknowledgerConsumer, JmsClient, JmsProducer}
import jms4s.config.QueueName
import jms4s.activemq.activeMQ
import jms4s.activemq.activeMQ._
import jms4s.jms.{JmsMessage, MessageFactory}
import org.typelevel.log4cats.slf4j.Slf4jFactory
import org.typelevel.log4cats.{Logger, LoggerFactory, SelfAwareStructuredLogger}
import uk.gov.nationalarchives.omega.api.LocalMessageStore.PersistentMessageId
import uk.gov.nationalarchives.omega.api.service.impl.{EchoRequest, EchoResponse, EchoService, EchoServiceError}
import uk.gov.nationalarchives.omega.api.service.{BusinessService, BusinessServiceError, BusinessServiceRequest, BusinessServiceResponse, RequestValidation, RequestValidationError}

import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success, Try}

// TODO(AR) improve dynamic loading of services and conversion of messages to/from case-classes for services
// TODO(AR) consider how database/elastic resources are to be used - what about underlying connection pools etc

object ApiServiceApp extends IOApp {

  implicit val loggerFactory: LoggerFactory[IO] = Slf4jFactory[IO]
  implicit val logger: SelfAwareStructuredLogger[IO] = LoggerFactory[IO].getLogger

  val localMessageStore = {
    val messageStoreFolder = Paths.get("/tmp/services-message-store")  // TODO(AR) should be injected from config/args
    Files.createDirectories(messageStoreFolder)
    new LocalMessageStore(messageStoreFolder)
  }

  val instanceId = 1 // TODO(AR) make this dynamic in future as we may have multiple instances for scalability

  //val contextRes: Resource[IO, JmsClient[IO]] = jmsClientResource // see providers section!
  val inputQueue: QueueName = QueueName("OMEGA.INPUT.QUEUE")
  val outputQueue: QueueName = QueueName("OMEGA.OUTPUT.QUEUE")

  private def createJmsClient()(implicit L: Logger[IO]): Resource[IO, JmsClient[IO]] =
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

  def transferMessageToLocalQueue(q: Queue[IO, LocalMessage])(jmsAcknowledgerConsumer: JmsAcknowledgerConsumer[IO]): IO[Unit] = {
    jmsAcknowledgerConsumer.handle { (jmsMessage, mf) =>
      for {
        persistentMessageId <- localMessageStore.persistMessage(jmsMessage)
        res <- acknowledgeMessage()
        _ <- q.offer(LocalMessage(persistentMessageId, jmsMessage))
        _ <- IO.delay { println(s"Queued: $persistentMessageId") }
      } yield res
    }
  }

  case class LocalMessage(persistentMessageId: PersistentMessageId, jmsMessage: JmsMessage)

  def createJmsInputQueueConsumer(client: JmsClient[IO])(queueName: QueueName, concurrencyLevel: Int, pollingInterval: FiniteDuration) : Resource[IO, JmsAcknowledgerConsumer[IO]] = {
//     for {
//      client <- contextRes
      /*consumer <-*/ client.createAcknowledgerConsumer(queueName, concurrencyLevel, pollingInterval)
//    } yield consumer
  }

  def createJmsProducer(client: JmsClient[IO])(concurrencyLevel: Int): Resource[IO, JmsProducer[IO]] = {
//    for {
//      client <- contextRes
      /*producer <- */ client.createProducer(concurrencyLevel)
//    } yield producer
  }

  def dispatcher(dispatcherId: Int)(q: Queue[IO, ApiServiceApp.LocalMessage], jmsProducer: JmsProducer[IO]) : IO[Unit] = {
      for {
        localMessage <- q.take
        _ <- IO.delay { println(s"Dispatcher # $dispatcherId, processing message id: ${localMessage.persistentMessageId}") }
        serviceRequest <- createServiceRequest[EchoService, EchoRequest, EchoResponse, EchoServiceError](localMessage.jmsMessage)
        businessService <- IO.pure(serviceRequest._1)
        businessServiceRequest <- IO.pure(serviceRequest._2)
        validatedBusinessServiceRequest <- validateBusinessServiceRequest(businessService, businessServiceRequest)
        businessResult <- execBusinessService(businessService, validatedBusinessServiceRequest)
        res <- sendResultToJmsQueue(jmsProducer, outputQueue, businessResult)
//        res <- jmsProducer.send(mf => (mf.makeTextMessage(businessResult).map((_, outputQueue))))
      } yield res
  }

  def createServiceRequest[B <: BusinessService[T, U, E], T <: BusinessServiceRequest, U <: BusinessServiceResponse, E <: BusinessServiceError](jmsMessage: JmsMessage): IO[Tuple2[B, T]] = {
    // TODO(AR) extract the business service id from a custom JMS Header/Property/Attribute "OSID: OSCRD01" (OmegaServiceID: OmegaServiceCreateRecord01)

    // TODO(AR) temporarily hardcoded to the EchoService, need a way to dynamically create service and request
    IO.delay {
      Tuple2(
        new EchoService().asInstanceOf[B],
        EchoRequest(jmsMessage.asTextF[Try].get).asInstanceOf[T]  // TODO(AR) need a better way of handling errors here
      )
    }
  }

//  def validateBusinessServiceRequest[T <: BusinessServiceRequest, U <: BusinessServiceResponse, E <: BusinessServiceError](businessService: BusinessService[T, U, E], businessServiceRequest: T) : IO[ValidatedNec[RequestValidationError, T]] = {
def validateBusinessServiceRequest[T <: BusinessServiceRequest](businessService: BusinessService[T, _, _], businessServiceRequest: T) : IO[ValidatedNec[RequestValidationError, T]] = {
    IO.delay {
      if (businessService.isInstanceOf[RequestValidation[T]]) {
        businessService.asInstanceOf[RequestValidation[T]].validateRequest(businessServiceRequest)
      } else {
        Validated.valid(businessServiceRequest)
      }
    }
  }

  def execBusinessService[T <: BusinessServiceRequest, U <: BusinessServiceResponse, E <: BusinessServiceError](businessService: BusinessService[T, U, E], validatedBusinessServiceRequest: ValidatedNec[RequestValidationError, T]): IO[ValidatedNec[RequestValidationError, Either[BusinessServiceError, U]]] = {
    IO.delay {
      validatedBusinessServiceRequest.map(businessService.process)
    }
  }

  def sendResultToJmsQueue[U <: BusinessServiceResponse, E <: BusinessServiceError](jmsProducer: JmsProducer[IO], outputQueue: QueueName, businessResult: ValidatedNec[RequestValidationError, Either[E, U]]): IO[Unit] = {

    //        res <- jmsProducer.send(mf => (mf.makeTextMessage(businessResult).map((_, outputQueue))))

    // TODO(AR) better conversion of BusinessServiceResponse is required so we can support multiple services


    val message: IO[String] = IO.delay {
      businessResult match {
        case Valid(Right(businessResult)) =>
          // TODO(AR) fix this hardcoding of types
          businessResult.asInstanceOf[EchoResponse].text

        case Valid(Left(serviceError)) =>
          val customerErrorReference = UUID.randomUUID();
          s"""{status: "SERVICE-ERROR", reference: "${customerErrorReference}", code: "${serviceError.code}", message: "${serviceError.message}"}"""

        case Invalid(requestValidationFailures) =>
          val customerErrorReference = UUID.randomUUID();
          s"""{status: "INVALID-REQUEST", reference: "${customerErrorReference}", message: "${requestValidationFailures}"}"""
      }
    }

    message.flatMap(messageText => jmsProducer.send(_.makeTextMessage(messageText).map((_, outputQueue))))

  }

  override def run(args: List[String]): IO[ExitCode] = {

    // TODO(AR) - one client, how to ack a consumer message after local persistence and then process it, and then produce a response
    // TODO(AR) how to wire up queues and services using a config file or DSL?

    // TODO(AR) request queue will typically be 1 (plus maybe a few more for expedited ops), response queues will be per external application
    val MAX_LOCAL_QUEUE_SIZE = 1000 // TODO(AR) make this configurable or computable?
    val MAX_CONSUMERS = 10
    val MAX_DISPATCHERS = 10
    val MAX_PRODUCERS = 10
    val localQueue: IO[Queue[IO, LocalMessage]] = Queue.bounded[IO, LocalMessage](MAX_LOCAL_QUEUE_SIZE)

    val jmsIo: Resource[IO, (JmsProducer[IO], JmsAcknowledgerConsumer[IO])] = for {
      jmsClient <- createJmsClient()
      jmsProducerAndConsumer <- Resource.both(
        createJmsProducer(jmsClient)(MAX_PRODUCERS),
        createJmsInputQueueConsumer(jmsClient)(inputQueue, MAX_CONSUMERS, 100.millis)
      )
    } yield jmsProducerAndConsumer

    val x = for {
      q <- localQueue
      res <- jmsIo.use { case (jmsProducer, jmsConsumer) =>
        for {
          // START
          res <- IO.race(
            transferMessageToLocalQueue(q)(jmsConsumer),

            // single dispatcher
            //        dispatcher(q).foreverM

            // multiple dispatchers
            List.range(start = 0, end = MAX_DISPATCHERS).parTraverse_ { i =>
              IO.println(s"Starting consumer #${i + 1}") >>
                dispatcher(i)(q, jmsProducer).foreverM
            }
          )
          // END

        } yield res
      }
    } yield res


//      res <- IO.race(
//        createJmsInputQueueConsumer(inputQueue, MAX_CONSUMERS, 100.millis).use(transferMessageToLocalQueue(q)),
//
//        // single dispatcher
////        dispatcher(q).foreverM
//
//        // multiple dispatchers
//        List.range(start = 0, end = MAX_DISPATCHERS).parTraverse_ { i =>
//          IO.println(s"Starting consumer #${i + 1}") >>
//            dispatcher(i)(q).foreverM
//        }
//
//      )
//    } yield res

    x.as(ExitCode.Success)


//    val xs:String = List.range(start = 0, end = 10).parTraverse_ { i =>
//      IO.println(s"Starting consumer #${i + 1}")
//    }

//    IO.race(
//      consumerRes.use(transferMessageToLocalQueue(localQueue)),
//      dispatcher,
//    ).as(ExitCode.Success)

//    val y: IO[Unit] = consumerRes.use(jmsAcknowledgerConsumer => {
//        val x: IO[Unit] = jmsAcknowledgerConsumer.handle { (jmsMessage, mf) =>
//          for {
//            //        text <- jmsMessage.asTextF[IO]
//            persistentMessageId <- localMessageStore.persistMessage(jmsMessage)
//            res <- acknowledgeMessage()
//            q <- localQueue
//            _ <- q.offer(jmsMessage)
//            //            .flatTap(_ => someBusinessService(jmsMessage, persistentMessageId))
//            //        res <- echoService(text, mf)
//          } yield res
//        } //.as(ExitCode.Success)
//        x
//      }
//    )
//
//
//
//    y.as(ExitCode.Success)

//    consumerRes.use(_.handle { (jmsMessage, mf) =>
//      for {
//        persistentMessageId <- localMessageStore.persistMessage(jmsMessage)
//        res <- IO.pure(AckAction.ack[IO])
//      } yield res
//    }.as(ExitCode.Success))
  }


//  def toBusinessService[T <: BusinessServiceRequest, U <: BusinessServiceResponse, E <: BusinessServiceError](service: BusinessService[T, U, E])(jmsMessage: JmsMessage, persistentMessageId: PersistentMessageId): IO[String] = {
//    IO.delay {
//
//    }
//  }

  def someBusinessService(jmsMessage: JmsMessage, persistentMessageId: PersistentMessageId): IO[String] = {
    IO.delay {
      println(s"Performing some business service for id: $persistentMessageId")
      jmsMessage.asTextF[Try] match {
        case Success(text) =>
          println(s"Message: $persistentMessageId, contained': $text'")
          s"Result for Message: $persistentMessageId == OK"

        case Failure(e) =>
          sys.error(s"Unable to extract text from message: $persistentMessageId")
          s"Result for Message: $persistentMessageId == ${e.getMessage}"
      }
    }
  }

  def asyncSomeBusinessService(jmsMessage: JmsMessage, persistentMessageId: PersistentMessageId) : IO[Unit] = {
    IO.async_[Unit] { cb =>
      val x :Either[Throwable, Unit] = jmsMessage.attemptAsText.map { text =>
        println(s"Performing some business service for id: $persistentMessageId: $text")
      }.toEither

      cb(x)
      () // TODO(AR) explicit unit return type

    }
  }
}
