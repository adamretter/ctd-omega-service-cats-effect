package uk.gov.nationalarchives.omega.api

import cats.effect.{ExitCode, IO, IOApp, Temporal}
import cats.effect.std.Queue

import java.util.UUID
import scala.concurrent.duration.DurationInt

object QueueExample extends IOApp {

//  val queue = Queue.bounded[IO, String](2)   // can add a bound the queue, which stalls the producer (#offer) if it out-paces the consumer
    val queue = Queue.unbounded[IO, String]

  def produce(q: Queue[IO, String]) : IO[Unit] = {
    for {
      msg <- IO.delay(s"msg: ${UUID.randomUUID()}")
      enqueued <- q.offer(msg)
      _ <- IO.delay(println(s"Enqueued: $msg"))
    } yield enqueued
  }

  def produceThreeForEver(q: Queue[IO, String]): IO[Unit] = {
    // produce 3 messages
    produce(q).replicateA(3) *>
    // sleep for 3 seconds before producing 3 more
    Temporal[IO].sleep(3.seconds) >> produceThreeForEver(q)
  }

  def consume(q: Queue[IO, String]) : IO[String] = {
    for {
      dequeued <- q.take
      _ <- IO.delay(println(s"Dequeued: $dequeued"))
    } yield dequeued
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val x = for {
      q <- queue
      res <- IO.race(
        produceThreeForEver(q),
//        consume(q).foreverM
        (consume(q) *> Temporal[IO].sleep(6.seconds)).foreverM  // slower consumer that shows the producer out pacing it

      )
    } yield res

    x.as(ExitCode.Success)
  }
}
