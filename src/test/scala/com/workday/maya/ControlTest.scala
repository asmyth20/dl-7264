package com.workday.maya

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

@RunWith(classOf[JUnitRunner])
class ControlTest extends WordSpec with Matchers {

  implicit val system: ActorSystem = ActorSystem("ControlTest")

  def partition(i: Int): Int = i % 2

  def keepRight(x: Int, y: Int): Int = y

  val even: Flow[Int, Int, NotUsed] = Flow[Int].map(_ / 2)
  val odd: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 2)
  val oddWithSubStreams: Flow[Int, Int, NotUsed] = Flow[Int]
    .splitWhen(_ => true)
    .flatMapConcat(i => Source(List(i, i)))
    .fold(0)(_ + _)
    .mergeSubstreams

  "oddWithSubstreams works in isolation" in {
    val res = Await.result(Source(0 until 10).filter(_ % 2 == 1).via(oddWithSubStreams).runWith(Sink.seq), 10 seconds)
    res shouldBe Seq(2, 6, 10, 14, 18)
  }

  "when1 processes simple branch flows" in {
    val flow: Flow[Int, Int, NotUsed] = Control.when1(partition, List(even, odd), keepRight)
    val res = Await.result(Source(0 until 10).via(flow).runWith(Sink.seq), 10 seconds)
    res shouldBe Seq(0, 2, 1, 6, 2, 10, 3, 14, 4, 18)
  }

  // times out!
  "when1 should also process branch flows that create and merge substreams" in {
    val flow: Flow[Int, Int, NotUsed] = Control.when1(partition, List(even, oddWithSubStreams), keepRight)
    val res = Await.result(Source(0 until 10).via(flow).runWith(Sink.seq), 10 seconds)
    res shouldBe Seq(0, 2, 1, 6, 2, 10, 3, 14, 4, 18)
  }

  "when2 processes simple branch flows" in {
    val flow: Flow[Int, Int, NotUsed] = Control.when2(partition, List(even, odd))
    val res = Await.result(Source(0 until 10).via(flow).runWith(Sink.seq), 10 seconds)
    res shouldBe Seq(0, 2, 1, 6, 2, 10, 3, 14, 4, 18)
  }

  "when2 should also process branch flows that create and merge substreams" in {
    val flow: Flow[Int, Int, NotUsed] = Control.when2(partition, List(even, oddWithSubStreams))
    val res = Await.result(Source(0 until 10).via(flow).runWith(Sink.seq), 10 seconds)
    // does not block, but unfortunately does not preserve the order
    res should contain theSameElementsAs Seq(0, 2, 1, 6, 2, 10, 3, 14, 4, 18)
  }
}
