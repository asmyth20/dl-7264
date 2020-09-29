package com.workday.maya

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._

object Control {

  def when1[A, B, C](partition: A => Int, actions: List[Flow[A, B, NotUsed]], map: (A, B) => C): Flow[A, C, NotUsed] = {

    Flow.fromGraph(GraphDSL.create() { implicit builder =>

      val b = builder.add(Broadcast[A](2))
      val p = builder.add(Partition(actions.size, partition))
      val m = builder.add(Merge[B](actions.size))
      actions.foreach(p ~> builder.add(_) ~> m)
      val agg = builder.add(Flow[(A, B)].map(x => map(x._1, x._2)))
      val z = builder.add(Zip[A, B])

      b ~> z.in0
      b ~> p
      m ~> z.in1
      z.out ~> agg

      FlowShape(b.in, agg.out)
    })
  }

  def when2[A, B](partition: A => Int, actions: List[Flow[A, B, NotUsed]]): Flow[A, B, NotUsed] = {

    Flow.fromGraph(GraphDSL.create() { implicit builder =>

      val p = builder.add(Partition(actions.size, partition))
      val m = builder.add(Merge[B](actions.size))
      actions.zipWithIndex foreach { x =>
        p.out(x._2) ~> builder.add(x._1) ~> m
      }

      FlowShape(p.in, m.out)
    })
  }

  def combine[A, B](flow: Flow[A, B, NotUsed]): Flow[A, (A, B), NotUsed] = {
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      val b = builder.add(Broadcast[A](2))
      val z = builder.add(Zip[A, B])

      b ~> z.in0
      b ~> builder.add(flow) ~> z.in1
      z.out
      FlowShape(b.in, z.out)
    })
  }

  def when3[A, B, C](partition: A => Int, actions: List[Flow[A, B, NotUsed]], map: (A, B) => C): Flow[A, C, NotUsed] = {

    Flow.fromGraph(GraphDSL.create() { implicit builder =>

      val p = builder.add(Partition(actions.size, partition))
      val m = builder.add(Merge[(A, B)](actions.size))
      actions.foreach(action => {
        p ~> builder.add(combine(action)) ~> m
      })
      val agg = builder.add(Flow[(A, B)].map(x => map(x._1, x._2)))
      m ~> agg

      FlowShape(p.in, agg.out)
    })
  }
}
