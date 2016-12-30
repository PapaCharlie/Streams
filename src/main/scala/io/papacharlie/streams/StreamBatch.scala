package io.papacharlie.streams

import com.twitter.util.{Await, Future}
import scala.collection.immutable.Queue

class StreamBatch private(underlying: Queue[Future[String]]) {
  def isEmpty: Boolean = underlying.isEmpty
  def head: Future[String] = underlying.head
  def length: Int = underlying.length
  def seq: Seq[Future[String]] = underlying.seq
  def enqueue(event: Future[String]): StreamBatch = new StreamBatch(underlying.enqueue(event))
  def dropCompleted: (StreamBatch, Option[String]) = {
    val newUnderlying = underlying.dropWhile(_.isDefined)
    val lastCompleted =
      // Await doesn't block here, since we're calling it on something that's already defined
      underlying.lift(underlying.length - newUnderlying.length - 1).map(Await.result(_))
    (new StreamBatch(newUnderlying), lastCompleted)
  }
  def lastCompleted: Option[String] =
    underlying.lift(underlying.lastIndexWhere(_.isDefined)).map(Await.result(_))
}

object StreamBatch {
  val empty: StreamBatch = new StreamBatch(Queue.empty)

  def apply(elems: Future[String]*): StreamBatch = {
    new StreamBatch(Queue(elems: _*))
  }

  def unapply(arg: StreamBatch): Some[Seq[Future[String]]] = unapplySeq(arg)
  def unapplySeq(arg: StreamBatch): Some[Seq[Future[String]]] = Some(arg.seq)
}
