/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.contrib.crdt.convergent

import akka.cluster.VectorClock
import akka.actor.Address
import akka.cluster.Cluster

object GCounter {
  def apply(): GCounter = new GCounter
}

/**
 * Implements a ConvergentReplicatedDataType 'Growing Counter' also called a 'G-Counter'.
 *
 * A G-Counter is a increment-only counter (inspired by vector clocks) in
 * which only increment and merge are possible. Incrementing the counter
 * adds 1 to the count for the current actor. Divergent histories are
 * resolved by taking the maximum count for each actor (like a vector
 * clock merge). The value of the counter is the sum of all actor counts.
 */
case class GCounter(
  private[crdt] val state: Map[Address, Int] = Map.empty[Address, Int])
  extends ConvergentReplicatedDataType {

  def value: Int = state.values.sum

  def :+(delta: Int)(implicit cluster: Cluster): GCounter = increment(cluster, delta)

  def increment(node: Cluster, delta: Int = 1): GCounter =
    increment(node.selfAddress, delta)

  private[crdt] def increment(key: Address): GCounter = increment(key, 1)

  private[crdt] def increment(key: Address, delta: Int): GCounter = {
    require(delta >= 0, "Can't decrement a GCounter")
    if (state.contains(key)) copy(state = state + (key -> (state(key) + delta)))
    else copy(state = state + (key -> delta))
  }

  override def merge(that: ConvergentReplicatedDataType): GCounter = that match {
    // FIXME must be a better way
    case a: GCounter ⇒ merge2(a)
    case _ ⇒
      throw new IllegalArgumentException("Can only merge with GCounter, not " + that.getClass.getName)
  }

  private def merge2(that: GCounter): GCounter = {
    (this.state.keySet ++ that.state.keySet).foldLeft(GCounter()) { (counter, key) ⇒
      counter.increment(key, Math.max(this.state.get(key).getOrElse(0), that.state.get(key).getOrElse(0)))
    }
  }
}

