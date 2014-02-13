/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.crdt.convergent

import scala.concurrent.duration._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.STMultiNodeSpec
import akka.remote.testkit.MultiNodeSpec
import akka.persistence.Persistence
import com.typesafe.config.ConfigFactory
import akka.cluster.Cluster
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit._

object CrdtSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.log-dead-letters-during-shutdown = off
    """))

  testTransport(on = true)

}

class CrdtSpecMultiJvmNode1 extends CrdtSpec
class CrdtSpecMultiJvmNode2 extends CrdtSpec
class CrdtSpecMultiJvmNode3 extends CrdtSpec

class CrdtSpec extends MultiNodeSpec(CrdtSpec) with STMultiNodeSpec with ImplicitSender {
  import CrdtSpec._
  import Replicator._

  override def initialParticipants = roles.size

  implicit val cluster = Cluster(system)
  val replicator = system.actorOf(Replicator.props(role = None, gossipInterval = 1.second), "replicator")
  val timeout = 2.seconds.dilated

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  "Cluster CRDT" must {

    "work in single node cluster" in {
      join(first, first)

      runOn(first) {

        within(5.seconds) {
          awaitAssert {
            replicator ! Internal.GetNodeCount
            expectMsg(1)
          }
        }

        replicator ! Get("A", ReadOne, timeout)
        expectMsg(NotFound("A"))

        val c3 = GCounter() :+ 3
        replicator ! Update("A", c3, WriteOne, timeout, 1)
        expectMsg(UpdateSuccess("A", 1))
        replicator ! Get("A", ReadOne, timeout)
        expectMsg(GetResult("A", c3))

        val c4 = c3 :+ 1
        // too strong consistency level
        replicator ! Update("A", c4, WriteTwo, timeout, 2)
        expectMsg(UpdateFailure("A", 2))
        replicator ! Get("A", ReadOne, timeout)
        expectMsg(GetResult("A", c4))

        val c5 = c4 :+ 1
        // too strong consistency level
        replicator ! Update("A", c5, WriteQuorum, timeout, 3)
        expectMsg(UpdateFailure("A", 3))
        replicator ! Get("A", ReadOne, timeout)
        expectMsg(GetResult("A", c5))

        val c6 = c5 :+ 1
        replicator ! Update("A", c6, WriteAll, timeout, 4)
        expectMsg(UpdateSuccess("A", 4))
        replicator ! Get("A", ReadAll, timeout)
        expectMsg(GetResult("A", c6))

      }

      enterBarrier("after-1")
    }
  }

  "replicate values to new node" in {
    join(second, first)

    runOn(first, second) {
      within(10.seconds) {
        awaitAssert {
          replicator ! Internal.GetNodeCount
          expectMsg(2)
        }
      }
    }

    enterBarrier("2-nodes")

    runOn(second) {
      // "A" should be replicated via gossip to the new node
      within(5.seconds) {
        awaitAssert {
          replicator ! Get("A", ReadOne, timeout)
          val c = expectMsgPF() { case GetResult("A", c: GCounter) ⇒ c }
          c.value should be(6)
        }
      }
    }

    enterBarrier("after-2")
  }

  "work in 2 node cluster" in {

    runOn(first, second) {

      // start with 20 on both nodes
      val c20 = GCounter() :+ 20
      replicator ! Update("B", c20, WriteOne, timeout, 1)
      expectMsg(UpdateSuccess("B", 1))

      // add 1 on both nodes using WriteTwo
      val c21 = c20 :+ 1
      replicator ! Update("B", c21, WriteTwo, timeout, 2)
      expectMsg(UpdateSuccess("B", 2))

      // the total, after replication should be 42
      awaitAssert {
        replicator ! Get("B", ReadTwo, timeout)
        val c = expectMsgPF() { case GetResult("B", c: GCounter) ⇒ c }
        c.value should be(42)
      }

      // add 1 on both nodes using WriteAll
      val c22 = c21 :+ 1
      replicator ! Update("B", c22, WriteAll, timeout, 3)
      expectMsg(UpdateSuccess("B", 3))

      // the total, after replication should be 44
      awaitAssert {
        replicator ! Get("B", ReadAll, timeout)
        val c = expectMsgPF() { case GetResult("B", c: GCounter) ⇒ c }
        c.value should be(44)
      }

    }

    enterBarrier("after-3")
  }

  "be replicated after succesful update" in {
    runOn(first) {
      val c30 = GCounter() :+ 30
      replicator ! Update("C", c30, WriteTwo, timeout, 1)
      expectMsg(UpdateSuccess("C", 1))
    }
    enterBarrier("update-c30")

    runOn(second) {
      replicator ! Get("C", ReadOne, timeout)
      val c30 = expectMsgPF() { case GetResult("C", c: GCounter) ⇒ c }
      c30.value should be(30)

      // replicate with gossip after WriteOne
      val c31 = c30 :+ 1
      replicator ! Update("C", c31, WriteOne, timeout, 2)
      expectMsg(UpdateSuccess("C", 2))
    }
    enterBarrier("update-c31")

    runOn(first) {
      // "C" should be replicated via gossip to the other node
      within(5.seconds) {
        awaitAssert {
          replicator ! Get("C", ReadOne, timeout)
          val c = expectMsgPF() { case GetResult("C", c: GCounter) ⇒ c }
          c.value should be(31)
        }
      }
    }
    enterBarrier("verified-c31")

    // and also for concurrent updates
    runOn(first, second) {
      replicator ! Get("C", ReadOne, timeout)
      val c31 = expectMsgPF() { case GetResult("C", c: GCounter) ⇒ c }
      c31.value should be(31)

      val c32 = c31 :+ 1
      replicator ! Update("C", c32, WriteOne, timeout, 3)
      expectMsg(UpdateSuccess("C", 3))

      within(5.seconds) {
        awaitAssert {
          replicator ! Get("C", ReadOne, timeout)
          val c = expectMsgPF() { case GetResult("C", c: GCounter) ⇒ c }
          c.value should be(33)
        }
      }
    }

    enterBarrier("after-4")
  }

  "converge after partition" in {
    runOn(first) {
      val c40 = GCounter() :+ 40
      replicator ! Update("D", c40, WriteTwo, timeout, 1)
      expectMsg(UpdateSuccess("D", 1))

      testConductor.blackhole(first, second, Direction.Both).await
    }
    enterBarrier("blackhole-first-second")

    runOn(first, second) {
      replicator ! Get("D", ReadOne, timeout)
      val c40 = expectMsgPF() { case GetResult("D", c: GCounter) ⇒ c }
      c40.value should be(40)
      val c41 = c40 :+ 1
      replicator ! Update("D", c41, WriteTwo, timeout, 2)
      expectMsg(UpdateFailure("D", 2))
      val c42 = c41 :+ 1
      replicator ! Update("D", c42, WriteTwo, timeout, 3)
      expectMsg(UpdateFailure("D", 3))
    }
    enterBarrier("updates-during-partion")

    runOn(first) {
      testConductor.passThrough(first, second, Direction.Both).await
    }
    enterBarrier("passThrough-first-second")

    runOn(first, second) {
      replicator ! Get("D", ReadTwo, timeout)
      val c44 = expectMsgPF() { case GetResult("D", c: GCounter) ⇒ c }
      c44.value should be(44)
    }

    enterBarrier("after-5")
  }

  "support quorum write and read with 3 nodes with 1 unreachable" in {
    join(third, first)

    runOn(first, second, third) {
      within(10.seconds) {
        awaitAssert {
          replicator ! Internal.GetNodeCount
          expectMsg(3)
        }
      }
    }
    enterBarrier("3-nodes")

    runOn(first, second, third) {
      val c50 = GCounter() :+ 50
      replicator ! Update("E", c50, WriteQuorum, timeout, 1)
      expectMsg(UpdateSuccess("E", 1))
    }
    enterBarrier("write-inital-quorum")

    runOn(first, second, third) {
      replicator ! Get("E", ReadQuorum, timeout)
      val c150 = expectMsgPF() { case GetResult("E", c: GCounter) ⇒ c }
      c150.value should be(150)
    }
    enterBarrier("read-inital-quorum")

    runOn(first) {
      testConductor.blackhole(first, third, Direction.Both).await
      testConductor.blackhole(second, third, Direction.Both).await
    }
    enterBarrier("blackhole-third")

    runOn(first) {
      replicator ! Get("E", ReadQuorum, timeout)
      val c150 = expectMsgPF() { case GetResult("E", c: GCounter) ⇒ c }
      c150.value should be(150)
      val c151 = c150 :+ 1
      replicator ! Update("E", c151, WriteQuorum, timeout, 2)
      expectMsg(UpdateSuccess("E", 2))
    }
    enterBarrier("quorum-update-from-first")

    runOn(second) {
      replicator ! Get("E", ReadQuorum, timeout)
      val c151 = expectMsgPF() { case GetResult("E", c: GCounter) ⇒ c }
      c151.value should be(151)
      val c152 = c151 :+ 1
      replicator ! Update("E", c152, WriteQuorum, timeout, 3)
      expectMsg(UpdateSuccess("E", 3))
    }

    runOn(first) {
      testConductor.passThrough(first, third, Direction.Both).await
      testConductor.passThrough(second, third, Direction.Both).await
    }
    enterBarrier("passThrough-third")

    runOn(third) {
      replicator ! Get("E", ReadQuorum, timeout)
      val c152 = expectMsgPF() { case GetResult("E", c: GCounter) ⇒ c }
      c152.value should be(152)
    }

    enterBarrier("after-6")
  }

  "converge after many concurrent updates" in within(10.seconds) {
    runOn(first, second, third) {
      var c = GCounter()
      for (n ← 1 to 100) {
        c :+= 1
        replicator ! Update("F", c, WriteTwo, timeout, n)
      }
      val results = receiveN(100)
      results.map(_.getClass).toSet should be(Set(classOf[UpdateSuccess]))
    }
    enterBarrier("100-updates-done")
    runOn(first, second, third) {
      replicator ! Get("F", ReadTwo, timeout)
      val c = expectMsgPF() { case GetResult("F", c: GCounter) ⇒ c }
      c.value should be(3 * 100)
    }
    enterBarrier("after-7")
  }

}

