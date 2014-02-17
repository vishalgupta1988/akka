/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.crdt.convergent

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.forkjoin.ThreadLocalRandom
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.Address
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.VectorClock

/**
 * The [[Replicator]] actor takes care of direct replication and gossip based
 * dissemination of Conflict Free Replicated Data Types (CRDT) to
 * replicas in the the cluster. The `Replicator` actor is started on each node
 * in the cluster, or group of nodes tagged with a specific role.
 * It requires convergent CRDTs, i.e. they provide a monotonic merge function and
 * the state changes always converge.
 * For good introduction to the subject watch the
 * <a href="http://vimeo.com/43903960">Eventually Consistent Data Structures</a>
 * talk by Sean Cribbs and and the
 * <a href="http://research.microsoft.com/apps/video/dl.aspx?id=153540">talk by Mark Shapiro</a>
 * and the excellent paper <a href="http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf">
 * A comprehensive study of Convergent and Commutative Replicated Data Types</a>
 * by Mark Shapiro et. al.
 *
 * A modified [[ConvergentReplicatedDataType]] is replicated by sending it
 * in a [[Replicator.Update]] message to the the local `Replicator`.
 * You supply a consistency level which has the following meaning:
 * <ul>
 * <li>`WriteOne` the value will only be written to the local replica</li>
 * <li>`WriteTwo` the value will immediately be written to at least two replicas,
 *     including the local replica</li>
 * <li>`WriteThree` the value will immediately be written to at least three replicas,
 *     including the local replica</li>
 * <li>`W(n)` the value will immediately be written to at least `n` replicas,
 *     including the local replica</li>
 * <li>`WriteQuorum` the value will immediately be written to a majority of replicas, i.e.
 *     at least `N/2 + 1` replicas, where N is the number of nodes in the cluster
 *     (or cluster role group)</li>
 * <li>`WriteAll` the value will immediately be written to all nodes in the cluster
 *     (or all nodes in the cluster role group)</li>
 * </ul>
 *
 * As reply of the `Update` a [[Replicator.UpdateSuccess]] is sent to the sender of the
 * `Update` if the value was successfully replicated according to the supplied consistency
 * level within the supplied timeout. Otherwise a [[Replicator.UpdateFailure]] is sent.
 * Note that `UpdateFailure` does not mean that the update completely failed or was rolled back.
 * It may still have been replicated to some nodes, and may eventually be replicated to all
 * nodes. The CRDT will always converge to the the same value no matter how many times you retry
 * the `Update`.
 *
 * To retrieve the current value of a CRDT you send [[Replicator.Get]] message to the
 * `Replicator`. You supply a consistency level which has the following meaning:
 * <ul>
 * <li>`ReadOne` the value will only be read from the local replica</li>
 * <li>`ReadTwo` the value will be read and merged from two replicas,
 *     including the local replica</li>
 * <li>`ReadThree` the value will be read and merged from three replicas,
 *     including the local replica</li>
 * <li>`R(n)` the value will be read and merged from `n` replicas,
 *     including the local replica</li>
 * <li>`ReadQuorum` the value will read and merged from a majority of replicas, i.e.
 *     at least `N/2 + 1` replicas, where N is the number of nodes in the cluster
 *     (or cluster role group)</li>
 * <li>`ReadAll` the value will be read and merged from all nodes in the cluster
 *     (or all nodes in the cluster role group)</li>
 * </ul>
 *
 * As reply of the `Get` a [[Replicator.GetResult]] is sent to the sender of the
 * `Get` if the value was successfully retrieved according to the supplied consistency
 * level within the supplied timeout. Otherwise a [[Replicator.GetFailure]] is sent.
 * If the key does not exist the reply will be [[Replicator.GetFailure]].
 *
 */
object Replicator {

  def props(
    role: Option[String],
    gossipInterval: FiniteDuration = 2.second): Props =
    Props(classOf[Replicator], role, gossipInterval)

  // FIXME Java API props

  sealed trait ReadConsistency
  object ReadOne extends R(1)
  object ReadTwo extends R(2)
  object ReadThree extends R(3)
  case class R(n: Int) extends ReadConsistency
  case object ReadQuorum extends ReadConsistency
  case object ReadAll extends ReadConsistency

  sealed trait WriteConsistency
  object WriteOne extends W(1)
  object WriteTwo extends W(2)
  object WriteThree extends W(3)
  case class W(n: Int) extends WriteConsistency
  case object WriteQuorum extends WriteConsistency
  case object WriteAll extends WriteConsistency

  case class Get(key: String, consistency: ReadConsistency, timeout: FiniteDuration)
  // FIXME correlationId for Get/GetResult?
  case class GetResult(key: String, crdt: ConvergentReplicatedDataType)
  case class NotFound(key: String)
  case class GetFailure(key: String)
  // FIXME might need a version for optimistic locking on local node
  case class Update(key: String, crdt: ConvergentReplicatedDataType, consistency: WriteConsistency,
                    timeout: FiniteDuration, correlationId: Any)
  case class UpdateSuccess(key: String, correlationId: Any)
  case class UpdateFailure(key: String, correlationId: Any)

  /**
   * INTERNAL API
   */
  private[crdt] object Internal {

    case object GossipTick
    case class Write(key: String, vCrdt: VersionedCrdt)
    case object WriteAck
    case class Read(key: String)
    case class ReadResult(vCrdt: Option[VersionedCrdt])
    case class ReadRepair(key: String, vCrdt: VersionedCrdt)

    // We don't need the VectorClock for the CRDT replication itself, 
    // but we use the partial casual ordering of the updates to determine 
    // what delta to send in the Gossip exchange
    case class VersionedCrdt(crdt: ConvergentReplicatedDataType, version: VectorClock) {
      def merge(other: VersionedCrdt): VersionedCrdt = copy(crdt merge other.crdt.asInstanceOf[crdt.T], version merge other.version)
    }
    case class Status(versions: Map[String, VectorClock])
    case class Gossip(crdts: Map[String, VersionedCrdt])

    // Testing purpose
    case object GetNodeCount
    case class NodeCount(n: Int)

  }

}

/**
 * @see [[Replicator$ Replicator companion object]]
 */
class Replicator(
  role: Option[String],
  gossipInterval: FiniteDuration) extends Actor with ActorLogging {

  import Replicator._
  import Replicator.Internal._

  val cluster = Cluster(context.system)
  import cluster.selfAddress
  val vclockNode = selfAddress.toString

  require(!cluster.isTerminated, "Cluster node must not be terminated")
  require(role.forall(cluster.selfRoles.contains),
    s"This cluster member [${selfAddress}] doesn't have the role [$role]")

  //Start periodic gossip to random nodes in cluster
  import context.dispatcher
  val gossipTask = context.system.scheduler.schedule(gossipInterval, gossipInterval, self, GossipTick)

  // other nodes, doesn't contain selfAddress
  var nodes: Set[Address] = Set.empty

  var crdts = Map.empty[String, VersionedCrdt]

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    cluster unsubscribe self
    gossipTask.cancel()
  }

  def matchingRole(m: Member): Boolean = role.forall(m.hasRole)

  def receive = {

    case Get(key, consistency, timeout) ⇒
      val localValue = crdts.get(key)
      if (consistency == ReadOne) {
        val reply = localValue match {
          case Some(VersionedCrdt(crdt, _)) ⇒ GetResult(key, crdt)
          case None                         ⇒ NotFound(key)
        }
        sender() ! reply
      } else
        context.actorOf(Props(classOf[ReadAggregator], key, consistency, timeout, nodes, localValue, sender()))

    case Read(key) ⇒
      sender() ! ReadResult(crdts.get(key))

    case Update(key, crdt, consistency, timeout, correlationId) ⇒
      val merged = change(key, crdt)
      if (consistency == WriteOne)
        sender() ! UpdateSuccess(key, correlationId)
      else
        context.actorOf(Props(classOf[WriteAggregator], key, merged, consistency, timeout, correlationId, nodes, sender()))

    case Write(key, vCrdt) ⇒
      write(key, vCrdt)
      sender() ! WriteAck

    case ReadRepair(key, vCrdt) ⇒
      write(key, vCrdt)

    case GossipTick ⇒
      gossip()

    case Status(otherVersions) ⇒
      if (log.isDebugEnabled)
        log.debug("Received gossip status from [{}], containing [{}]", sender().path.address,
          otherVersions.keys.mkString(", "))

      def isOtherOutdated(key: String, otherV: VectorClock): Boolean =
        crdts.get(key) match {
          case Some(VersionedCrdt(_, v)) if otherV < v || otherV <> v ⇒ true
          case _ ⇒ false
        }
      val otherOutdatedKeys = otherVersions.collect {
        case (key, otherV) if isOtherOutdated(key, otherV) ⇒ key
      }
      val otherMissingKeys = crdts.keySet -- otherVersions.keySet
      // FIXME max limit of number of elements to send in one Gossip message
      val keys = otherMissingKeys ++ otherOutdatedKeys
      if (keys.nonEmpty) {
        if (log.isDebugEnabled)
          log.debug("Sending gossip to [{}], missing [{}], outdated [{}]", sender().path.address,
            otherMissingKeys.mkString(", "), otherOutdatedKeys.mkString(", "))
        val g = Gossip(crdts.collect { case e @ (k, v) if keys(k) ⇒ e })
        sender() ! g
      }

    case Gossip(otherCrdts) ⇒
      if (log.isDebugEnabled)
        log.debug("Received gossip from [{}], containing [{}]", sender().path.address, otherCrdts.keys.mkString(", "))
      otherCrdts.foreach {
        case (key, vCrdt) ⇒
          write(key, vCrdt)
      }

    case state: CurrentClusterState ⇒
      nodes = state.members.collect {
        case m if m.status != MemberStatus.Joining && matchingRole(m) && m.address != selfAddress ⇒ m.address
      }

    case MemberUp(m) ⇒
      if (matchingRole(m) && m.address != selfAddress)
        nodes += m.address

    case MemberRemoved(m, _) ⇒
      if (m.address == selfAddress)
        context stop self
      else if (matchingRole(m)) {
        nodes -= m.address
      }

    case _: MemberEvent ⇒ // not of interest

    case GetNodeCount ⇒
      // selfAddress is not included in the set
      sender() ! NodeCount(nodes.size + 1)
  }

  def change(key: String, crdt: ConvergentReplicatedDataType): VersionedCrdt =
    crdts.get(key) match {
      case Some(vCrdt @ VersionedCrdt(existing, v)) ⇒
        if (existing.getClass == crdt.getClass) {
          val merged = VersionedCrdt(crdt merge existing.asInstanceOf[crdt.T], v :+ vclockNode)
          crdts = crdts.updated(key, merged)
          merged
        } else {
          log.warning("Wrong type for updating [{}], existing type [{}], got [{}]",
            key, existing.getClass.getName, crdt.getClass.getName)
          vCrdt
        }
      case None ⇒
        val vCrdt = VersionedCrdt(crdt, new VectorClock :+ vclockNode)
        crdts = crdts.updated(key, vCrdt)
        vCrdt
    }

  def write(key: String, writeCrdt: VersionedCrdt): Unit =
    crdts.get(key) match {
      case Some(vCrdt @ VersionedCrdt(existing, _)) ⇒
        if (existing.getClass == writeCrdt.crdt.getClass) {
          val merged = writeCrdt merge vCrdt
          crdts = crdts.updated(key, merged)
        } else {
          log.warning("Wrong type for writing [{}], existing type [{}], got [{}]",
            key, existing.getClass.getName, writeCrdt.crdt.getClass.getName)
        }
      case None ⇒
        crdts = crdts.updated(key, writeCrdt)
    }

  def gossip(): Unit = selectRandomNode(nodes.toVector) foreach gossipTo

  def gossipTo(address: Address): Unit =
    // FIXME can we start the exchange by sending something smaller, like MD5 digest
    replica(address) ! Status(crdts.map { case (key, vCrdt) ⇒ (key, vCrdt.version) })

  def selectRandomNode(addresses: immutable.IndexedSeq[Address]): Option[Address] =
    if (addresses.isEmpty) None else Some(addresses(ThreadLocalRandom.current nextInt addresses.size))

  def replica(address: Address): ActorSelection =
    context.actorSelection(self.path.toStringWithAddress(address))
}

/**
 * INTERNAL API
 */
private[akka] abstract class ReadWriteAggregator extends Actor {
  import Replicator.Internal._

  def timeout: FiniteDuration
  def nodes: Set[Address]

  import context.dispatcher
  var timeoutSchedule = context.system.scheduler.scheduleOnce(timeout, self, ReceiveTimeout)

  var remaining = nodes

  override def postStop(): Unit = {
    timeoutSchedule.cancel()
  }

  def replica(address: Address): ActorSelection =
    context.actorSelection(context.parent.path.toStringWithAddress(address))

  def becomeDone(): Unit = {
    if (remaining.isEmpty)
      context.stop(self)
    else {
      // stay around a bit more to collect acks, avoiding deadletters
      context.become(done)
      timeoutSchedule.cancel()
      timeoutSchedule = context.system.scheduler.scheduleOnce(2.seconds, self, ReceiveTimeout)
    }
  }

  def done: Receive = {
    case WriteAck | _: ReadResult ⇒
      remaining -= sender().path.address
      if (remaining.isEmpty) context.stop(self)
    case ReceiveTimeout ⇒ context.stop(self)
  }
}

/**
 * INTERNAL API
 */
private[akka] class WriteAggregator(
  key: String,
  vCrdt: Replicator.Internal.VersionedCrdt,
  consistency: Replicator.WriteConsistency,
  override val timeout: FiniteDuration,
  correlationId: Any,
  override val nodes: Set[Address],
  replyTo: ActorRef) extends ReadWriteAggregator {

  import Replicator._
  import Replicator.Internal._

  val doneWhenRemainingSize = consistency match {
    case W(n)     ⇒ nodes.size - (n - 1)
    case WriteAll ⇒ 0
    case WriteQuorum ⇒
      val N = nodes.size + 1
      if (N < 3) -1
      else {
        val w = N / 2 + 1 // write to at least (N/2+1) nodes
        N - w
      }
  }

  override def preStart(): Unit = {
    // FIXME perhaps not send to all, e.g. for WriteTwo we could start with less
    val writeMsg = Write(key, vCrdt)
    nodes.foreach { replica(_) ! writeMsg }

    if (remaining.size == doneWhenRemainingSize)
      reply(ok = true)
    else if (doneWhenRemainingSize < 0 || remaining.size < doneWhenRemainingSize)
      reply(ok = false)
  }

  def receive = {
    case WriteAck ⇒
      remaining -= sender().path.address
      if (remaining.size == doneWhenRemainingSize)
        reply(ok = true)
    case ReceiveTimeout ⇒ reply(ok = false)
  }

  def reply(ok: Boolean): Unit = {
    if (ok)
      replyTo.tell(UpdateSuccess(key, correlationId), context.parent)
    else
      replyTo.tell(UpdateFailure(key, correlationId), context.parent)
    becomeDone()
  }
}

/**
 * INTERNAL API
 */
private[akka] class ReadAggregator(
  key: String,
  consistency: Replicator.ReadConsistency,
  override val timeout: FiniteDuration,
  override val nodes: Set[Address],
  localValue: Option[Replicator.Internal.VersionedCrdt],
  replyTo: ActorRef) extends ReadWriteAggregator {

  import Replicator._
  import Replicator.Internal._

  var result = localValue
  val doneWhenRemainingSize = consistency match {
    case R(n)    ⇒ nodes.size - (n - 1)
    case ReadAll ⇒ 0
    case ReadQuorum ⇒
      val N = nodes.size + 1
      if (N < 3) -1
      else {
        val r = N / 2 + 1 // read from at least (N/2+1) nodes
        N - r
      }
  }

  override def preStart(): Unit = {
    // FIXME perhaps not send to all, e.g. for ReadTwo we could start with less
    val readMsg = Read(key)
    nodes.foreach { replica(_) ! readMsg }

    if (remaining.size == doneWhenRemainingSize)
      reply(ok = true)
    else if (doneWhenRemainingSize < 0 || remaining.size < doneWhenRemainingSize)
      reply(ok = false)
  }

  def receive = {
    case ReadResult(vCrdt) ⇒
      result = (result, vCrdt) match {
        case (Some(a), Some(b))  ⇒ Some(a.merge(b))
        case (r @ Some(_), None) ⇒ r
        case (None, r @ Some(_)) ⇒ r
        case (None, None)        ⇒ None
      }
      remaining -= sender().path.address
      if (remaining.size == doneWhenRemainingSize)
        reply(ok = true)
    case ReceiveTimeout ⇒ reply(ok = false)
  }

  def reply(ok: Boolean): Unit = {
    val replyMsg = (ok, result) match {
      case (true, Some(vCrdt)) ⇒
        context.parent ! ReadRepair(key, vCrdt)
        GetResult(key, vCrdt.crdt)
      case (true, None) ⇒ NotFound(key)
      case (false, _)   ⇒ GetFailure(key)
    }
    replyTo.tell(replyMsg, context.parent)
    becomeDone()
  }
}

/*
More TODO
- Java API
- Akka extension + config
- subscribe to CRDT changes
- documentation
- protobuf
- removal/pruning of keys and its CRDT
- GC/compaction of removed nodes in CRDTs 
- optimizations
*/ 