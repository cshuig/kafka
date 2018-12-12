/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator.group

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import kafka.common.OffsetAndMetadata
import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import org.apache.kafka.common.TopicPartition

import scala.collection.{Seq, immutable, mutable}

private[group] sealed trait GroupState

/**
 * Group is preparing to rebalance
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to sync group with REBALANCE_IN_PROGRESS
 *         remove member on leave group request
 *         park join group requests from new or existing members until all expected members have joined
 *         allow offset commits from previous generation
 *         allow offset fetch requests
 * transition: some members have joined by the timeout => CompletingRebalance
 *             all members have left the group => Empty
 *             group is removed by partition emigration => Dead
 */
private[group] case object PreparingRebalance extends GroupState

/**
 * Group is awaiting state assignment from the leader
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to offset commits with REBALANCE_IN_PROGRESS
 *         park sync group requests from followers until transition to Stable
 *         allow offset fetch requests
 * transition: sync group with state assignment received from leader => Stable
 *             join group from new member or existing member with updated metadata => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             member failure detected => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private[group] case object CompletingRebalance extends GroupState

/**
 * Group is stable
 *
 * action: respond to member heartbeats normally
 *         respond to sync group from any member with current assignment
 *         respond to join group from followers with matching metadata with current group metadata
 *         allow offset commits from member of current generation
 *         allow offset fetch requests
 * transition: member failure detected via heartbeat => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             leader join-group received => PreparingRebalance
 *             follower join-group with new metadata => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private[group] case object Stable extends GroupState

/**
 * Group has no more members and its metadata is being removed
 *
 * action: respond to join group with UNKNOWN_MEMBER_ID
 *         respond to sync group with UNKNOWN_MEMBER_ID
 *         respond to heartbeat with UNKNOWN_MEMBER_ID
 *         respond to leave group with UNKNOWN_MEMBER_ID
 *         respond to offset commit with UNKNOWN_MEMBER_ID
 *         allow offset fetch requests
 * transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
 */
private[group] case object Dead extends GroupState

/**
  * Group has no more members, but lingers until all offsets have expired. This state
  * also represents groups which use Kafka only for offset commits and have no members.
  *
  * action: respond normally to join group from new members
  *         respond to sync group with UNKNOWN_MEMBER_ID
  *         respond to heartbeat with UNKNOWN_MEMBER_ID
  *         respond to leave group with UNKNOWN_MEMBER_ID
  *         respond to offset commit with UNKNOWN_MEMBER_ID
  *         allow offset fetch requests
  * transition: last offsets removed in periodic expiration task => Dead
  *             join group from a new member => PreparingRebalance
  *             group is removed by partition emigration => Dead
  *             group is removed by expiration => Dead
  */
private[group] case object Empty extends GroupState


private object GroupMetadata {
  /**
    * key：   操作的状态
    * value： 前置条件
    *
    * 实例： 组状态为 Stable或CompletingRebalance或Empty时，才能进行 PreparingRebalance
    */
  private val validPreviousStates: Map[GroupState, Set[GroupState]] =
    Map(Dead -> Set(Stable, PreparingRebalance, CompletingRebalance, Empty, Dead),
      CompletingRebalance -> Set(PreparingRebalance),
      Stable -> Set(CompletingRebalance),
      PreparingRebalance -> Set(Stable, CompletingRebalance, Empty),
      Empty -> Set(PreparingRebalance))

  def loadGroup(groupId: String,
                initialState: GroupState,
                generationId: Int,
                protocolType: String,
                protocol: String,
                leaderId: String,
                members: Iterable[MemberMetadata]): GroupMetadata = {
    val group = new GroupMetadata(groupId, initialState)
    group.generationId = generationId
    group.protocolType = if (protocolType == null || protocolType.isEmpty) None else Some(protocolType)
    group.protocol = Option(protocol)
    group.leaderId = Option(leaderId)
    members.foreach(group.add)
    group
  }
}

/**
 * Case class used to represent group metadata for the ListGroups API
 */
case class GroupOverview(groupId: String,
                         protocolType: String)

/**
 * Case class used to represent group metadata for the DescribeGroup API
 */
case class GroupSummary(state: String,
                        protocolType: String,
                        protocol: String,
                        members: List[MemberSummary])

/**
  * We cache offset commits along with their commit record offset. This enables us to ensure that the latest offset
  * commit is always materialized when we have a mix of transactional and regular offset commits. Without preserving
  * information of the commit record offset, compaction of the offsets topic it self may result in the wrong offset commit
  * being materialized.
  */
case class CommitRecordMetadataAndOffset(appendedBatchOffset: Option[Long], offsetAndMetadata: OffsetAndMetadata) {
  def olderThan(that: CommitRecordMetadataAndOffset) : Boolean = appendedBatchOffset.get < that.appendedBatchOffset.get
}

/**
 * Group contains the following metadata:
 *
 *  Membership metadata:
 *  1. Members registered in this group
 *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *  3. Protocol metadata associated with group members
 *
 *  State metadata:
 *  1. group state
 *  2. generation id
 *  3. leader id
 */
@nonthreadsafe
private[group] class GroupMetadata(val groupId: String, initialState: GroupState) extends Logging {
  private[group] val lock = new ReentrantLock

  private var state: GroupState = initialState
  var protocolType: Option[String] = None
  // 纪元编号
  var generationId = 0
  private var leaderId: Option[String] = None // 组内 只有一个主消费者
  private var protocol: Option[String] = None // 组内 只有一个协议
  // 组内的消费者成员元数据
  private val members = new mutable.HashMap[String, MemberMetadata]
  // 分区提交记录的元数据
  private val offsets = new mutable.HashMap[TopicPartition, CommitRecordMetadataAndOffset]
  private val pendingOffsetCommits = new mutable.HashMap[TopicPartition, OffsetAndMetadata]
  private val pendingTransactionalOffsetCommits = new mutable.HashMap[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]()
  private var receivedTransactionalOffsetCommits = false
  private var receivedConsumerOffsetCommits = false

  var newMemberAdded: Boolean = false

  def inLock[T](fun: => T): T = CoreUtils.inLock(lock)(fun)

  def is(groupState: GroupState) = state == groupState
  def not(groupState: GroupState) = state != groupState
  def has(memberId: String) = members.contains(memberId)
  def get(memberId: String) = members(memberId)

  def isLeader(memberId: String): Boolean = leaderId.contains(memberId)
  def leaderOrNull: String = leaderId.orNull
  def protocolOrNull: String = protocol.orNull

  /**
    * 添加消费者 元数据 到组内， 会更新 Leader Consumer 的编号
    * @param member
    */
  def add(member: MemberMetadata) {
    if (members.isEmpty)
      this.protocolType = Some(member.protocolType)

    assert(groupId == member.groupId)
    assert(this.protocolType.orNull == member.protocolType)
    assert(supportsProtocols(member.protocols))

    if (leaderId.isEmpty)
      leaderId = Some(member.memberId)  // 第一个成员作为： Leader Consumer
    members.put(member.memberId, member)
  }

  def remove(memberId: String) {
    members.remove(memberId)
    if (isLeader(memberId)) { // Leader Consumer被删除
      leaderId = if (members.isEmpty) {
        None
      } else {
        // 下一个成员升级为 Leader Consumer
        Some(members.keys.head)
      }
    }
  }

  def currentState = state

  // 筛选出 awaitingJoinCallback=null 的对象列表; 没有发生 重新加入组 的成员， 已经在 members 中，但是没有回调对象。
  def notYetRejoinedMembers = members.values.filter(_.awaitingJoinCallback == null).toList

  def allMembers = members.keySet

  def allMemberMetadata = members.values.toList

  // 从组内所有的消费者中，选择最大的会话超时时间，作为： 再平衡操作的超时时间
  def rebalanceTimeoutMs = members.values.foldLeft(0) { (timeout, member) =>
    timeout.max(member.rebalanceTimeoutMs)
  }

  // TODO: decide if ids should be predictable or random
  def generateMemberIdSuffix = UUID.randomUUID().toString

  def canRebalance = GroupMetadata.validPreviousStates(PreparingRebalance).contains(state)

  def transitionTo(groupState: GroupState) {
    assertValidTransition(groupState)
    state = groupState
  }

  def selectProtocol: String = {
    if (members.isEmpty)
      throw new IllegalStateException("Cannot select protocol for empty group")

    // select the protocol for this group which is supported by all members
    val candidates = candidateProtocols

    // let each member vote for one of the protocols and choose the one with the most votes
    val votes: List[(String, Int)] = allMemberMetadata
      .map(_.vote(candidates))
      .groupBy(identity)
      .mapValues(_.size)
      .toList

    votes.maxBy(_._2)._1
  }

  private def candidateProtocols = {
    // get the set of protocols that are commonly supported by all members
    allMemberMetadata
      .map(_.protocols)
      .reduceLeft((commonProtocols, protocols) => commonProtocols & protocols)
  }

  def supportsProtocols(memberProtocols: Set[String]) = {
    // 获取memberProtocols和candidateProtocols 的交际
    members.isEmpty || (memberProtocols & candidateProtocols).nonEmpty
  }

  def initNextGeneration() = {
    assert(notYetRejoinedMembers == List.empty[MemberMetadata])
    if (members.nonEmpty) {
      generationId += 1
      protocol = Some(selectProtocol)
      logger.info("[加入组请求] group-state 从 [{}] 切换到 [CompletingRebalance]", currentState);
      transitionTo(CompletingRebalance)
    } else {
      generationId += 1
      protocol = None
      logger.info("[加入组请求] group-state 从 [{}] 切换到 [Empty]", currentState);
      transitionTo(Empty)
    }
    receivedConsumerOffsetCommits = false
    receivedTransactionalOffsetCommits = false
  }

  def currentMemberMetadata: Map[String, Array[Byte]] = {
    if (is(Dead) || is(PreparingRebalance))
      throw new IllegalStateException("Cannot obtain member metadata for group in state %s".format(state))
    members.map{ case (memberId, memberMetadata) => (memberId, memberMetadata.metadata(protocol.get))}.toMap
  }

  def summary: GroupSummary = {
    if (is(Stable)) {
      val protocol = protocolOrNull
      if (protocol == null)
        throw new IllegalStateException("Invalid null group protocol for stable group")

      val members = this.members.values.map { member => member.summary(protocol) }
      GroupSummary(state.toString, protocolType.getOrElse(""), protocol, members.toList)
    } else {
      val members = this.members.values.map{ member => member.summaryNoMetadata() }
      GroupSummary(state.toString, protocolType.getOrElse(""), GroupCoordinator.NoProtocol, members.toList)
    }
  }

  def overview: GroupOverview = {
    GroupOverview(groupId, protocolType.getOrElse(""))
  }

  def initializeOffsets(offsets: collection.Map[TopicPartition, CommitRecordMetadataAndOffset],
                        pendingTxnOffsets: Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]) {
    this.offsets ++= offsets
    this.pendingTransactionalOffsetCommits ++= pendingTxnOffsets
  }

  def onOffsetCommitAppend(topicPartition: TopicPartition, offsetWithCommitRecordMetadata: CommitRecordMetadataAndOffset) {
    if (pendingOffsetCommits.contains(topicPartition)) {
      if (offsetWithCommitRecordMetadata.appendedBatchOffset.isEmpty)
        throw new IllegalStateException("Cannot complete offset commit write without providing the metadata of the record " +
          "in the log.")
      if (!offsets.contains(topicPartition) || offsets(topicPartition).olderThan(offsetWithCommitRecordMetadata))
        offsets.put(topicPartition, offsetWithCommitRecordMetadata)
    }

    pendingOffsetCommits.get(topicPartition) match {
      case Some(stagedOffset) if offsetWithCommitRecordMetadata.offsetAndMetadata == stagedOffset =>
        pendingOffsetCommits.remove(topicPartition)
      case _ =>
        // The pendingOffsetCommits for this partition could be empty if the topic was deleted, in which case
        // its entries would be removed from the cache by the `removeOffsets` method.
    }
  }

  def failPendingOffsetWrite(topicPartition: TopicPartition, offset: OffsetAndMetadata): Unit = {
    pendingOffsetCommits.get(topicPartition) match {
      case Some(pendingOffset) if offset == pendingOffset => pendingOffsetCommits.remove(topicPartition)
      case _ =>
    }
  }

  def prepareOffsetCommit(offsets: Map[TopicPartition, OffsetAndMetadata]) {
    receivedConsumerOffsetCommits = true
    // 先将 offset 信息更新到 group 的 pendingOffsetCommits 中（这时还没有真正提交，后面如果失败的话，是可以撤回的）
    pendingOffsetCommits ++= offsets
  }

  def prepareTxnOffsetCommit(producerId: Long, offsets: Map[TopicPartition, OffsetAndMetadata]) {
    trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $offsets is pending")
    receivedTransactionalOffsetCommits = true
    val producerOffsets = pendingTransactionalOffsetCommits.getOrElseUpdate(producerId,
      mutable.Map.empty[TopicPartition, CommitRecordMetadataAndOffset])

    offsets.foreach { case (topicPartition, offsetAndMetadata) =>
      producerOffsets.put(topicPartition, CommitRecordMetadataAndOffset(None, offsetAndMetadata))
    }
  }

  def hasReceivedConsistentOffsetCommits : Boolean = {
    !receivedConsumerOffsetCommits || !receivedTransactionalOffsetCommits
  }

  /* Remove a pending transactional offset commit if the actual offset commit record was not written to the log.
   * We will return an error and the client will retry the request, potentially to a different coordinator.
   */
  def failPendingTxnOffsetCommit(producerId: Long, topicPartition: TopicPartition): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffsets) =>
        val pendingOffsetCommit = pendingOffsets.remove(topicPartition)
        trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetCommit failed " +
          s"to be appended to the log")
        if (pendingOffsets.isEmpty)
          pendingTransactionalOffsetCommits.remove(producerId)
      case _ =>
        // We may hit this case if the partition in question has emigrated already.
    }
  }

  def onTxnOffsetCommitAppend(producerId: Long, topicPartition: TopicPartition,
                              commitRecordMetadataAndOffset: CommitRecordMetadataAndOffset) {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffset) =>
        if (pendingOffset.contains(topicPartition)
          && pendingOffset(topicPartition).offsetAndMetadata == commitRecordMetadataAndOffset.offsetAndMetadata)
          pendingOffset.update(topicPartition, commitRecordMetadataAndOffset)
      case _ =>
        // We may hit this case if the partition in question has emigrated.
    }
  }

  /* Complete a pending transactional offset commit. This is called after a commit or abort marker is fully written
   * to the log.
   */
  def completePendingTxnOffsetCommit(producerId: Long, isCommit: Boolean): Unit = {
    val pendingOffsetsOpt = pendingTransactionalOffsetCommits.remove(producerId)
    if (isCommit) {
      pendingOffsetsOpt.foreach { pendingOffsets =>
        pendingOffsets.foreach { case (topicPartition, commitRecordMetadataAndOffset) =>
          if (commitRecordMetadataAndOffset.appendedBatchOffset.isEmpty)
            throw new IllegalStateException(s"Trying to complete a transactional offset commit for producerId $producerId " +
              s"and groupId $groupId even though the offset commit record itself hasn't been appended to the log.")

          val currentOffsetOpt = offsets.get(topicPartition)
          if (currentOffsetOpt.forall(_.olderThan(commitRecordMetadataAndOffset))) {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              "committed and loaded into the cache.")
            offsets.put(topicPartition, commitRecordMetadataAndOffset)
          } else {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              s"committed, but not loaded since its offset is older than current offset $currentOffsetOpt.")
          }
        }
      }
    } else {
      trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetsOpt aborted")
    }
  }

  def activeProducers = pendingTransactionalOffsetCommits.keySet

  def hasPendingOffsetCommitsFromProducer(producerId: Long) =
    pendingTransactionalOffsetCommits.contains(producerId)

  /**
    *
    * @param topicPartitions  被删除的 topic分区 集合
    * @return 返回每个topic分区对应的 offset元数据对象
    */
  def removeOffsets(topicPartitions: Seq[TopicPartition]): immutable.Map[TopicPartition, OffsetAndMetadata] = {
    topicPartitions.flatMap { topicPartition =>

      pendingOffsetCommits.remove(topicPartition)
      pendingTransactionalOffsetCommits.foreach { case (_, pendingOffsets) =>
        pendingOffsets.remove(topicPartition)
      }
      val removedOffset = offsets.remove(topicPartition)
      removedOffset.map(topicPartition -> _.offsetAndMetadata)
    }.toMap
  }

  def removeExpiredOffsets(startMs: Long) : Map[TopicPartition, OffsetAndMetadata] = {
    val expiredOffsets = offsets
      .filter {
        case (topicPartition, commitRecordMetadataAndOffset) =>
          commitRecordMetadataAndOffset.offsetAndMetadata.expireTimestamp < startMs && !pendingOffsetCommits.contains(topicPartition)
      }
      .map {
        case (topicPartition, commitRecordOffsetAndMetadata) =>
          (topicPartition, commitRecordOffsetAndMetadata.offsetAndMetadata)
      }
    offsets --= expiredOffsets.keySet
    expiredOffsets.toMap
  }

  def allOffsets = offsets.map { case (topicPartition, commitRecordMetadataAndOffset) =>
    (topicPartition, commitRecordMetadataAndOffset.offsetAndMetadata)
  }.toMap

  def offset(topicPartition: TopicPartition): Option[OffsetAndMetadata] = offsets.get(topicPartition).map(_.offsetAndMetadata)

  // visible for testing
  private[group] def offsetWithRecordMetadata(topicPartition: TopicPartition): Option[CommitRecordMetadataAndOffset] = offsets.get(topicPartition)

  def numOffsets = offsets.size

  def hasOffsets = offsets.nonEmpty || pendingOffsetCommits.nonEmpty || pendingTransactionalOffsetCommits.nonEmpty

  private def assertValidTransition(targetState: GroupState) {
    if (!GroupMetadata.validPreviousStates(targetState).contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
        .format(groupId, GroupMetadata.validPreviousStates(targetState).mkString(","), targetState, state))
  }

  override def toString: String = {
    "GroupMetadata(" +
      s"groupId=$groupId, " +
      s"generation=$generationId, " +
      s"protocolType=$protocolType, " +
      s"currentState=$currentState, " +
      s"members=$members)"
  }

}

