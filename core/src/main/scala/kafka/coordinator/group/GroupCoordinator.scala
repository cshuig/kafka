/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.OffsetAndMetadata
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch.{NO_PRODUCER_EPOCH, NO_PRODUCER_ID}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Seq, immutable}
import scala.math.max

/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 * <p>
 * <b>Delayed operation locking notes:</b>
 * Delayed operations in GroupCoordinator use `group` as the delayed operation
 * lock. ReplicaManager.appendRecords may be invoked while holding the group lock
 * used by its callback.  The delayed callback may acquire the group lock
 * since the delayed operation is completed only if the group lock can be acquired.
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time) extends Logging {
  import GroupCoordinator._

  // 将回调方法定义为一个 ： 高级类型
  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = (Array[Byte], Errors) => Unit

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enableMetadataExpiration: Boolean = true) {
    info("Starting up.")
    groupManager.startup(enableMetadataExpiration)
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  /**
    * 不同消费者在不同时刻发送请求给服务端，服务端并不会立即发送响应结果给消费者。
    * 比如： 服务端会等到 收集完 所有的消费者成员信息后才发送 "JoinGroup 响应"：
    *   给Leader Consumer 响应组内所有消费者成员的订阅信息
    *   给普通 Consumer 响应空信息
    * 等到收到主消费者发送的 "消费组分配结果" 才会发送 "SyncGroup响应"
    *   此时组内所有的消费者都会收到分配给自己的 topic 分区信息
    * @param groupId
    * @param memberId
    * @param clientId
    * @param clientHost
    * @param rebalanceTimeoutMs
    * @param sessionTimeoutMs
    * @param protocolType
    * @param protocols
    * @param responseCallback
    */
  def handleJoinGroup(groupId: String,
                      memberId: String,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback) {
    logger.info("收到新的加入请求：groupId={}, memberId={}, clientId={}, clientHost={}, rebalanceTimeoutMs={}, sessionTimeoutMs={}, protocolType={}, protocols={}", groupId, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols)
    if (!isActive.get) {
      // 协调器不可用，比如未启动成功、被关闭等
      responseCallback(joinError(memberId, Errors.COORDINATOR_NOT_AVAILABLE))
    } else if (!validGroupId(groupId)) {
      //消费者客户端传递的 groupId 无效，如为空
      responseCallback(joinError(memberId, Errors.INVALID_GROUP_ID))
    } else if (!isCoordinatorForGroup(groupId)) {
      // 消费者连接错了 协调器
      responseCallback(joinError(memberId, Errors.NOT_COORDINATOR))
    } else if (isCoordinatorLoadInProgress(groupId)) {
      // 协调器正在加载， 通常是协调器自身在进行迁移
      responseCallback(joinError(memberId, Errors.COORDINATOR_LOAD_IN_PROGRESS))
    } else if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
               sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      // 消费者客户端设置的会话超时时间无效
      responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT))
    } else {
      // only try to create the group if the group is not unknown AND
      // the member id is UNKNOWN, if member is specified but group does not
      // exist we should reject the request
      groupManager.getGroup(groupId) match {
        case None =>
          if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {
            // 不存在这个消费组， 但这个消费者却有 消费组内的 成员编号
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
          } else {
            val group = groupManager.addGroup(new GroupMetadata(groupId, initialState = Empty))
            doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
          }

        case Some(group) =>
          doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
      }
    }
  }

  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          clientId: String,
                          clientHost: String,
                          rebalanceTimeoutMs: Int,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback) {
    group.inLock {
      if (!group.is(Empty) && (!group.protocolType.contains(protocolType) || !group.supportsProtocols(protocols.map(_._1).toSet))) {
        // 消费组不为空 且 请求加入的消费者带来的协议不受组支持，则拒绝加入
        // if the new member does not support the group protocol, reject it
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else if (group.is(Empty) && (protocols.isEmpty || protocolType.isEmpty)) {
        // 第一个成员 协议为空或者 协议类型为空，则拒绝加入。因为第一个加入的消费者，会被当做 Leader Consumer
        //reject if first member with empty group protocol or protocolType is empty
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // IF THE MEMBER TRYING TO REGISTER WITH A UN-RECOGNIZED ID, SEND THE RESPONSE TO LET
        // it reset its member id and retry

        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
      } else {
        group.currentState match {
          case Dead =>
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
          case PreparingRebalance =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              logger.info("[加入组请求] group-state={}, 组内不存在memeberId={}，执行新增成员操作", group.currentState, memberId)
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
            } else {
              val member = group.get(memberId)
              logger.info("[加入组请求] group-state={}, 当前组已经存在memeberId={}，执行修改成员操作", group.currentState, member.memberId)
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            }

          case CompletingRebalance =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              logger.info("[加入组请求] group-state={}, 组内不存在memeberId={}，执行新增成员操作", group.currentState, memberId)
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
            } else {
              val member = group.get(memberId)
              if (member.matches(protocols)) {
                // member is joining with the same metadata (which could be because it failed to
                // receive the initial JoinGroup response), so just return current group information
                // for the current generation.
                responseCallback(JoinGroupResult(
                  members = if (group.isLeader(memberId)) {
                    group.currentMemberMetadata
                  } else {
                    Map.empty
                  },
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocolOrNull,
                  leaderId = group.leaderOrNull,
                  error = Errors.NONE))
              } else {
                // member has changed metadata, so force a rebalance
                logger.info("[加入组请求] group-state={}, 当前组已经存在memeberId={}，执行修改成员操作", group.currentState, member.memberId)
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              }
            }

          case Empty | Stable =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              // if the member id is unknown, register the member to the group
              logger.info("[加入组请求] group-state={}, 组内不存在memeberId={}，执行新增成员操作", group.currentState, memberId)
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
            } else {
              val member = group.get(memberId)
              if (group.isLeader(memberId) || !member.matches(protocols)) {
                // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                // The latter allows the leader to trigger rebalances for changes affecting assignment
                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                logger.info("[加入组请求] group-state={}, 当前组已经存在memeberId={}，执行修改成员操作", group.currentState, member.memberId)
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              } else {
                // for followers with no actual change to their metadata, just return group information
                // for the current generation which will allow them to issue SyncGroup
                responseCallback(JoinGroupResult(
                  members = Map.empty,
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocolOrNull,
                  leaderId = group.leaderOrNull,
                  error = Errors.NONE))
              }
            }
        }

        if (group.is(PreparingRebalance))
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))
      }
    }
  }

  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback) {
    if (!isActive.get) {
      responseCallback(Array.empty, Errors.COORDINATOR_NOT_AVAILABLE)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Array.empty, Errors.NOT_COORDINATOR)
    } else {
      groupManager.getGroup(groupId) match {
        case None => responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
        case Some(group) => doSyncGroup(group, generation, memberId, groupAssignment, responseCallback)
      }
    }
  }

  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback) {
    group.inLock {
      if (!group.has(memberId)) {
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
      } else if (generationId != group.generationId) {
        responseCallback(Array.empty, Errors.ILLEGAL_GENERATION)
      } else {
        logger.info("[同步组请求] 当前组状态:group-state={}", group.currentState)
        group.currentState match {
          case Empty | Dead =>
            responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)

          case PreparingRebalance =>
            responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS)

          case CompletingRebalance =>
            group.get(memberId).awaitingSyncCallback = responseCallback

            // if this is the leader, then we can attempt to persist state and transition to stable
            if (group.isLeader(memberId)) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              val missing = group.allMembers -- groupAssignment.keySet
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              groupManager.storeGroup(group, assignment, (error: Errors) => {
                group.inLock {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the CompletingRebalance state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  if (group.is(CompletingRebalance) && generationId == group.generationId) {
                    if (error != Errors.NONE) {
                      logger.info("[同步组请求] 存储回调异常，当前组状态:group-state={}", group.currentState)
                      resetAndPropagateAssignmentError(group, error)
                      maybePrepareRebalance(group)
                    } else {
                      setAndPropagateAssignment(group, assignment)
                      logger.info("[同步组请求] group-state 从 [{}] 切换到 [Stable]", group.currentState);
                      group.transitionTo(Stable)
                    }
                  }
                }
              })
            }

          case Stable =>
            logger.info("[同步组请求] 当前组是文档状态，直接返回当前组的分配信息")
            // if the group is stable, we just return the current assignment
            val memberMetadata = group.get(memberId)
            responseCallback(memberMetadata.assignment, Errors.NONE)
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
        }
      }
    }
  }

  def handleLeaveGroup(groupId: String, memberId: String, responseCallback: Errors => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.COORDINATOR_NOT_AVAILABLE)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR)
    } else if (isCoordinatorLoadInProgress(groupId)) {
      responseCallback(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    } else {
      groupManager.getGroup(groupId) match {
        case None =>
          // if the group is marked as dead, it means some other thread has just removed the group
          // from the coordinator metadata; this is likely that the group has migrated to some other
          // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
          // joining without specified consumer id,
          responseCallback(Errors.UNKNOWN_MEMBER_ID)

        case Some(group) =>
          group.inLock {
            if (group.is(Dead) || !group.has(memberId)) {
              responseCallback(Errors.UNKNOWN_MEMBER_ID)
            } else {
              val member = group.get(memberId)
              removeHeartbeatForLeavingMember(group, member)
              debug(s"Member ${member.memberId} in group ${group.groupId} has left, removing it from the group")
              removeMemberAndUpdateGroup(group, member)
              responseCallback(Errors.NONE)
            }
          }
      }
    }
  }

  def handleDeleteGroups(groupIds: Set[String]): Map[String, Errors] = {
    if (!isActive.get) {
      groupIds.map(_ -> Errors.COORDINATOR_NOT_AVAILABLE).toMap
    } else {
      var groupErrors: Map[String, Errors] = Map()
      var eligibleGroups: Seq[GroupMetadata] = Seq()

      groupIds.foreach { groupId =>
        if (!validGroupId(groupId))
          groupErrors += groupId -> Errors.INVALID_GROUP_ID
        else if (!isCoordinatorForGroup(groupId))
          groupErrors += groupId -> Errors.NOT_COORDINATOR
        else if (isCoordinatorLoadInProgress(groupId))
          groupErrors += groupId -> Errors.COORDINATOR_LOAD_IN_PROGRESS
        else {
          groupManager.getGroup(groupId) match {
            case None =>
              groupErrors += groupId ->
                (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
            case Some(group) =>
              group.inLock {
                group.currentState match {
                  case Dead =>
                    groupErrors += groupId ->
                      (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
                  case Empty =>
                    logger.info("[删除组请求] group-state 从 [{}] 切换到 [Dead]", group.currentState);
                    group.transitionTo(Dead)
                    eligibleGroups :+= group
                  case _ =>
                    groupErrors += groupId -> Errors.NON_EMPTY_GROUP
                }
              }
          }
        }
      }

      if (eligibleGroups.nonEmpty) {
        groupManager.cleanupGroupMetadata(None, eligibleGroups, Long.MaxValue)
        groupErrors ++= eligibleGroups.map(_.groupId -> Errors.NONE).toMap
        info(s"The following groups were deleted: ${eligibleGroups.map(_.groupId).mkString(", ")}")
      }

      groupErrors
    }
  }

  def handleHeartbeat(groupId: String,
                      memberId: String,
                      generationId: Int,
                      responseCallback: Errors => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.COORDINATOR_NOT_AVAILABLE)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR)
    } else if (isCoordinatorLoadInProgress(groupId)) {
      // the group is still loading, so respond just blindly
      responseCallback(Errors.NONE)
    } else {
      groupManager.getGroup(groupId) match {
        case None =>
          responseCallback(Errors.UNKNOWN_MEMBER_ID)

        case Some(group) =>
          group.inLock {
            group.currentState match {
              case Dead =>
                // if the group is marked as dead, it means some other thread has just removed the group
                // from the coordinator metadata; this is likely that the group has migrated to some other
                // coordinator OR the group is in a transient unstable phase. Let the member retry
                // joining without the specified member id,
                responseCallback(Errors.UNKNOWN_MEMBER_ID)

              case Empty =>
                responseCallback(Errors.UNKNOWN_MEMBER_ID)

              case CompletingRebalance =>
                if (!group.has(memberId))
                  responseCallback(Errors.UNKNOWN_MEMBER_ID)
                else
                  responseCallback(Errors.REBALANCE_IN_PROGRESS)

              case PreparingRebalance =>
                if (!group.has(memberId)) {
                  responseCallback(Errors.UNKNOWN_MEMBER_ID)
                } else if (generationId != group.generationId) {
                  responseCallback(Errors.ILLEGAL_GENERATION)
                } else {
                  val member = group.get(memberId)
                  completeAndScheduleNextHeartbeatExpiration(group, member)
                  responseCallback(Errors.REBALANCE_IN_PROGRESS)
                }

              case Stable =>
                if (!group.has(memberId)) {
                  responseCallback(Errors.UNKNOWN_MEMBER_ID)
                } else if (generationId != group.generationId) {
                  responseCallback(Errors.ILLEGAL_GENERATION)
                } else {
                  val member = group.get(memberId)
                  completeAndScheduleNextHeartbeatExpiration(group, member)
                  responseCallback(Errors.NONE)
                }
            }
          }
      }
    }
  }

  def handleTxnCommitOffsets(groupId: String,
                             producerId: Long,
                             producerEpoch: Short,
                             offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                             responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroup(groupId) match {
      case Some(error) => responseCallback(offsetMetadata.mapValues(_ => error))
      case None =>
        val group = groupManager.getGroup(groupId).getOrElse {
          groupManager.addGroup(new GroupMetadata(groupId, initialState = Empty))
        }
        doCommitOffsets(group, NoMemberId, NoGeneration, producerId, producerEpoch, offsetMetadata, responseCallback)
    }
  }

  /**
    * @param groupId
    * @param memberId
    * @param generationId
    * @param offsetMetadata
    * @param responseCallback
    */
  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Errors] => Unit) {
    validateGroup(groupId) match {
      case Some(error) => responseCallback(offsetMetadata.mapValues(_ => error))
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            if (generationId < 0) {
              // the group is not relying on Kafka for group management, so allow the commit
              val group = groupManager.addGroup(new GroupMetadata(groupId, initialState = Empty))
              doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
                offsetMetadata, responseCallback)
            } else {
              // or this is a request coming from an older generation. either way, reject the commit
              responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION))
            }

          case Some(group) =>
            doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
              offsetMetadata, responseCallback)
        }
    }
  }

  def scheduleHandleTxnCompletion(producerId: Long,
                                  offsetsPartitions: Iterable[TopicPartition],
                                  transactionResult: TransactionResult) {
    require(offsetsPartitions.forall(_.topic == Topic.GROUP_METADATA_TOPIC_NAME))
    val isCommit = transactionResult == TransactionResult.COMMIT
    groupManager.scheduleHandleTxnCompletion(producerId, offsetsPartitions.map(_.partition).toSet, isCommit)
  }

  private def doCommitOffsets(group: GroupMetadata,
                              memberId: String,
                              generationId: Int,
                              producerId: Long,
                              producerEpoch: Short,
                              offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                              responseCallback: immutable.Map[TopicPartition, Errors] => Unit) {
    group.inLock {
      if (group.is(Dead)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID))
      } else if ((generationId < 0 && group.is(Empty)) || (producerId != NO_PRODUCER_ID)) {
        // The group is only using Kafka to store offsets.
        // Also, for transactional offset commits we don't need to validate group membership and the generation.
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback, producerId, producerEpoch)
      } else if (group.is(CompletingRebalance)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS))
      } else if (!group.has(memberId)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID))
      } else if (generationId != group.generationId) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION))
      } else {
        val member = group.get(memberId)
        completeAndScheduleNextHeartbeatExpiration(group, member)
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)
      }
    }
  }

  def handleFetchOffsets(groupId: String,
                         partitions: Option[Seq[TopicPartition]] = None): (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {
    if (!isActive.get)
      (Errors.COORDINATOR_NOT_AVAILABLE, Map())
    else if (!isCoordinatorForGroup(groupId)) {
      debug(s"Could not fetch offsets for group $groupId (not group coordinator)")
      (Errors.NOT_COORDINATOR, Map())
    } else if (isCoordinatorLoadInProgress(groupId))
      (Errors.COORDINATOR_LOAD_IN_PROGRESS, Map())
    else {
      // return offsets blindly regardless the current group state since the group may be using
      // Kafka commit storage without automatic group management
      (Errors.NONE, groupManager.getOffsets(groupId, partitions))
    }
  }

  def handleListGroups(): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading) Errors.COORDINATOR_LOAD_IN_PROGRESS else Errors.NONE
      (errorCode, groupManager.currentGroups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    if (!isActive.get) {
      (Errors.COORDINATOR_NOT_AVAILABLE, GroupCoordinator.EmptyGroup)
    } else if (!isCoordinatorForGroup(groupId)) {
      (Errors.NOT_COORDINATOR, GroupCoordinator.EmptyGroup)
    } else if (isCoordinatorLoadInProgress(groupId)) {
      (Errors.COORDINATOR_LOAD_IN_PROGRESS, GroupCoordinator.EmptyGroup)
    } else {
      groupManager.getGroup(groupId) match {
        case None => (Errors.NONE, GroupCoordinator.DeadGroup)
        case Some(group) =>
          group.inLock {
            (Errors.NONE, group.summary)
          }
      }
    }
  }

  def handleDeletedPartitions(topicPartitions: Seq[TopicPartition]) {
    groupManager.cleanupGroupMetadata(Some(topicPartitions), groupManager.currentGroups, time.milliseconds())
  }

  private def validateGroup(groupId: String): Option[Errors] = {
    if (!isActive.get)
      Some(Errors.COORDINATOR_NOT_AVAILABLE)
    else if (!isCoordinatorForGroup(groupId))
      Some(Errors.NOT_COORDINATOR)
    else if (isCoordinatorLoadInProgress(groupId))
      Some(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    else
      None
  }

  private def onGroupUnloaded(group: GroupMetadata) {
    group.inLock {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState
      logger.info("[onGroupUnloaded] group-state 从 [{}] 切换到 [Dead]", group.currentState);
      group.transitionTo(Dead)

      previousState match {
        case Empty | Dead =>
        case PreparingRebalance =>
          for (member <- group.allMemberMetadata) {
            if (member.awaitingJoinCallback != null) {
              member.awaitingJoinCallback(joinError(member.memberId, Errors.NOT_COORDINATOR))
              member.awaitingJoinCallback = null
            }
          }
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        case Stable | CompletingRebalance =>
          for (member <- group.allMemberMetadata) {
            if (member.awaitingSyncCallback != null) {
              member.awaitingSyncCallback(Array.empty[Byte], Errors.NOT_COORDINATOR)
              member.awaitingSyncCallback = null
            }
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  private def onGroupLoaded(group: GroupMetadata) {
    group.inLock {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable) || group.is(Empty))
      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  def handleGroupImmigration(offsetTopicPartitionId: Int) {
    groupManager.loadGroupsForPartition(offsetTopicPartitionId, onGroupLoaded)
  }

  def handleGroupEmigration(offsetTopicPartitionId: Int) {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]) {
    assert(group.is(CompletingRebalance))
    println("将组内的所有成员的 assignment 赋予 各自的分配结果")
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    propagateAssignment(group, Errors.NONE)
  }

  private def resetAndPropagateAssignmentError(group: GroupMetadata, error: Errors) {
    assert(group.is(CompletingRebalance))
    println("将组内的所有成员的 assignment 置空")
    group.allMemberMetadata.foreach(_.assignment = Array.empty[Byte])
    propagateAssignment(group, error)
  }

  private def propagateAssignment(group: GroupMetadata, error: Errors) {
    for (member <- group.allMemberMetadata) {
      if (member.awaitingSyncCallback != null) {
        // 先响应给客户端，在置空
        println("响应给客户端 member=" + member.memberId)
        member.awaitingSyncCallback(member.assignment, error)
        member.awaitingSyncCallback = null

        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  private def validGroupId(groupId: String): Boolean = {
    groupId != null && !groupId.isEmpty
  }

  private def joinError(memberId: String, error: Errors): JoinGroupResult = {
    JoinGroupResult(
      members = Map.empty,
      memberId = memberId,
      generationId = 0,
      subProtocol = GroupCoordinator.NoProtocol,
      leaderId = GroupCoordinator.NoLeader,
      error = error)
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata) {
    // complete current heartbeat expectation
    member.latestHeartbeat = time.milliseconds()
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)

    // reschedule the next heartbeat expiration deadline
    val newHeartbeatDeadline = member.latestHeartbeat + member.sessionTimeoutMs
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs)
    logger.info("尝试完成或添加成员心跳, memberKey={}", memberKey)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata) {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  /**
    * 此方法表示：消费组元数据 中没有该消费者，需要添加消费者的 成员元数据
    * 添加成员 并 执行再平衡
    * @param rebalanceTimeoutMs   再平衡超时时间
    * @param sessionTimeoutMs     消费者的会话超时时间
    * @param clientId             客户端编号
    * @param clientHost           客户端地址
    * @param protocolType         协议类型
    * @param protocols            消费者协议和元数据
    * @param group                分组元数据
    * @param callback             响应回调方法
    * @return
    */
  private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                    sessionTimeoutMs: Int,
                                    clientId: String,
                                    clientHost: String,
                                    protocolType: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback) = {
    // 为加入组请求的消费者设置成员编号
    val memberId = clientId + "-" + group.generateMemberIdSuffix
    // 封装消费者组内的 成员元数据对象
    val member = new MemberMetadata(memberId, group.groupId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols)
    // 消费者响应回调； 是一个值对象
    member.awaitingJoinCallback = callback
    // update the newmemberadded flag to indicate that the join group can be further delayed
    // 更新 newmemberadded 标志以指示可以进一步延迟加入组
    if (group.is(PreparingRebalance) && group.generationId == 0)
      group.newMemberAdded = true

    group.add(member)
    // 可能需要执行 再平衡
    maybePrepareRebalance(group)
    member
  }

  /**
    * 此方法表示：消费组元数据 已有该消费者，只需要更新消费者的 成员元数据
    *
    * 更新成员，并执行再平衡
    *
    * @param group  消费组元数据对象
    * @param member 组内的成员元数据对象
    * @param protocols  消费者的协议和元数据
    * @param callback 响应回调函数
    */
  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback) {
    member.supportedProtocols = protocols
    member.awaitingJoinCallback = callback
    // 可能需要执行 再平衡
    maybePrepareRebalance(group)
  }

  /**
    *  添加 或 更新消费者的成员元数据， 可能需要执行 再平衡
    * @param group
    */
  private def maybePrepareRebalance(group: GroupMetadata) {
    group.inLock {
      if (group.canRebalance) {
        logger.info("触发了在平衡条件: PreparingRebalance -> Set(Stable, CompletingRebalance, Empty), 当前状态为:{}", group.currentState)
        // 只有消费组状态为 empty 或"稳定" 或 "等待同步" 时，才允许调用它；这个方法会先将组状态更新为 准备再平衡
        prepareRebalance(group)
      }
    }
  }

  /**
    * 在平衡的时候， 会将消费组的状态更改为： 准备在平衡
    * @param group
    */
  private def prepareRebalance(group: GroupMetadata) {
    // if any members are awaiting sync, cancel their request and have them rejoin
    if (group.is(CompletingRebalance))
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    val delayedRebalance = if (group.is(Empty)) {
      logger.info("第一个成员加入时，创建初始化延迟加入对象： new InitialDelayedJoin")
      new InitialDelayedJoin(this,
        joinPurgatory,
        group,
        groupConfig.groupInitialRebalanceDelayMs,
        groupConfig.groupInitialRebalanceDelayMs,
        max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
    } else {
      logger.info("创建延迟加入对象： new DelayedJoin")
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)
    }

    logger.info("group-state 从 [{}] 切换到 [PreparingRebalance]", group.currentState);
    //将消费组的状态 从 稳定状态 切换为： 准备在平衡
    group.transitionTo(PreparingRebalance)

    info(s"Preparing to rebalance group ${group.groupId} with old generation ${group.generationId} " +
      s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

    val groupKey = GroupKey(group.groupId)
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  private def removeMemberAndUpdateGroup(group: GroupMetadata, member: MemberMetadata) {
    group.remove(member.memberId)
    // FLAG FIXME: 如果移除操作后，组内成员为空, 是否需要将组的状态切换为：Empty
    group.currentState match {
      case Dead | Empty =>
      case Stable | CompletingRebalance => maybePrepareRebalance(group)
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group.inLock {
      // notYetRejoinedMembers 如果返回为空，
      if (group.notYetRejoinedMembers.isEmpty)
        forceComplete()
      else false
    }
  }

  def onExpireJoin() {
    // TODO: add metrics for restabilize timeouts
  }

  def onCompleteJoin(group: GroupMetadata) {
    logger.info("[begin] onCompleteJoin=============")
    logger.info("[加入组请求] group-state={}", group.currentState)
    group.inLock {
      // remove any members who haven't joined the group yet
      group.notYetRejoinedMembers.foreach { failedMember =>
        removeHeartbeatForLeavingMember(group, failedMember)
        group.remove(failedMember.memberId)
        // TODO: cut the socket connection to the client
      }

      if (!group.is(Dead)) {
        group.initNextGeneration()
        if (group.is(Empty)) {
          info(s"Group ${group.groupId} with generation ${group.generationId} is now empty " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          groupManager.storeGroup(group, Map.empty, error => {
            if (error != Errors.NONE) {
              // we failed to write the empty group metadata. If the broker fails before another rebalance,
              // the previous generation written to the log will become active again (and most likely timeout).
              // This should be safe since there are no active members in an empty generation, so we just warn.
              warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
            }
          })
        } else {
          info(s"Stabilized group ${group.groupId} generation ${group.generationId} " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          // trigger the awaiting join group response callback for all the members after rebalancing
          for (member <- group.allMemberMetadata) {
            assert(member.awaitingJoinCallback != null)
            val joinResult = JoinGroupResult(
              members = if (group.isLeader(member.memberId)) {
                group.currentMemberMetadata
              } else {
                Map.empty
              },
              memberId = member.memberId,
              generationId = group.generationId,
              subProtocol = group.protocolOrNull,
              leaderId = group.leaderOrNull,
              error = Errors.NONE)
            logger.info("响应成员 memberId={}, leaderId={}", member.memberId, joinResult.leaderId)
            member.awaitingJoinCallback(joinResult)
            member.awaitingJoinCallback = null
            completeAndScheduleNextHeartbeatExpiration(group, member)
          }
        }
      }
    }
    logger.info("[end] onCompleteJoin=============, group-state={}", group.currentState)
  }

  def tryCompleteHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group.inLock {
      if (shouldKeepMemberAlive(member, heartbeatDeadline) || member.isLeaving)
        forceComplete()
      else false
    }
  }

  def onExpireHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long) {
    group.inLock {
      if (!shouldKeepMemberAlive(member, heartbeatDeadline)) {
        info(s"Member ${member.memberId} in group ${group.groupId} has failed, removing it from the group")
        removeMemberAndUpdateGroup(group, member)
      }
    }
  }

  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  private def shouldKeepMemberAlive(member: MemberMetadata, heartbeatDeadline: Long) =
    member.awaitingJoinCallback != null ||
      member.awaitingSyncCallback != null ||
      member.latestHeartbeat + member.sessionTimeoutMs > heartbeatDeadline

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoGeneration = -1
  val NoMemberId = ""
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            time: Time): GroupCoordinator = {
    // 延迟心跳
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    //
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkClient, replicaManager, heartbeatPurgatory, joinPurgatory, time)
  }

  private[group] def offsetConfig(config: KafkaConfig) = OffsetConfig(
    maxMetadataSize = config.offsetMetadataMaxSize,
    loadBufferSize = config.offsetsLoadBufferSize,
    offsetsRetentionMs = config.offsetsRetentionMinutes * 60L * 1000L,
    offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
    offsetsTopicNumPartitions = config.offsetsTopicPartitions,
    offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
    offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
    offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
    offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
    offsetCommitRequiredAcks = config.offsetCommitRequiredAcks
  )

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time): GroupCoordinator = {
    val offsetConfig = this.offsetConfig(config)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs,
      groupInitialRebalanceDelayMs = config.groupInitialRebalanceDelay)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, config.interBrokerProtocolVersion,
      offsetConfig, replicaManager, zkClient, time)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time)
  }

}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int,
                       groupInitialRebalanceDelayMs: Int)

case class JoinGroupResult(members: Map[String, Array[Byte]],
                           memberId: String,
                           generationId: Int,
                           subProtocol: String,
                           leaderId: String,
                           error: Errors)
