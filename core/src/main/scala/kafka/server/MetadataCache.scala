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

package kafka.server

import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.api._
import kafka.cluster.{Broker, EndPoint}
import kafka.common.TopicAndPartition
import kafka.controller.StateChangeLogger
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{MetadataResponse, UpdateMetadataRequest}
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.{Seq, Set, mutable}

/**
 *  A cache for the state (e.g., current leader) of each partition. This cache is updated through
 *  UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 */
class MetadataCache(brokerId: Int) extends Logging {

  /**
    * 存储了 topic 对应的 分区-分区状态实例
    * key： topic名字
    * value：
    * >     key: 分区号
    * >     value: 分区状态实例对象
    */
  private val cache = mutable.Map[String, mutable.Map[Int, UpdateMetadataRequest.PartitionState]]()
  @volatile private var controllerId: Option[Int] = None
  private val aliveBrokers = mutable.Map[Int, Broker]()
  private val aliveNodes = mutable.Map[Int, collection.Map[ListenerName, Node]]()
  private val partitionMetadataLock = new ReentrantReadWriteLock()

  this.logIdent = s"[MetadataCache brokerId=$brokerId] "
  private val stateChangeLogger = new StateChangeLogger(brokerId, inControllerContext = false, None)

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def getEndpoints(brokers: Iterable[Int], listenerName: ListenerName, filterUnavailableEndpoints: Boolean): Seq[Node] = {
    val result = new mutable.ArrayBuffer[Node](math.min(aliveBrokers.size, brokers.size))
    brokers.foreach { brokerId =>
      val endpoint = getAliveEndpoint(brokerId, listenerName) match {
        case None => if (!filterUnavailableEndpoints) Some(new Node(brokerId, "", -1)) else None
        case Some(node) => Some(node)
      }
      endpoint.foreach(result +=)
    }
    result
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  /**
    * 得到 指定 topic 的所有分区元数据集合
    * @param topic
    * @param listenerName
    * @param errorUnavailableEndpoints
    * @return
    */
  private def getPartitionMetadata(topic: String, listenerName: ListenerName, errorUnavailableEndpoints: Boolean): Option[Iterable[MetadataResponse.PartitionMetadata]] = {
    // 从元数据缓存中的到指定 topic的 分区-分区状态实例 map 对象
    cache.get(topic).map { partitions =>
      partitions.map { case (partitionId, partitionState) =>
        val topicPartition = TopicAndPartition(topic, partitionId)
        // 获取指定 Leader brokerId 所在的节点信息(如： host、port)
        val maybeLeaderNode = getAliveEndpoint(partitionState.basePartitionState.leader, listenerName)
        // 得到所有副本所在的 brokerId 集合
        val replicasIntList = partitionState.basePartitionState.replicas.asScala.map(_.toInt)
        // 所有副本所在的 brokerId 集合 转换为 所有副本的 Node 实例集合， 转换过程有可能出现节点获取不到
        val replicaInfoNodeList = getEndpoints(replicasIntList, listenerName, errorUnavailableEndpoints)
        // 在线的 副本 的所有 Node 实例集合
        val offlineReplicaInfoNodeList = getEndpoints(partitionState.offlineReplicas.asScala.map(_.toInt), listenerName, errorUnavailableEndpoints)

        maybeLeaderNode match {
          case None =>
            debug(s"Error while fetching metadata for $topicPartition: leader not available")
            // 通过 Leader brokerId 获取不到 Leader Node，响应：LEADER_NOT_AVAILABLE 错误码
            new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE, partitionId, Node.noNode(),
              replicaInfoNodeList.asJava, java.util.Collections.emptyList(), offlineReplicaInfoNodeList.asJava)

          case Some(leader) =>
            // 得到所有的同步副本brokerId集合
            val isrIntList = partitionState.basePartitionState.isr.asScala.map(_.toInt)
            // 所有的同步副本brokerId集合 转换为 Node 集合
            val isrInfoNodeList = getEndpoints(isrIntList, listenerName, errorUnavailableEndpoints)
            // 如果 所有副本的 Node 实例集合个数 少于 所有副本所在的 brokerId 个数
            if (replicaInfoNodeList.size < replicasIntList.size) {
              debug(s"Error while fetching metadata for $topicPartition: replica information not available for " +
                s"following brokers ${replicasIntList.filterNot(replicaInfoNodeList.map(_.id).contains).mkString(",")}")
              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                replicaInfoNodeList.asJava, isrInfoNodeList.asJava, offlineReplicaInfoNodeList.asJava)
            } else if (isrInfoNodeList.size < isrIntList.size) {
              // ISR Node实例个数 少于 ISR id个数
              debug(s"Error while fetching metadata for $topicPartition: in sync replica information not available for " +
                s"following brokers ${isrIntList.filterNot(isrInfoNodeList.map(_.id).contains).mkString(",")}")
              new MetadataResponse.PartitionMetadata(Errors.REPLICA_NOT_AVAILABLE, partitionId, leader,
                replicaInfoNodeList.asJava, isrInfoNodeList.asJava, offlineReplicaInfoNodeList.asJava)
            } else {
              // 返回正常的 分区元数据实例对象，对象内部包含了： Leader Node实例、所有副本Node实例集合、ISR 副本Node实例集合、在线副本NOde集合
              new MetadataResponse.PartitionMetadata(Errors.NONE, partitionId, leader, replicaInfoNodeList.asJava,
                isrInfoNodeList.asJava, offlineReplicaInfoNodeList.asJava)
            }
        }
      }
    }
  }

  /**
    * 获取指定 brokerId 所在的节点信息(如： host、port)
    *
    * @param brokerId
    * @param listenerName
    * @return
    */
  private def getAliveEndpoint(brokerId: Int, listenerName: ListenerName): Option[Node] =
    inReadLock(partitionMetadataLock) {
      // Returns None if broker is not alive or if the broker does not have a listener named `listenerName`.
      // Since listeners can be added dynamically, a broker with a missing listener could be a transient error.
      aliveNodes.get(brokerId).flatMap(_.get(listenerName))
    }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  def getTopicMetadata(topics: Set[String], listenerName: ListenerName, errorUnavailableEndpoints: Boolean = false): Seq[MetadataResponse.TopicMetadata] = {
    inReadLock(partitionMetadataLock) {
      topics.toSeq.flatMap { topic =>
        // 先通过 topic 的到它所有的分区元数据对象列表： Iterable[MetadataResponse.PartitionMetadata]
        getPartitionMetadata(topic, listenerName, errorUnavailableEndpoints)
          // 通过map将 Iterable[MetadataResponse.PartitionMetadata] 转换为 List<PartitionMetadata>
          .map { partitionMetadata =>
          new MetadataResponse.TopicMetadata(Errors.NONE, topic, Topic.isInternal(topic), partitionMetadata.toBuffer.asJava)
        }
      }
    }
  }

  def getAllTopics(): Set[String] = {
    inReadLock(partitionMetadataLock) {
      cache.keySet.toSet
    }
  }

  def getNonExistingTopics(topics: Set[String]): Set[String] = {
    inReadLock(partitionMetadataLock) {
      topics -- cache.keySet
    }
  }

  def isBrokerAlive(brokerId: Int): Boolean = {
    inReadLock(partitionMetadataLock) {
      aliveBrokers.contains(brokerId)
    }
  }

  def getAliveBrokers: Seq[Broker] = {
    inReadLock(partitionMetadataLock) {
      aliveBrokers.values.toBuffer
    }
  }

  private def addOrUpdatePartitionInfo(topic: String,
                                       partitionId: Int,
                                       stateInfo: UpdateMetadataRequest.PartitionState) {
    inWriteLock(partitionMetadataLock) {
      val infos = cache.getOrElseUpdate(topic, mutable.Map())
      infos(partitionId) = stateInfo
    }
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[UpdateMetadataRequest.PartitionState] = {
    inReadLock(partitionMetadataLock) {
      cache.get(topic).flatMap(_.get(partitionId))
    }
  }

  // if the leader is not known, return None;
  // if the leader is known and corresponding node is available, return Some(node)
  // if the leader is known but corresponding node with the listener name is not available, return Some(NO_NODE)
  def getPartitionLeaderEndpoint(topic: String, partitionId: Int, listenerName: ListenerName): Option[Node] = {
    inReadLock(partitionMetadataLock) {
      cache.get(topic).flatMap(_.get(partitionId)) map { partitionInfo =>
        val leaderId = partitionInfo.basePartitionState.leader

        aliveNodes.get(leaderId) match {
          case Some(nodeMap) =>
            nodeMap.getOrElse(listenerName, Node.noNode)
          case None =>
            Node.noNode
        }
      }
    }
  }

  def getControllerId: Option[Int] = controllerId

  // This method returns the deleted TopicPartitions received from UpdateMetadataRequest
  def updateCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest): Seq[TopicPartition] = {
    inWriteLock(partitionMetadataLock) {
      controllerId = updateMetadataRequest.controllerId match {
          case id if id < 0 => None
          case id => Some(id)
        }
      aliveNodes.clear()
      aliveBrokers.clear()
      updateMetadataRequest.liveBrokers.asScala.foreach { broker =>
        // `aliveNodes` is a hot path for metadata requests for large clusters, so we use java.util.HashMap which
        // is a bit faster than scala.collection.mutable.HashMap. When we drop support for Scala 2.10, we could
        // move to `AnyRefMap`, which has comparable performance.
        val nodes = new java.util.HashMap[ListenerName, Node]
        val endPoints = new mutable.ArrayBuffer[EndPoint]
        broker.endPoints.asScala.foreach { ep =>
          endPoints += EndPoint(ep.host, ep.port, ep.listenerName, ep.securityProtocol)
          nodes.put(ep.listenerName, new Node(broker.id, ep.host, ep.port))
        }
        aliveBrokers(broker.id) = Broker(broker.id, endPoints, Option(broker.rack))
        aliveNodes(broker.id) = nodes.asScala
      }
      aliveNodes.get(brokerId).foreach { listenerMap =>
        val listeners = listenerMap.keySet
        if (!aliveNodes.values.forall(_.keySet == listeners))
          error(s"Listeners are not identical across brokers: $aliveNodes")
      }

      val deletedPartitions = new mutable.ArrayBuffer[TopicPartition]
      updateMetadataRequest.partitionStates.asScala.foreach { case (tp, info) =>
        val controllerId = updateMetadataRequest.controllerId
        val controllerEpoch = updateMetadataRequest.controllerEpoch
        if (info.basePartitionState.leader == LeaderAndIsr.LeaderDuringDelete) {
          removePartitionInfo(tp.topic, tp.partition)
          stateChangeLogger.trace(s"Deleted partition $tp from metadata cache in response to UpdateMetadata " +
            s"request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
          deletedPartitions += tp
        } else {
          addOrUpdatePartitionInfo(tp.topic, tp.partition, info)
          stateChangeLogger.trace(s"Cached leader info $info for partition $tp in response to " +
            s"UpdateMetadata request sent by controller $controllerId epoch $controllerEpoch with correlation id $correlationId")
        }
      }
      deletedPartitions
    }
  }

  def contains(topic: String): Boolean = {
    inReadLock(partitionMetadataLock) {
      cache.contains(topic)
    }
  }

  def contains(tp: TopicPartition): Boolean = getPartitionInfo(tp.topic, tp.partition).isDefined

  private def removePartitionInfo(topic: String, partitionId: Int): Boolean = {
    cache.get(topic).exists { infos =>
      infos.remove(partitionId)
      if (infos.isEmpty) cache.remove(topic)
      true
    }
  }

}
