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

package kafka.controller

import kafka.cluster.Broker
import org.apache.kafka.common.TopicPartition

import scala.collection.{Seq, Set, mutable}

class ControllerContext {
  val stats = new ControllerStats

  var controllerChannelManager: ControllerChannelManager = null

  var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty
  var epoch: Int = KafkaController.InitialControllerEpoch - 1
  var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion - 1
  // 这个控制器负责的所有 topic 列表
  var allTopics: Set[String] = Set.empty
  /**
    * key: 某个topic的某个分区 如：假设 topic-0
    * value： 就是这个分区所有副本所在的所有 broker 节点 如： [0,1,2]
    *
    * topic每个分区的所有副本 分配到的broker情况；
    * 如 3个分区 ： 0,1,2
    *   3个副本，
    *   4个broker： 0,1,2,3
    *
    * key = testTopic-0, value = [b0, b1, b2] 第一个分区0：第一个副本(leader)被分配到 b0代理节点上，第二个副本(follower) 被分配到 b1代理节点上，第三个副本(follower) 被分配到 b2代理节点上
    * key = testTopic-1, value = [b1, b2, b3]  ...
    * key = testTopic-2, value = [b2, b3, b0]  ...
    */
  var partitionReplicaAssignment: mutable.Map[TopicPartition, Seq[Int]] = mutable.Map.empty

  /**
    * key: 某个topic的某个分区 如：假设 topic-0
    * value: 这个分区所有副本的 leader以及 isr 分配情况。
    *
    * 如 3个分区 ： 0,1,2
    *   3个副本，
    *   4个broker： b0, b1, b2, b3
    *
    * key = testTopic-0, value = {leader:0, leaderEpoch:1, isr:[b0, b1, b2], zkVersion:1} 第一个分区中，leader是副本0，isr正常同步的副本对应的broker集合
    * key = testTopic-1, value = {leader:1, leaderEpoch:1, isr:[b1, b2, b3], zkVersion:1}
    * key = testTopic-2, value = {leader:2, leaderEpoch:1, isr:[b2, b3, b0], zkVersion:1}
    */
  var partitionLeadershipInfo: mutable.Map[TopicPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty
  /**
    * 分区被重新分配
    */
  val partitionsBeingReassigned: mutable.Map[TopicPartition, ReassignedPartitionsContext] = mutable.Map.empty

  val replicasOnOfflineDirs: mutable.Map[Int, Set[TopicPartition]] = mutable.Map.empty

  private var liveBrokersUnderlying: Set[Broker] = Set.empty
  private var liveBrokerIdsUnderlying: Set[Int] = Set.empty

  // setter
  def liveBrokers_=(brokers: Set[Broker]) {
    liveBrokersUnderlying = brokers
    liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
  }

  // getter
  def liveBrokers = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))
  def liveBrokerIds = liveBrokerIdsUnderlying -- shuttingDownBrokerIds

  def liveOrShuttingDownBrokerIds = liveBrokerIdsUnderlying
  def liveOrShuttingDownBrokers = liveBrokersUnderlying

  /**
    * --flag-- 代理节点上的 全部分区
    * 如： b0 -> [testTopic-p0, testTopic-p1]
    *
    * @param brokerId
    * @return
    */
  def partitionsOnBroker(brokerId: Int): Set[TopicPartition] = {
    partitionReplicaAssignment.collect {
      case (topicPartition, replicas) if replicas.contains(brokerId) => topicPartition
    }.toSet
  }

  def isReplicaOnline(brokerId: Int, topicPartition: TopicPartition, includeShuttingDownBrokers: Boolean = false): Boolean = {
    val brokerOnline = {
      if (includeShuttingDownBrokers) liveOrShuttingDownBrokerIds.contains(brokerId)
      else liveBrokerIds.contains(brokerId)
    }
    brokerOnline && !replicasOnOfflineDirs.getOrElse(brokerId, Set.empty).contains(topicPartition)
  }

  /**
    * --flag-- 代理节点列表 的 所有副本
    * 如 b0
    *   [
    *     testTopic-0-0, testTopic-2-0
    *   ]
    * @param brokerIds
    * @return
    */
  def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
    brokerIds.flatMap { brokerId =>
      partitionReplicaAssignment.collect { case (topicPartition, replicas) if replicas.contains(brokerId) =>
        PartitionAndReplica(topicPartition, brokerId)
      }
    }.toSet
  }

  /**
    * --flag-- topic的所有副本
    *
    * 如：
    *  三个分区、三个副本
    * testTopic ->
    * [
    *   testTopic-0-0, testTopic-0-1, testTopic-0-2,
    *   testTopic-1-0, testTopic-1-1, testTopic-1-2,
    *   testTopic-2-0, testTopic-2-1, testTopic-2-2
    * ]
    *
    * @param topic
    * @return
    */
  def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
    partitionReplicaAssignment
      .filter { case (topicPartition, _) => topicPartition.topic == topic }
      .flatMap { case (topicPartition, replicas) =>
        replicas.map(PartitionAndReplica(topicPartition, _))
      }.toSet
  }

  /**
    * --flag-- topic的所有分区
    * 如：
    *  三个分区
    * testTopic ->
    * [
    *   testTopic-0,
    *   testTopic-1,
    *   testTopic-2
    * ]
    *
    * @param topic
    * @return
    */
  def partitionsForTopic(topic: String): collection.Set[TopicPartition] =
    partitionReplicaAssignment.keySet.filter(topicPartition => topicPartition.topic == topic)

  /**
    * --flag-- 集群中所有存活的副本
    * 如：[
    *     testTopic-0-0, testTopic-0-1, testTopic-0-2,
    *     testTopic-1-0, testTopic-1-1, testTopic-1-2,
    *     testTopic-2-0, testTopic-2-1, testTopic-2-2,
    *
    *     testTopicNew-0-0, testTopicNew-0-1, testTopicNew-0-2,
    *     testTopicNew-1-0, testTopicNew-1-1, testTopicNew-1-2,
    *     testTopicNew-2-0, testTopicNew-2-1, testTopicNew-2-2,
    *   ]
    *
    * @return
    */
  def allLiveReplicas(): Set[PartitionAndReplica] = {
    replicasOnBrokers(liveBrokerIds).filter { partitionAndReplica =>
      isReplicaOnline(partitionAndReplica.replica, partitionAndReplica.topicPartition)
    }
  }

  /**
    * --flag-- 分区列表的所有副本
    * 如：
    *   testTopic-0 ->
    *   [
    *     testTopic-0-0, testTopic-0-1, testTopic-0-2
    *   ]
    * @param partitions
    * @return
    */
  def replicasForPartition(partitions: collection.Set[TopicPartition]): collection.Set[PartitionAndReplica] = {
    partitions.flatMap { p =>
      val replicas = partitionReplicaAssignment(p)
      replicas.map(PartitionAndReplica(p, _))
    }
  }

  /**
    * --flag-- 删除一个 topic
    * @param topic
    */
  def removeTopic(topic: String) = {
    partitionLeadershipInfo = partitionLeadershipInfo.filter { case (topicPartition, _) => topicPartition.topic != topic }
    partitionReplicaAssignment = partitionReplicaAssignment.filter { case (topicPartition, _) => topicPartition.topic != topic }
    allTopics -= topic
  }

}
