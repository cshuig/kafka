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

package kafka.controller

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock

import kafka.metrics.KafkaTimer
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread

import scala.collection._

object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
}

/**
  * 这个其实就是一个 生产者 与 消费者 的中间 broker
  *
  * @param controllerId
  * @param rateAndTimeMetrics
  * @param eventProcessedListener
  */
class ControllerEventManager(controllerId: Int, rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
                             eventProcessedListener: ControllerEvent => Unit) {

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  /**
    *--flag-- 存放的各种各样的控制器事件， 线程会实时监听这个阻塞队列，一旦有事件进来，就消费并执行相应事件
    */
  private val queue = new LinkedBlockingQueue[ControllerEvent]
  //--flag-- 控制器事件处理线程
  private val thread = new ControllerEventThread(ControllerEventManager.ControllerEventThreadName)

  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    clearAndPut(KafkaController.ShutdownEventThread)
    thread.awaitShutdown()
  }

  def put(event: ControllerEvent): Unit = inLock(putLock) {
    queue.put(event)
  }

  def clearAndPut(event: ControllerEvent): Unit = inLock(putLock) {
    queue.clear()
    queue.put(event)
  }

  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    override def doWork(): Unit = {
      queue.take() match {
        case KafkaController.ShutdownEventThread => initiateShutdown()
        case controllerEvent =>
          _state = controllerEvent.state

          try {
            //速率和时间指标
            rateAndTimeMetrics(state).time {
              controllerEvent.process()
            }
          } catch {
            case e: Throwable => error(s"Error processing event $controllerEvent", e)
          }

          try eventProcessedListener(controllerEvent)
          catch {
            case e: Throwable => error(s"Error while invoking listener for processed event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

}
