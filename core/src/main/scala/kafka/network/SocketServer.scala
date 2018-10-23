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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels.{Selector => NSelector, _}
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.network.{ChannelBuilder, ChannelBuilders, KafkaChannel, ListenerName, Selectable, Send, Selector => KSelector}
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}
import org.slf4j.event.Level

import scala.collection.JavaConverters._
import scala.collection._
import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.util.control.ControlThrowable

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time, val credentialProvider: CredentialProvider) extends Logging with KafkaMetricsGroup {

  private val maxQueuedRequests = config.queuedMaxRequests

  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  private val logContext = new LogContext(s"[SocketServer brokerId=${config.brokerId}] ")
  this.logIdent = logContext.logPrefix

  private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
  private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", "socket-server-metrics")
  private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", "socket-server-metrics")
  memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))
  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE

  //--flag-- 服务端的请求通道，保护一个全局的请求队列(requestQueue) 和 多个 Processor处理器对应的响应队列()
  val requestChannel = new RequestChannel(maxQueuedRequests)
  //--flag-- Processor线程数组转 Map, key：pid(0开始)， value： Processor线程
  private val processors = new ConcurrentHashMap[Int, Processor]()
  private var nextProcessorId = 0

  private[network] val acceptors = new ConcurrentHashMap[EndPoint, Acceptor]()
  private var connectionQuotas: ConnectionQuotas = _
  private var stoppedProcessingRequests = false

  /**
   * Start the socket server. Acceptors for all the listeners are started. Processors
   * are started if `startupProcessors` is true. If not, processors are only started when
   * [[kafka.network.SocketServer#startProcessors()]] is invoked. Delayed starting of processors
   * is used to delay processing client connections until server is fully initialized, e.g.
   * to ensure that all credentials have been loaded before authentications are performed.
   * Acceptors are always started during `startup` so that the bound port is known when this
   * method completes even when ephemeral ports are used. Incoming connections on this server
   * are processed when processors start up and invoke [[org.apache.kafka.common.network.Selector#poll]].
   *
   * @param startupProcessors Flag indicating whether `Processor`s must be started.
   */
  def startup(startupProcessors: Boolean = true) {
    this.synchronized {
      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)
      //--flag-- 创建 1个Acceptor线程 和 N个 Processor 线程
      createAcceptorAndProcessors(config.numNetworkThreads, config.listeners)
      if (startupProcessors) {
        startProcessors()
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {

        def value = SocketServer.this.synchronized {
          val ioWaitRatioMetricNames = processors.values.asScala.map { p =>
            metrics.metricName("io-wait-ratio", "socket-server-metrics", p.metricTags)
          }
          ioWaitRatioMetricNames.map { metricName =>
            Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
          }.sum / processors.size
        }
      }
    )
    newGauge("MemoryPoolAvailable",
      new Gauge[Long] {
        def value = memoryPool.availableMemory()
      }
    )
    newGauge("MemoryPoolUsed",
      new Gauge[Long] {
        def value = memoryPool.size() - memoryPool.availableMemory()
      }
    )
    info("Started " + acceptors.size + " acceptor threads")
  }

  /**
   * Starts processors of all the acceptors of this server if they have not already been started.
   * This method is used for delayed starting of processors if [[kafka.network.SocketServer#startup]]
   * was invoked with `startupProcessors=false`.
   */
  def startProcessors(): Unit = synchronized {
    acceptors.values.asScala.foreach { _.startProcessors() }
    info(s"Started processors for ${acceptors.size} acceptors")
  }

  private def endpoints = config.listeners.map(l => l.listenerName -> l).toMap

  /**
    * --flag-- 创建 Acceptor 和 Processor 线程
    *
    *   Acceptor 持有 多个 Processor 线程， 用来将新连接分发给 Processor 线程
    * @param processorsPerListener
    * @param endpoints
    */
  private def createAcceptorAndProcessors(processorsPerListener: Int,
                                          endpoints: Seq[EndPoint]): Unit = synchronized {

    val sendBufferSize = config.socketSendBufferBytes
    val recvBufferSize = config.socketReceiveBufferBytes
    val brokerId = config.brokerId

    endpoints.foreach { endpoint =>
      val listenerName = endpoint.listenerName
      val securityProtocol = endpoint.securityProtocol

      //--flag-- 创建 1个 Acceptor 线程
      val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId, connectionQuotas)
      addProcessors(acceptor, endpoint, processorsPerListener)
      //--flag-- Acceptor线程名字： kafka-socket-acceptor-开头， 负责监听来着客户端的新连接，只做新连接监听工作，很单一，很轻量级。没任何压力
      KafkaThread.nonDaemon(s"kafka-socket-acceptor-$listenerName-$securityProtocol-${endpoint.port}", acceptor).start()
      //--flag-- 需要等待 Acceptor 线程成功启动
      acceptor.awaitStartup()
      acceptors.put(endpoint, acceptor)
    }
  }

  /**
    * --flag-- 用来创建 Processor 线程
    * @param acceptor
    * @param endpoint
    * @param newProcessorsPerListener
    */
  private def addProcessors(acceptor: Acceptor, endpoint: EndPoint, newProcessorsPerListener: Int): Unit = synchronized {
    val listenerName = endpoint.listenerName
    val securityProtocol = endpoint.securityProtocol
    //--flag-- Processor线程是用数组结构保存，轮询方便
    val listenerProcessors = new ArrayBuffer[Processor]()

    //--flag-- 根据 num.network.threads 参数创建 N 个 Processor 线程
    for (_ <- 0 until newProcessorsPerListener) {
      //--flag-- id从0开始
      val processor = newProcessor(nextProcessorId, connectionQuotas, listenerName, securityProtocol, memoryPool)
      //--flag-- 加入到数组中
      listenerProcessors += processor
      requestChannel.addProcessor(processor)
      nextProcessorId += 1
    }
    listenerProcessors.foreach(p => processors.put(p.id, p))
    acceptor.addProcessors(listenerProcessors)
  }

  /**
    * Stop processing requests and new connections.
    * --flag--
    * 停止处理请求和新连接
    * 当关闭 Broker 时，会通知关闭
    */
  def stopProcessingRequests() = {
    info("Stopping socket server request processors")
    this.synchronized {
      acceptors.asScala.values.foreach(_.shutdown())
      processors.asScala.values.foreach(_.shutdown())
      requestChannel.clear()
      stoppedProcessingRequests = true
    }
    info("Stopped socket server request processors")
  }

  /**
    * --flag--
    *   调整 Processor 线程数量， 由 DynamicBrokerConfig 触发
    *   逻辑：
    *     1、当 新的线程数量 > 旧的线程数量 时： 新增 newNumNetworkThreads - oldNumNetworkThreads 个 Processor 线程数
    *     2、当 新的线程数量 < 旧的线程数量 时： 删除 newNumNetworkThreads - oldNumNetworkThreads 个 Processor 线程数
    * @param oldNumNetworkThreads 旧的线程数量
    * @param newNumNetworkThreads 新的线程数量
    */
  def resizeThreadPool(oldNumNetworkThreads: Int, newNumNetworkThreads: Int): Unit = synchronized {
    info(s"Resizing network thread pool size for each listener from $oldNumNetworkThreads to $newNumNetworkThreads")
    if (newNumNetworkThreads > oldNumNetworkThreads) {
      acceptors.asScala.foreach { case (endpoint, acceptor) =>
        addProcessors(acceptor, endpoint, newNumNetworkThreads - oldNumNetworkThreads)
      }
    } else if (newNumNetworkThreads < oldNumNetworkThreads)
      acceptors.asScala.values.foreach(_.removeProcessors(oldNumNetworkThreads - newNumNetworkThreads, requestChannel))
  }

  /**
    * Shutdown the socket server. If still processing requests, shutdown
    * acceptors and processors first.
    */
  def shutdown() = {
    info("Shutting down socket server")
    this.synchronized {
      if (!stoppedProcessingRequests)
        stopProcessingRequests()
      requestChannel.shutdown()
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
      acceptors.get(endpoints(listenerName)).serverChannel.socket.getLocalPort
    } catch {
      case e: Exception =>
        throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  def addListeners(listenersAdded: Seq[EndPoint]): Unit = synchronized {
    info(s"Adding listeners for endpoints $listenersAdded")
    createAcceptorAndProcessors(config.numNetworkThreads, listenersAdded)
    startProcessors()
  }

  def removeListeners(listenersRemoved: Seq[EndPoint]): Unit = synchronized {
    info(s"Removing listeners for endpoints $listenersRemoved")
    listenersRemoved.foreach { endpoint =>
      acceptors.asScala.remove(endpoint).foreach(_.shutdown())
    }
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                      securityProtocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      listenerName,
      securityProtocol,
      config,
      metrics,
      credentialProvider,
      memoryPool,
      logContext
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors.get(index)

}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  @volatile private var shutdownLatch = new CountDownLatch(0)

  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    if (alive.getAndSet(false))
      wakeup()
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning: Boolean = alive.get

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel): Unit = {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      CoreUtils.swallow(channel.socket().close(), this, Level.ERROR)
      CoreUtils.swallow(channel.close(), this, Level.ERROR)
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
  *
  * --flag-- 用来 接收和配置新连接， 每个 broker 只有一个
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  //--flag-- 使用 NIO Selector 方式
  private val nioSelector = NSelector.open()
  //--flag-- 用来监听端口的 服务端 Socket 套接字对象
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)
  private val processors = new ArrayBuffer[Processor]()
  private val processorsStarted = new AtomicBoolean

  private[network] def addProcessors(newProcessors: Buffer[Processor]): Unit = synchronized {
    processors ++= newProcessors
    if (processorsStarted.get)
      startProcessors(newProcessors)
  }

  private[network] def startProcessors(): Unit = synchronized {
    if (!processorsStarted.getAndSet(true)) {
      startProcessors(processors)
    }
  }

  /**
    * --flag--: Processor线程名字为：kafka-network-thread-开头
    *   负责对 Socket 进行读写
    * @param processors
    */
  private def startProcessors(processors: Seq[Processor]): Unit = synchronized {
    processors.foreach { processor =>
      KafkaThread.nonDaemon(s"kafka-network-thread-$brokerId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
        processor).start()
    }
  }

  private[network] def removeProcessors(removeCount: Int, requestChannel: RequestChannel): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    val toRemove = processors.takeRight(removeCount)
    processors.remove(processors.size - removeCount, removeCount)
    toRemove.foreach(_.shutdown())
    toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
  }

  override def shutdown(): Unit = {
    super.shutdown()
    synchronized {
      processors.foreach(_.shutdown())
    }
  }

  /**
   * Accept loop that checks for new connection attempts
    * --flag-- Acceptor 线程循环检测新连接事件
   */
  def run() {
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    startupComplete()
    try {
      var currentProcessor = 0
      while (isRunning) {
        try {
          //--flag-- 给选择器指定阻塞超时等待时间
          val ready = nioSelector.select(500)
          if (ready > 0) {
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                // 移除已选的key，以防重复处理
                iter.remove()
                if (key.isAcceptable) { // 判断此key是否是 新连接事件
                  val processor = synchronized {
                    // 采用轮询的方式选择一个 Processor 线程
                    currentProcessor = currentProcessor % processors.size
                    processors(currentProcessor)
                  }
                  // 获取客户端新连接，并将其分配给指定的 Processor 线程
                  accept(key, processor)
                } else
                  // acceptor 线程 未识别的状态 key
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread, mod(numProcessors) will be done later
                //--flag-- Processor 线程轮询计数加1
                currentProcessor = currentProcessor + 1
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      CoreUtils.swallow(serverChannel.close(), this, Level.ERROR)
      CoreUtils.swallow(nioSelector.close(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  /*
   * Create a server socket to listen for connections on.
   *
   *
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    //--flag-- 将通道设置为非阻塞模式，才可以将它注册到 选择器中
    serverChannel.configureBlocking(false)
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      //--flag-- 将该通道的 ServerSocket 绑定到
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   * --flag-- 获取到新连接
   */
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    // 得到与客户端连接(长连接)的套接字通道对象
    val socketChannel = serverSocketChannel.accept()
    try {
      // 统计并检测客户端最大连接数
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))
      //--flag-- 将客户端新的连接通道分配给 Processor 处理
      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

private[kafka] object Processor {
  val IdlePercentMetricName = "IdlePercent"
  val NetworkProcessorMetricTag = "networkProcessor"
  val ListenerMetricTag = "listener"
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               listenerName: ListenerName,
                               securityProtocol: SecurityProtocol,
                               config: KafkaConfig,
                               metrics: Metrics,
                               credentialProvider: CredentialProvider,
                               memoryPool: MemoryPool,
                               logContext: LogContext) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  import Processor._
  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote, index) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort, Integer.parseInt(index))
        }
      }
      case _ => None
    }
  }

  private[network] case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int, index: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort-$index"
  }
  //--flag-- Acceptor线程会将新连接写入这个并发队列，接着通知 Processor 线程处理
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()

  //--flag-- 响应队列
  private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()

  private[kafka] val metricTags = mutable.LinkedHashMap(
    ListenerMetricTag -> listenerName.value,
    NetworkProcessorMetricTag -> id.toString
  ).asJava

  newGauge(IdlePercentMetricName,
    new Gauge[Double] {
      def value = {
        Option(metrics.metric(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags)))
          .fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }
    },
    // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
    // also includes the listener name)
    Map(NetworkProcessorMetricTag -> id.toString)
  )

  //--flag-- Processor线程的选择器，用来管理当前线程从Acceptor分配到的客户端套接字通道： SocketChannel
  private val selector = createSelector(
    ChannelBuilders.serverChannelBuilder(listenerName,
      listenerName == config.interBrokerListenerName,
      securityProtocol,
      config,
      credentialProvider.credentialCache,
      credentialProvider.tokenCache))
  // Visible to override for testing
  protected[network] def createSelector(channelBuilder: ChannelBuilder): KSelector = {
    channelBuilder match {
      case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
      case _ =>
    }
    new KSelector(
      maxRequestSize,
      connectionsMaxIdleMs,
      metrics,
      time,
      "socket-server",
      metricTags,
      false,
      true,
      channelBuilder,
      memoryPool,
      logContext)
  }

  // Connection ids have the format `localAddr:localPort-remoteAddr:remotePort-index`. The index is a
  // non-negative incrementing value that ensures that even if remotePort is reused after a connection is
  // closed, connection ids are not reused while requests from the closed connection are being processed.
  private var nextConnectionIndex = 0

  override def run() {
    startupComplete()
    try {
      while (isRunning) {
        try {
          // setup any new connections that have been queued up
          //--flag-- 1、获取新连接，并将其注册到选择器上，注册可以读取事件： OP_READ
          configureNewConnections()
          // register any new responses for writing
          /**
            * --flag-- 2、处理响应队列, 为 KafkaChannel 对象设置 Send 发送对象 以及 为 SocketChannel 添加 OP_WRITE 事件，
            * 接下来的 poll 就能检测到 这个事件并触发，就能将响应数据写入输出通道到客户端
            */
          processNewResponses()

          /**
            * --flag-- 3、从 Selector 中获取 OP_XXXX 事件的数据，并将完成读取的数据放入 Selector#completedReceives中； 读取超时时间为： 300ms
            */
          poll()
          // 处理将上一步poll到的数据，构造出请求上下文、请求对象并放入请求通道的请求队列中
          processCompletedReceives()
          // 处理 poll() 中触发的 OP_WRITE 事件中成功将响应队列中的数据输出到客户端的 Send 对象
          processCompletedSends()
          // 处理上面整个过程中出现的 有问题的被关闭的 通道连接。
          processDisconnected()
        } catch {
          // We catch all the throwables here to prevent the processor thread from exiting. We do this because
          // letting a processor exit might cause a bigger impact on the broker. This behavior might need to be
          // reviewed if we see an exception that needs the entire broker to stop. Usually the exceptions thrown would
          // be either associated with a specific socket channel or a bad request. These exceptions are caught and
          // processed by the individual methods above which close the failing channel and continue processing other
          // channels. So this catch block should only ever see ControlThrowables.
          case e: Throwable => processException("Processor got uncaught exception.", e)
        }
      }
    } finally {
      debug("Closing selector - processor " + id)
      CoreUtils.swallow(closeAll(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  private def processException(errorMessage: String, throwable: Throwable) {
    throwable match {
      case e: ControlThrowable => throw e
      case e => error(errorMessage, e)
    }
  }

  private def processChannelException(channelId: String, errorMessage: String, throwable: Throwable) {
    if (openOrClosingChannel(channelId).isDefined) {
      error(s"Closing socket for $channelId because of error", throwable)
      close(channelId)
    }
    processException(errorMessage, throwable)
  }

  private def processNewResponses() {
    var curr: RequestChannel.Response = null
    // 循环从响应队列中获取响应对象
    while ({curr = dequeueResponse(); curr != null}) {
      val channelId = curr.request.context.connectionId
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            updateRequestMetrics(curr)
            trace("Socket server received empty response to send, registering for read: " + curr)
            openOrClosingChannel(channelId).foreach(c => selector.unmute(c.id))
          case RequestChannel.SendAction =>
            //--flag-- 获取到 Send 对象
            val responseSend = curr.responseSend.getOrElse(
              throw new IllegalStateException(s"responseSend must be defined for SendAction, response: $curr"))
            //--flag-- 处理发送响应
            sendResponse(curr, responseSend)
          case RequestChannel.CloseConnectionAction =>
            updateRequestMetrics(curr)
            trace("Closing socket connection actively according to the response code.")
            close(channelId)
        }
      } catch {
        case e: Throwable =>
          processChannelException(channelId, s"Exception while processing response for $channelId", e)
      }
    }
  }

  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send) {
    val connectionId = response.request.context.connectionId
    trace(s"Socket server received response to send to $connectionId, registering for write and sending data: $response")
    // `channel` can be None if the connection was closed remotely or if selector closed it for being idle for too long
    if (channel(connectionId).isEmpty) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $connectionId")
      response.request.updateRequestMetrics(0L, response)
    }
    // Invoke send for closingChannel as well so that the send is failed and the channel closed properly and
    // removed from the Selector after discarding any pending staged receives.
    // `openOrClosingChannel` can be None if the selector closed the connection because it was idle for too long
    if (openOrClosingChannel(connectionId).isDefined) {
      selector.send(responseSend)
      //--flag--一个Channel写的数据必须在发送后先记录在inflightResponse中, 用于验证response的正确性
      inflightResponses += (connectionId -> response)
    }
  }

  private def poll() {
    try selector.poll(300)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        // The exception is not re-thrown and any completed sends/receives/connections/disconnections
        // from this poll will be processed.
        error(s"Processor $id poll failed due to illegal state or IO exception")
    }
  }

  /**
    * --flag-- 主要作用是，将从通道中读取到的数据解析转换为 请求上下文对象、请求对象，最后写入 请求队列，由业务处理线程消费处理。
    */
  private def processCompletedReceives() {
    selector.completedReceives.asScala.foreach { receive =>
      try {
        openOrClosingChannel(receive.source) match {
          case Some(channel) =>
            val header = RequestHeader.parse(receive.payload) // payload： 就是客户端请求的完整数据
            //--flag-- 构建请求上下文对象
            val context = new RequestContext(header, receive.source, channel.socketAddress,
              channel.principal, listenerName, securityProtocol)
            //--flag-- 构造一个请求对象
            val req = new RequestChannel.Request(processor = id, context = context,
              startTimeNanos = time.nanoseconds, memoryPool, receive.payload, requestChannel.metrics)
            //--flag-- 将请求写入请求队列， 后续由业务处理线程池去消费处理
            requestChannel.sendRequest(req)
            selector.mute(receive.source)
          case None =>
            // This should never happen since completed receives are processed immediately after `poll()`
            throw new IllegalStateException(s"Channel ${receive.source} removed from selector before processing completed receive")
        }
      } catch {
        // note that even though we got an exception, we can assume that receive.source is valid.
        // Issues with constructing a valid receive object were handled earlier
        case e: Throwable =>
          processChannelException(receive.source, s"Exception while processing request from ${receive.source}", e)
      }
    }
  }

  private def processCompletedSends() {
    selector.completedSends.asScala.foreach { send =>
      try {
        val resp = inflightResponses.remove(send.destination).getOrElse {
          throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
        }
        updateRequestMetrics(resp)
        selector.unmute(send.destination)
      } catch {
        case e: Throwable => processChannelException(send.destination,
            s"Exception while processing completed send to ${send.destination}", e)
      }
    }
  }

  private def updateRequestMetrics(response: RequestChannel.Response) {
    val request = response.request
    val networkThreadTimeNanos = openOrClosingChannel(request.context.connectionId).fold(0L)(_.getAndResetNetworkThreadTimeNanos())
    request.updateRequestMetrics(networkThreadTimeNanos, response)
  }

  /**
    * 处理因各种原因如 写失败、close了的连接， 需要清理已经暂用的内存
    */
  private def processDisconnected() {
    selector.disconnected.keySet.asScala.foreach { connectionId =>
      try {
        val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
          throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
        }.remoteHost
        inflightResponses.remove(connectionId).foreach(updateRequestMetrics)
        // the channel has been closed by the selector but the quotas still need to be updated
        // 减少连接计数器
        connectionQuotas.dec(InetAddress.getByName(remoteHost))
      } catch {
        case e: Throwable => processException(s"Exception while processing disconnection of $connectionId", e)
      }
    }
  }

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   * The channel will be immediately removed from the selector's `channels` or `closingChannels`
   * and no further disconnect notifications will be sent for this channel by the selector.
   * If responses are pending for the channel, they are dropped and metrics is updated.
   * If the channel has already been removed from selector, no action is taken.
   */
  private def close(connectionId: String): Unit = {
    openOrClosingChannel(connectionId).foreach { channel =>
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)
      selector.close(connectionId)

      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    //--flag-- 添加socketChannel至并发队列并唤醒Processor线程处理
    newConnections.add(socketChannel)
    //唤醒Processor线程从队列中获取socketChannel并处理
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    while (!newConnections.isEmpty) {
      //--flag-- 获取队列元素并从队列中移除，如果队列为空，则返回 null
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        //--flag-- 取到客户端的 SocketChannel，并将其注册到选择器中进行管理
        selector.register(connectionId(channel.socket), channel)
      } catch {
        // We explicitly catch all exceptions and close the socket to avoid a socket leak.
        case e: Throwable =>
          val remoteAddress = channel.socket.getRemoteSocketAddress
          // need to close the channel here to avoid a socket leak.
          close(channel)
          processException(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(channel.id)
    }
    selector.close()
    removeMetric(IdlePercentMetricName, Map(NetworkProcessorMetricTag -> id.toString))
  }

  // 'protected` to allow override for testing
  protected[network] def connectionId(socket: Socket): String = {
    val localHost = socket.getLocalAddress.getHostAddress
    val localPort = socket.getLocalPort
    val remoteHost = socket.getInetAddress.getHostAddress
    val remotePort = socket.getPort
    val connId = ConnectionId(localHost, localPort, remoteHost, remotePort, nextConnectionIndex).toString
    nextConnectionIndex = if (nextConnectionIndex == Int.MaxValue) 0 else nextConnectionIndex + 1
    connId
  }

  private[network] def enqueueResponse(response: RequestChannel.Response): Unit = {
    responseQueue.put(response)
    wakeup()
  }

  /**
    * --flag-- 从响应队列中取出一个响应对象
    * @return
    */
  private def dequeueResponse(): RequestChannel.Response = {
    val response = responseQueue.poll()
    if (response != null)
      response.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds
    response
  }

  private[network] def responseQueueSize = responseQueue.size

  // Only for testing
  private[network] def inflightResponseCount: Int = inflightResponses.size

  // Visible for testing
  // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.
  private[network] def openOrClosingChannel(connectionId: String): Option[KafkaChannel] =
     Option(selector.channel(connectionId)).orElse(Option(selector.closingChannel(connectionId)))

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  // Visible for testing
  private[network] def numStagedReceives(connectionId: String): Int =
    openOrClosingChannel(connectionId).map(c => selector.numStagedReceives(c)).getOrElse(0)

  /**
   * Wakeup the thread for selection.
   */
  override def wakeup() = selector.wakeup()

  override def shutdown(): Unit = {
    super.shutdown()
    removeMetric("IdlePercent", Map("networkProcessor" -> id.toString))
  }

}

/**
  *
  * @param defaultMax Integer.MAX_VALUE
  * @param overrideQuotas 通过参数 max.connections.per.ip.overrides 配置 ,如 host1:10,host2:20
  */
class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  //一个匿名函数
  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  private val counts = mutable.Map[InetAddress, Int]()

  /**
    * --flag--
    * 统计每个客户端的连接数， 同时会检测 每个客户端是否超过最大连接数限制
    *
    * @param address
    */
  def inc(address: InetAddress) {
    counts.synchronized {
      //--flag-- 用map结构来统计，如果是同一个客户端(使用：ipaddress 即ip地址)累加就累加
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)

      //--flag-- 获取客户端配置的最大连接数，如果没有配置，则返回默认最大连接数配置值，Integer.MAX_VALUE
      val max = overrides.getOrElse(address, defaultMax)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
