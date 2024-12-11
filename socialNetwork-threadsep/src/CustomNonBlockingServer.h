/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_CUSTOMNONBLOCKINGSERVER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_CUSTOMNONBLOCKINGSERVER_H_ 1

#include <thrift/Thrift.h>
// #include <thrift/std.h>
#include <thrift/server/TServer.h>
#include <thrift/transport/PlatformSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TNonblockingServerTransport.h>
// #include <thrift/concurrency/ThreadManager.h>
#include <climits>
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/PlatformThreadFactory.h>
#include <thrift/concurrency/Mutex.h>
#include <stack>
#include <vector>
#include <string>
#include <cstdlib>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <event.h>
#include <event2/event_compat.h>
#include <event2/event_struct.h>

#include <thrift/thrift-config.h>

// #include <thrift/server/CustomNonblockingServer.h>
#include <thrift/concurrency/Exception.h>
// #include <thrift/transport/TSocket.h>
// #include <thrift/concurrency/PlatformThreadFactory.h>
// #include <thrift/transport/PlatformSocket.h>
// #include "../../gen-cpp/HomeTimelineService.h"
#include "CustomThreadManager.h"
#include "CustomThreadFactory.h"

#include <map>
#include <functional>
// #include <torch/torch.h>

// namespace apache {
// namespace thrift {
// namespace server {

using apache::thrift::transport::TMemoryBuffer;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TNonblockingServerTransport;
using apache::thrift::protocol::TProtocol;
using apache::thrift::concurrency::Runnable;
// using apache::thrift::concurrency::ThreadManager;
using apache::thrift::concurrency::PlatformThreadFactory;
using apache::thrift::concurrency::ThreadFactory;
using apache::thrift::concurrency::Thread;
using apache::thrift::concurrency::Mutex;
using apache::thrift::concurrency::Guard;

#ifdef LIBEVENT_VERSION_NUMBER
#define LIBEVENT_VERSION_MAJOR (LIBEVENT_VERSION_NUMBER >> 24)
#define LIBEVENT_VERSION_MINOR ((LIBEVENT_VERSION_NUMBER >> 16) & 0xFF)
#define LIBEVENT_VERSION_REL ((LIBEVENT_VERSION_NUMBER >> 8) & 0xFF)
#else
// assume latest version 1 series
#define LIBEVENT_VERSION_MAJOR 1
#define LIBEVENT_VERSION_MINOR 14
#define LIBEVENT_VERSION_REL 13
#define LIBEVENT_VERSION_NUMBER                                                                      ((LIBEVENT_VERSION_MAJOR << 24) | (LIBEVENT_VERSION_MINOR << 16) | (LIBEVENT_VERSION_REL << 8))
#endif

#if LIBEVENT_VERSION_NUMBER < 0x02000000
typedef THRIFT_SOCKET evutil_socket_t;
#endif

#ifndef SOCKOPT_CAST_T
#ifndef _WIN32
#define SOCKOPT_CAST_T void
#else
#define SOCKOPT_CAST_T char
#endif // _WIN32
#endif

  struct modelType {
    float weight;
    float bias;

    modelType(float weight, float bias) : weight(weight), bias(bias) {}
  }; 

template <class T>
inline const SOCKOPT_CAST_T* const_cast_sockopt2(const T* v) {
  return reinterpret_cast<const SOCKOPT_CAST_T*>(v);
}

template <class T>
inline SOCKOPT_CAST_T* cast_sockopt2(T* v) {
  return reinterpret_cast<SOCKOPT_CAST_T*>(v);
}



/**
 * This is a non-blocking server in C++ for high performance that
 * operates a set of IO threads (by default only one). It assumes that
 * all incoming requests are framed with a 4 byte length indicator and
 * writes out responses using the same framing.
 */

/// Overload condition actions.
enum TOverloadAction {
  T_OVERLOAD_NO_ACTION,       ///< Don't handle overload */
  T_OVERLOAD_CLOSE_ON_ACCEPT, ///< Drop new connections immediately */
  T_OVERLOAD_DRAIN_TASK_QUEUE ///< Drop some tasks from head of task queue */
};

class TNonblockingIOThread;

class CustomNonblockingServer : public apache::thrift::server::TServer {
private:
  class TConnection;

  friend class TNonblockingIOThread;

private:
  /// Listen backlog
  static const int LISTEN_BACKLOG = 1024;

  /// Default limit on size of idle connection pool
  static const size_t CONNECTION_STACK_LIMIT = 1024;

  /// Default limit on frame size
  static const int MAX_FRAME_SIZE = 256 * 1024 * 1024;

  /// Default limit on total number of connected sockets
  static const int MAX_CONNECTIONS = INT_MAX;

  /// Default limit on connections in handler/task processing
  static const int MAX_ACTIVE_PROCESSORS = INT_MAX;

  /// Default size of write buffer
  static const int WRITE_BUFFER_DEFAULT_SIZE = 1024;

  /// Maximum size of read buffer allocated to idle connection (0 = unlimited)
  static const int IDLE_READ_BUFFER_LIMIT = 1024;

  /// Maximum size of write buffer allocated to idle connection (0 = unlimited)
  static const int IDLE_WRITE_BUFFER_LIMIT = 1024;

  /// # of calls before resizing oversized buffers (0 = check only on close)
  static const int RESIZE_BUFFER_EVERY_N = 512;

  /// # of IO threads to use by default
  static const int DEFAULT_IO_THREADS = 8;

  /// # of IO threads this server will use
  size_t numIOThreads_;

  /// Whether to set high scheduling priority for IO threads
  bool useHighPriorityIOThreads_;

  /// Server socket file descriptor
  THRIFT_SOCKET serverSocket_;

  /// The optional user-provided event-base (for single-thread servers)
  event_base* userEventBase_;

  /// For processing via thread pool, may be NULL
  std::shared_ptr<CustomThreadManager> threadManager_;

  /// Is thread pool processing?
  bool threadPoolProcessing_;

  // Factory to create the IO threads
  std::shared_ptr<ThreadFactory> ioThreadFactory_;

  // Vector of IOThread objects that will handle our IO
  std::vector<std::shared_ptr<TNonblockingIOThread> > ioThreads_;

  // Index of next IO Thread to be used (for round-robin)
  uint32_t nextIOThread_;

  // Synchronizes access to connection stack and similar data
  Mutex connMutex_;

  /// Number of TConnection object we've created
  size_t numTConnections_;

  /// Number of Connections processing or waiting to process
  size_t numActiveProcessors_;

  /// Limit for how many TConnection objects to cache
  size_t connectionStackLimit_;

  /// Limit for number of connections processing or waiting to process
  size_t maxActiveProcessors_;

  /// Limit for number of open connections
  size_t maxConnections_;

  /// Limit for frame size
  size_t maxFrameSize_;

  /// Time in milliseconds before an unperformed task expires (0 == infinite).
  int64_t taskExpireTime_;

  /**
   * Hysteresis for overload state.  This is the fraction of the overload
   * value that needs to be reached before the overload state is cleared;
   * must be <= 1.0.
   */
  double overloadHysteresis_;

  /// Action to take when we're overloaded.
  TOverloadAction overloadAction_;

  /**
   * The write buffer is initialized (and when idleWriteBufferLimit_ is checked
   * and found to be exceeded, reinitialized) to this size.
   */
  size_t writeBufferDefaultSize_;

  /**
   * Max read buffer size for an idle TConnection.  When we place an idle
   * TConnection into connectionStack_ or on every resizeBufferEveryN_ calls,
   * we will free the buffer (such that it will be reinitialized by the next
   * received frame) if it has exceeded this limit.  0 disables this check.
   */
  size_t idleReadBufferLimit_;

  /**
   * Max write buffer size for an idle connection.  When we place an idle
   * TConnection into connectionStack_ or on every resizeBufferEveryN_ calls,
   * we insure that its write buffer is <= to this size; otherwise we
   * replace it with a new one of writeBufferDefaultSize_ bytes to insure that
   * idle connections don't hog memory. 0 disables this check.
   */
  size_t idleWriteBufferLimit_;

  /**
   * Every N calls we check the buffer size limits on a connected TConnection.
   * 0 disables (i.e. the checks are only done when a connection closes).
   */
  int32_t resizeBufferEveryN_;

  /// Set if we are currently in an overloaded state.
  bool overloaded_;

  /// Count of connections dropped since overload started
  uint32_t nConnectionsDropped_;

  /// Count of connections dropped on overload since server started
  uint64_t nTotalConnectionsDropped_;

  /**
   * This is a stack of all the objects that have been created but that
   * are NOT currently in use. When we close a connection, we place it on this
   * stack so that the object can be reused later, rather than freeing the
   * memory and reallocating a new object later.
   */
  std::stack<TConnection*> connectionStack_;

  /**
   * This container holds pointers to all active connections. This container
   * allows the server to clean up unlcosed connection objects at destruction,
   * which in turn allows their transports, protocols, processors and handlers
   * to deallocate and clean up correctly.
   */
  std::vector<TConnection*> activeConnections_;

  /*
  */
  std::shared_ptr<TNonblockingServerTransport> serverTransport_;

  /**
   * Called when server socket had something happen.  We accept all waiting
   * client connections on listen socket fd and assign TConnection objects
   * to handle those requests.
   *
   * @param which the event flag that triggered the handler.
   */
  void handleEvent(THRIFT_SOCKET fd, short which);

  void init() {
    serverSocket_ = THRIFT_INVALID_SOCKET;
    numIOThreads_ = DEFAULT_IO_THREADS;
    nextIOThread_ = 0;
    useHighPriorityIOThreads_ = false;
    userEventBase_ = NULL;
    threadPoolProcessing_ = false;
    numTConnections_ = 0;
    numActiveProcessors_ = 0;
    connectionStackLimit_ = CONNECTION_STACK_LIMIT;
    maxActiveProcessors_ = MAX_ACTIVE_PROCESSORS;
    maxConnections_ = MAX_CONNECTIONS;
    maxFrameSize_ = MAX_FRAME_SIZE;
    taskExpireTime_ = 0;
    overloadHysteresis_ = 0.8;
    overloadAction_ = T_OVERLOAD_NO_ACTION;
    writeBufferDefaultSize_ = WRITE_BUFFER_DEFAULT_SIZE;
    idleReadBufferLimit_ = IDLE_READ_BUFFER_LIMIT;
    idleWriteBufferLimit_ = IDLE_WRITE_BUFFER_LIMIT;
    resizeBufferEveryN_ = RESIZE_BUFFER_EVERY_N;
    overloaded_ = false;
    nConnectionsDropped_ = 0;
    nTotalConnectionsDropped_ = 0;
  }

public:
  CustomNonblockingServer(const std::shared_ptr<apache::thrift::TProcessorFactory>& processorFactory,
                     const std::shared_ptr<apache::thrift::transport::TNonblockingServerTransport>& serverTransport)
    : TServer(processorFactory), serverTransport_(serverTransport) {
    init();
  }

  CustomNonblockingServer(const std::shared_ptr<apache::thrift::TProcessor>& processor,
                     const std::shared_ptr<apache::thrift::transport::TNonblockingServerTransport>& serverTransport)
    : TServer(processor), serverTransport_(serverTransport) {
    init();
  }


  CustomNonblockingServer(const std::shared_ptr<apache::thrift::TProcessorFactory>& processorFactory,
                     const std::shared_ptr<apache::thrift::protocol::TProtocolFactory>& protocolFactory,
                     const std::shared_ptr<apache::thrift::transport::TNonblockingServerTransport>& serverTransport,
                     const std::shared_ptr<ThreadFactory>& ioThreadFactory,
                     const std::shared_ptr<CustomThreadManager>& threadManager
                     = std::shared_ptr<CustomThreadManager>()
                     )
    : TServer(processorFactory), serverTransport_(serverTransport), ioThreadFactory_(ioThreadFactory) {
    init();

    setInputProtocolFactory(protocolFactory);
    setOutputProtocolFactory(protocolFactory);
    setThreadManager(threadManager);
  }

  CustomNonblockingServer(const std::shared_ptr<apache::thrift::TProcessor>& processor,
                     const std::shared_ptr<apache::thrift::protocol::TProtocolFactory>& protocolFactory,
                     const std::shared_ptr<apache::thrift::transport::TNonblockingServerTransport>& serverTransport,
                     const std::shared_ptr<ThreadFactory>& ioThreadFactory,
                     const std::shared_ptr<CustomThreadManager>& threadManager
                     = std::shared_ptr<CustomThreadManager>())
    : TServer(processor), serverTransport_(serverTransport), ioThreadFactory_(ioThreadFactory) {
    init();

    setInputProtocolFactory(protocolFactory);
    setOutputProtocolFactory(protocolFactory);
    setThreadManager(threadManager);
  }

  CustomNonblockingServer(const std::shared_ptr<apache::thrift::TProcessorFactory>& processorFactory,
                     const std::shared_ptr<apache::thrift::transport::TTransportFactory>& inputTransportFactory,
                     const std::shared_ptr<apache::thrift::transport::TTransportFactory>& outputTransportFactory,
                     const std::shared_ptr<apache::thrift::protocol::TProtocolFactory>& inputProtocolFactory,
                     const std::shared_ptr<apache::thrift::protocol::TProtocolFactory>& outputProtocolFactory,
                     const std::shared_ptr<apache::thrift::transport::TNonblockingServerTransport>& serverTransport,
                     const std::shared_ptr<ThreadFactory>& ioThreadFactory,
                     const std::shared_ptr<CustomThreadManager>& threadManager
                     = std::shared_ptr<CustomThreadManager>())
    : TServer(processorFactory), serverTransport_(serverTransport), ioThreadFactory_(ioThreadFactory) {
    init();

    setInputTransportFactory(inputTransportFactory);
    setOutputTransportFactory(outputTransportFactory);
    setInputProtocolFactory(inputProtocolFactory);
    setOutputProtocolFactory(outputProtocolFactory);
    setThreadManager(threadManager);
  }

  CustomNonblockingServer(const std::shared_ptr<apache::thrift::TProcessor>& processor,
                     const std::shared_ptr<apache::thrift::transport::TTransportFactory>& inputTransportFactory,
                     const std::shared_ptr<apache::thrift::transport::TTransportFactory>& outputTransportFactory,
                     const std::shared_ptr<apache::thrift::protocol::TProtocolFactory>& inputProtocolFactory,
                     const std::shared_ptr<apache::thrift::protocol::TProtocolFactory>& outputProtocolFactory,
                     const std::shared_ptr<apache::thrift::transport::TNonblockingServerTransport>& serverTransport,
                     const std::shared_ptr<ThreadFactory>& ioThreadFactory,
                     const std::shared_ptr<CustomThreadManager>& threadManager
                     = std::shared_ptr<CustomThreadManager>())
    : TServer(processor), serverTransport_(serverTransport), ioThreadFactory_(ioThreadFactory) {
    init();

    setInputTransportFactory(inputTransportFactory);
    setOutputTransportFactory(outputTransportFactory);
    setInputProtocolFactory(inputProtocolFactory);
    setOutputProtocolFactory(outputProtocolFactory);
    setThreadManager(threadManager);
  }

  ~CustomNonblockingServer();

  void setThreadManager(std::shared_ptr<CustomThreadManager> threadManager);
  int getListenPort() { return serverTransport_->getListenPort(); }

  std::shared_ptr<CustomThreadManager> getThreadManager() { return threadManager_; }

  /**
   * Sets the number of IO threads used by this server. Can only be used before
   * the call to serve() and has no effect afterwards.  We always use a
   * PosixThreadFactory for the IO worker threads, because they must joinable
   * for clean shutdown.
   */
  void setNumIOThreads(size_t numThreads) {
    numIOThreads_ = numThreads;
    // User-provided event-base doesn't works for multi-threaded servers
    assert(numIOThreads_ <= 1 || !userEventBase_);
  }

  /** Return whether the IO threads will get high scheduling priority */
  bool useHighPriorityIOThreads() const { return useHighPriorityIOThreads_; }

  /** Set whether the IO threads will get high scheduling priority. */
  void setUseHighPriorityIOThreads(bool val) { useHighPriorityIOThreads_ = val; }

  /** Return the number of IO threads used by this server. */
  size_t getNumIOThreads() const { return numIOThreads_; }

  /**
   * Get the maximum number of unused TConnection we will hold in reserve.
   *
   * @return the current limit on TConnection pool size.
   */
  size_t getConnectionStackLimit() const { return connectionStackLimit_; }

  /**
   * Set the maximum number of unused TConnection we will hold in reserve.
   *
   * @param sz the new limit for TConnection pool size.
   */
  void setConnectionStackLimit(size_t sz) { connectionStackLimit_ = sz; }

  bool isThreadPoolProcessing() const { return threadPoolProcessing_; }

  void addTask(std::shared_ptr<Runnable> task) {
    threadManager_->add(task, 0LL, taskExpireTime_);
  }

  /**
   * Return the count of sockets currently connected to.
   *
   * @return count of connected sockets.
   */
  size_t getNumConnections() const { return numTConnections_; }

  /**
   * Return the count of sockets currently connected to.
   *
   * @return count of connected sockets.
   */
  size_t getNumActiveConnections() const { return getNumConnections() - getNumIdleConnections(); }

  /**
   * Return the count of connection objects allocated but not in use.
   *
   * @return count of idle connection objects.
   */
  size_t getNumIdleConnections() const { return connectionStack_.size(); }

  /**
   * Return count of number of connections which are currently processing.
   * This is defined as a connection where all data has been received and
   * either assigned a task (when threading) or passed to a handler (when
   * not threading), and where the handler has not yet returned.
   *
   * @return # of connections currently processing.
   */
  size_t getNumActiveProcessors() const { return numActiveProcessors_; }

  /// Increment the count of connections currently processing.
  void incrementActiveProcessors() {
    Guard g(connMutex_);
    ++numActiveProcessors_;
  }

  /// Decrement the count of connections currently processing.
  void decrementActiveProcessors() {
    Guard g(connMutex_);
    if (numActiveProcessors_ > 0) {
      --numActiveProcessors_;
    }
  }

  /**
   * Get the maximum # of connections allowed before overload.
   *
   * @return current setting.
   */
  size_t getMaxConnections() const { return maxConnections_; }

  /**
   * Set the maximum # of connections allowed before overload.
   *
   * @param maxConnections new setting for maximum # of connections.
   */
  void setMaxConnections(size_t maxConnections) { maxConnections_ = maxConnections; }

  /**
   * Get the maximum # of connections waiting in handler/task before overload.
   *
   * @return current setting.
   */
  size_t getMaxActiveProcessors() const { return maxActiveProcessors_; }

  /**
   * Set the maximum # of connections waiting in handler/task before overload.
   *
   * @param maxActiveProcessors new setting for maximum # of active processes.
   */
  void setMaxActiveProcessors(size_t maxActiveProcessors) {
    maxActiveProcessors_ = maxActiveProcessors;
  }

  /**
   * Get the maximum allowed frame size.
   *
   * If a client tries to send a message larger than this limit,
   * its connection will be closed.
   *
   * @return Maxium frame size, in bytes.
   */
  size_t getMaxFrameSize() const { return maxFrameSize_; }

  /**
   * Set the maximum allowed frame size.
   *
   * @param maxFrameSize The new maximum frame size.
   */
  void setMaxFrameSize(size_t maxFrameSize) { maxFrameSize_ = maxFrameSize; }

  /**
   * Get fraction of maximum limits before an overload condition is cleared.
   *
   * @return hysteresis fraction
   */
  double getOverloadHysteresis() const { return overloadHysteresis_; }

  /**
   * Set fraction of maximum limits before an overload condition is cleared.
   * A good value would probably be between 0.5 and 0.9.
   *
   * @param hysteresisFraction fraction <= 1.0.
   */
  void setOverloadHysteresis(double hysteresisFraction) {
    if (hysteresisFraction <= 1.0 && hysteresisFraction > 0.0) {
      overloadHysteresis_ = hysteresisFraction;
    }
  }

  /**
   * Get the action the server will take on overload.
   *
   * @return a TOverloadAction enum value for the currently set action.
   */
  TOverloadAction getOverloadAction() const { return overloadAction_; }

  /**
   * Set the action the server is to take on overload.
   *
   * @param overloadAction a TOverloadAction enum value for the action.
   */
  void setOverloadAction(TOverloadAction overloadAction) { overloadAction_ = overloadAction; }

  /**
   * Get the time in milliseconds after which a task expires (0 == infinite).
   *
   * @return a 64-bit time in milliseconds.
   */
  int64_t getTaskExpireTime() const { return taskExpireTime_; }

  /**
   * Set the time in milliseconds after which a task expires (0 == infinite).
   *
   * @param taskExpireTime a 64-bit time in milliseconds.
   */
  void setTaskExpireTime(int64_t taskExpireTime) { taskExpireTime_ = taskExpireTime; }

  /**
   * Determine if the server is currently overloaded.
   * This function checks the maximums for open connections and connections
   * currently in processing, and sets an overload condition if they are
   * exceeded.  The overload will persist until both values are below the
   * current hysteresis fraction of their maximums.
   *
   * @return true if an overload condition exists, false if not.
   */
  bool serverOverloaded();

  /** Pop and discard next task on threadpool wait queue.
   *
   * @return true if a task was discarded, false if the wait queue was empty.
   */
  bool drainPendingTask();

  /**
   * Get the starting size of a TConnection object's write buffer.
   *
   * @return # bytes we initialize a TConnection object's write buffer to.
   */
  size_t getWriteBufferDefaultSize() const { return writeBufferDefaultSize_; }

  /**
   * Set the starting size of a TConnection object's write buffer.
   *
   * @param size # bytes we initialize a TConnection object's write buffer to.
   */
  void setWriteBufferDefaultSize(size_t size) { writeBufferDefaultSize_ = size; }

  /**
   * Get the maximum size of read buffer allocated to idle TConnection objects.
   *
   * @return # bytes beyond which we will dealloc idle buffer.
   */
  size_t getIdleReadBufferLimit() const { return idleReadBufferLimit_; }

  /**
   * [NOTE: This is for backwards compatibility, use getIdleReadBufferLimit().]
   * Get the maximum size of read buffer allocated to idle TConnection objects.
   *
   * @return # bytes beyond which we will dealloc idle buffer.
   */
  size_t getIdleBufferMemLimit() const { return idleReadBufferLimit_; }

  /**
   * Set the maximum size read buffer allocated to idle TConnection objects.
   * If a TConnection object is found (either on connection close or between
   * calls when resizeBufferEveryN_ is set) with more than this much memory
   * allocated to its read buffer, we free it and allow it to be reinitialized
   * on the next received frame.
   *
   * @param limit of bytes beyond which we will shrink buffers when checked.
   */
  void setIdleReadBufferLimit(size_t limit) { idleReadBufferLimit_ = limit; }

  /**
   * [NOTE: This is for backwards compatibility, use setIdleReadBufferLimit().]
   * Set the maximum size read buffer allocated to idle TConnection objects.
   * If a TConnection object is found (either on connection close or between
   * calls when resizeBufferEveryN_ is set) with more than this much memory
   * allocated to its read buffer, we free it and allow it to be reinitialized
   * on the next received frame.
   *
   * @param limit of bytes beyond which we will shrink buffers when checked.
   */
  void setIdleBufferMemLimit(size_t limit) { idleReadBufferLimit_ = limit; }

  /**
   * Get the maximum size of write buffer allocated to idle TConnection objects.
   *
   * @return # bytes beyond which we will reallocate buffers when checked.
   */
  size_t getIdleWriteBufferLimit() const { return idleWriteBufferLimit_; }

  /**
   * Set the maximum size write buffer allocated to idle TConnection objects.
   * If a TConnection object is found (either on connection close or between
   * calls when resizeBufferEveryN_ is set) with more than this much memory
   * allocated to its write buffer, we destroy and construct that buffer with
   * writeBufferDefaultSize_ bytes.
   *
   * @param limit of bytes beyond which we will shrink buffers when idle.
   */
  void setIdleWriteBufferLimit(size_t limit) { idleWriteBufferLimit_ = limit; }

  /**
   * Get # of calls made between buffer size checks.  0 means disabled.
   *
   * @return # of calls between buffer size checks.
   */
  int32_t getResizeBufferEveryN() const { return resizeBufferEveryN_; }

  /**
   * Check buffer sizes every "count" calls.  This allows buffer limits
   * to be enforced for persistent connections with a controllable degree
   * of overhead. 0 disables checks except at connection close.
   *
   * @param count the number of calls between checks, or 0 to disable
   */
  void setResizeBufferEveryN(int32_t count) { resizeBufferEveryN_ = count; }

  /**
   * Main workhorse function, starts up the server listening on a port and
   * loops over the libevent handler.
   */
  void serve();

  /**
   * Causes the server to terminate gracefully (can be called from any thread).
   */
  void stop();

  /// Creates a socket to listen on and binds it to the local port.
  void createAndListenOnSocket();

  /**
   * Register the optional user-provided event-base (for single-thread servers)
   *
   * This method should be used when the server is running in a single-thread
   * mode, and the event base is provided by the user (i.e., the caller).
   *
   * @param user_event_base the user-provided event-base. The user is
   * responsible for freeing the event base memory.
   */
  void registerEvents(event_base* user_event_base);

  /**
   * Returns the optional user-provided event-base (for single-thread servers).
   */
  event_base* getUserEventBase() const { return userEventBase_; }

  /** Some transports, like THeaderTransport, require passing through
   * the framing size instead of stripping it.
   */
  bool getHeaderTransport();

  /**
   * Updates the CPU affinity for the thread pool managed by this server
   * 
   * @param cpuIds Vector of CPU IDs to bind the thread pool to
   * @return true if successful, false if threadManager is not initialized
   */
  bool changeWorkerCpuset(const std::vector<int>& cpuIds) {
    if (!threadManager_) {
      return false;
    }
    
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    
    // Add each CPU ID to the set
    for (int cpu : cpuIds) {
      CPU_SET(cpu, &cpuset);
    }
    
    // Update thread manager's CPU affinity
    threadManager_->changeCpuset(&cpuset);
    return true;
  }

  /**
   * Updates the CPU affinity for the IO threads managed by this server
   * 
   * @param cpuIds Vector of CPU IDs to bind the IO threads to
   * @return true if successful, false if IO threads are not initialized
   */
  // bool changeIOCpuset(const std::vector<int>& cpuIds) {
  //   if (ioThreads_.empty()) {
  //       return false;
  //   }

  //   cpu_set_t cpuset;
  //   CPU_ZERO(&cpuset);
    
  //   // Add each CPU ID to the set
  //   for (int cpu : cpuIds) {
  //       CPU_SET(cpu, &cpuset);
  //   }

  //   // Update thread factory's CPU affinity if it exists
  //   if (ioThreadFactory_) {
  //       ioThreadFactory_->changeCpuset(&cpuset);
  //   }

  //   // Update each IO thread's CPU affinity through PthreadThread's changeCpuset
  //   for (auto& ioThread : ioThreads_) {
  //       if (ioThread && ioThread->getThread()) {
  //           std::shared_ptr<PthreadThread> pthreadThread = 
  //               std::dynamic_pointer_cast<PthreadThread>(ioThread->getThread());
  //           if (pthreadThread) {
  //               pthreadThread->changeCpuset(&cpuset);
  //           }
  //       }
  //   }

  //   return true;
  // }

private:
  /**
   * Callback function that the threadmanager calls when a task reaches
   * its expiration time.  It is needed to clean up the expired connection.
   *
   * @param task the runnable associated with the expired task.
   */
  void expireClose(std::shared_ptr<Runnable> task);

  /**
   * Return an initialized connection object.  Creates or recovers from
   * pool a TConnection and initializes it with the provided socket FD
   * and flags.
   *
   * @param socket FD of socket associated with this connection.
   * @param addr the sockaddr of the client
   * @param addrLen the length of addr
   * @return pointer to initialized TConnection object.
   */
  TConnection* createConnection(std::shared_ptr<TSocket> socket);

  /**
   * Returns a connection to pool or deletion.  If the connection pool
   * (a stack) isn't full, place the connection object on it, otherwise
   * just delete it.
   *
   * @param connection the TConection being returned.
   */
  void returnConnection(TConnection* connection);
};

class TNonblockingIOThread : public Runnable {
public:
  // Creates an IO thread and sets up the event base.  The listenSocket should
  // be a valid FD on which listen() has already been called.  If the
  // listenSocket is < 0, accepting will not be done.
  TNonblockingIOThread(CustomNonblockingServer* server,
                       int number,
                       THRIFT_SOCKET listenSocket,
                       bool useHighPriority);

  ~TNonblockingIOThread();

  // Returns the event-base for this thread.
  event_base* getEventBase() const { return eventBase_; }

  // Returns the server for this thread.
  CustomNonblockingServer* getServer() const { return server_; }

  // Returns the number of this IO thread.
  int getThreadNumber() const { return number_; }

  // Returns the thread id associated with this object.  This should
  // only be called after the thread has been started.
  Thread::id_t getThreadId() const { return threadId_; }

  // Returns the send-fd for task complete notifications.
  evutil_socket_t getNotificationSendFD() const { return notificationPipeFDs_[1]; }

  // Returns the read-fd for task complete notifications.
  evutil_socket_t getNotificationRecvFD() const { return notificationPipeFDs_[0]; }

  // Returns the actual thread object associated with this IO thread.
  std::shared_ptr<Thread> getThread() const { return thread_; }

  // Sets the actual thread object associated with this IO thread.
  void setThread(const std::shared_ptr<Thread>& t) { thread_ = t; }

  // Used by TConnection objects to indicate processing has finished.
  bool notify(CustomNonblockingServer::TConnection* conn);

  // Enters the event loop and does not return until a call to stop().
  virtual void run();

  // Exits the event loop as soon as possible.
  void stop();

  // Ensures that the event-loop thread is fully finished and shut down.
  void join();

  /// Registers the events for the notification & listen sockets
  void registerEvents();

private:
  /**
   * C-callable event handler for signaling task completion.  Provides a
   * callback that libevent can understand that will read a connection
   * object's address from a pipe and call connection->transition() for
   * that object.
   *
   * @param fd the descriptor the event occurred on.
   */
  static void notifyHandler(evutil_socket_t fd, short which, void* v);

  /**
   * C-callable event handler for listener events.  Provides a callback
   * that libevent can understand which invokes server->handleEvent().
   *
   * @param fd the descriptor the event occurred on.
   * @param which the flags associated with the event.
   * @param v void* callback arg where we placed CustomNonblockingServer's "this".
   */
  static void listenHandler(evutil_socket_t fd, short which, void* v) {
    ((CustomNonblockingServer*)v)->handleEvent(fd, which);
  }

  /// Exits the loop ASAP in case of shutdown or error.
  void breakLoop(bool error);

  /// Create the pipe used to notify I/O process of task completion.
  void createNotificationPipe();

  /// Unregisters our events for notification and listen sockets.
  void cleanupEvents();

  /// Sets (or clears) high priority scheduling status for the current thread.
  void setCurrentThreadHighPriority(bool value);

private:
  /// associated server
  CustomNonblockingServer* server_;

  /// thread number (for debugging).
  const int number_;

  /// The actual physical thread id.
  Thread::id_t threadId_;

  /// If listenSocket_ >= 0, adds an event on the event_base to accept conns
  THRIFT_SOCKET listenSocket_;

  /// Sets a high scheduling priority when running
  bool useHighPriority_;

  /// pointer to eventbase to be used for looping
  event_base* eventBase_;

  /// Set to true if this class is responsible for freeing the event base
  /// memory.
  bool ownEventBase_;

  /// Used with eventBase_ for connection events (only in listener thread)
  struct event serverEvent_;

  /// Used with eventBase_ for task completion notification
  struct event notificationEvent_;

  /// File descriptors for pipe used for task completion notification.
  evutil_socket_t notificationPipeFDs_[2];

  /// Actual IO Thread
  std::shared_ptr<Thread> thread_;
};
// }
// }
// } // apache::thrift::server



#include <algorithm>
#include <iostream>

#ifdef HAVE_POLL_H
#include <poll.h>
#elif HAVE_SYS_POLL_H
#include <sys/poll.h>
#elif HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#include <netinet/tcp.h>
#endif

#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif

#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif

#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include <assert.h>

#ifdef HAVE_SCHED_H
#include <sched.h>
#endif

#ifndef AF_LOCAL
#define AF_LOCAL AF_UNIX
#endif

#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif

// namespace apache {
// namespace thrift {
// namespace server {

using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;
using namespace apache::thrift;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransportException;
using std::shared_ptr;

/// Three states for sockets: recv frame size, recv data, and send mode
enum TSocketState { SOCKET_RECV_FRAMING, SOCKET_RECV, SOCKET_SEND };

/**
 * Five states for the nonblocking server:
 *  1) initialize
 *  2) read 4 byte frame size
 *  3) read frame of data
 *  4) send back data (if any)
 *  5) force immediate connection close
 */
enum TAppState {
  APP_INIT,
  APP_READ_FRAME_SIZE,
  APP_READ_REQUEST,
  APP_WAIT_TASK,
  APP_SEND_RESULT,
  APP_CLOSE_CONNECTION
};

/**
 * Represents a connection that is handled via libevent. This connection
 * essentially encapsulates a socket that has some associated libevent state.
 */
class CustomNonblockingServer::TConnection {
private:
  /// Server IO Thread handling this connection
  TNonblockingIOThread* ioThread_;

  /// Server handle
  CustomNonblockingServer* server_;

  /// TProcessor
  std::shared_ptr<TProcessor> processor_;

  /// Object wrapping network socket
  std::shared_ptr<TSocket> tSocket_;

  /// Libevent object
  struct event event_;

  /// Libevent flags
  short eventFlags_;

  /// Socket mode
  TSocketState socketState_;

  /// Application state
  TAppState appState_;

  /// How much data needed to read
  uint32_t readWant_;

  /// Where in the read buffer are we
  uint32_t readBufferPos_;

  /// Read buffer
  uint8_t* readBuffer_;

  /// Read buffer size
  uint32_t readBufferSize_;

  /// Write buffer
  uint8_t* writeBuffer_;

  /// Write buffer size
  uint32_t writeBufferSize_;

  /// How far through writing are we?
  uint32_t writeBufferPos_;

  /// Largest size of write buffer seen since buffer was constructed
  size_t largestWriteBufferSize_;

  /// Count of the number of calls for use with getResizeBufferEveryN().
  int32_t callsForResize_;

  /// Transport to read from
  std::shared_ptr<TMemoryBuffer> inputTransport_;

  /// Transport that processor writes to
  std::shared_ptr<TMemoryBuffer> outputTransport_;

  /// extra transport generated by transport factory (e.g. BufferedRouterTransport)
  std::shared_ptr<TTransport> factoryInputTransport_;
  std::shared_ptr<TTransport> factoryOutputTransport_;

  /// Protocol decoder
  std::shared_ptr<TProtocol> inputProtocol_;

  /// Protocol encoder
  std::shared_ptr<TProtocol> outputProtocol_;

  /// Server event handler, if any
  std::shared_ptr<apache::thrift::server::TServerEventHandler> serverEventHandler_;

  /// Thrift call context, if any
  void* connectionContext_;

  /// Go into read mode
  void setRead() { setFlags(EV_READ | EV_PERSIST); }

  /// Go into write mode
  void setWrite() { setFlags(EV_WRITE | EV_PERSIST); }

  /// Set socket idle
  void setIdle() { setFlags(0); }

  /**
   * Set event flags for this connection.
   *
   * @param eventFlags flags we pass to libevent for the connection.
   */
  void setFlags(short eventFlags);

  /**
   * Libevent handler called (via our static wrapper) when the connection
   * socket had something happen.  Rather than use the flags libevent passed,
   * we use the connection state to determine whether we need to read or
   * write the socket.
   */
  void workSocket();

public:
  class Task;

  /// Constructor
  TConnection(std::shared_ptr<TSocket> socket,
              TNonblockingIOThread* ioThread) {
    readBuffer_ = NULL;
    readBufferSize_ = 0;

    ioThread_ = ioThread;
    server_ = ioThread->getServer();

    // Allocate input and output transports these only need to be allocated
    // once per TConnection (they don't need to be reallocated on init() call)
    inputTransport_.reset(new TMemoryBuffer(readBuffer_, readBufferSize_));
    outputTransport_.reset(
        new TMemoryBuffer(static_cast<uint32_t>(server_->getWriteBufferDefaultSize())));

    tSocket_ =  socket;

    init(ioThread);
  }

  ~TConnection() { std::free(readBuffer_); }

  /// Close this connection and free or reset its resources.
  void close();

  /**
    * Check buffers against any size limits and shrink it if exceeded.
    *
    * @param readLimit we reduce read buffer size to this (if nonzero).
    * @param writeLimit if nonzero and write buffer is larger, replace it.
    */
  void checkIdleBufferMemLimit(size_t readLimit, size_t writeLimit);

  /// Initialize
  void init(TNonblockingIOThread* ioThread);

  /// set socket for connection
  void setSocket(std::shared_ptr<TSocket> socket);

  /**
   * This is called when the application transitions from one state into
   * another. This means that it has finished writing the data that it needed
   * to, or finished receiving the data that it needed to.
   */
  void transition();

  /**
   * C-callable event handler for connection events.  Provides a callback
   * that libevent can understand which invokes connection_->workSocket().
   *
   * @param fd the descriptor the event occurred on.
   * @param which the flags associated with the event.
   * @param v void* callback arg where we placed TConnection's "this".
   */
  static void eventHandler(evutil_socket_t fd, short /* which */, void* v) {
    assert(fd == static_cast<evutil_socket_t>(((TConnection*)v)->getTSocket()->getSocketFD()));
    ((TConnection*)v)->workSocket();
  }

  /**
   * Notification to server that processing has ended on this request.
   * Can be called either when processing is completed or when a waiting
   * task has been preemptively terminated (on overload).
   *
   * Don't call this from the IO thread itself.
   *
   * @return true if successful, false if unable to notify (check THRIFT_GET_SOCKET_ERROR).
   */
  bool notifyIOThread() { return ioThread_->notify(this); }

  /*
   * Returns the number of this connection's currently assigned IO
   * thread.
   */
  int getIOThreadNumber() const { return ioThread_->getThreadNumber(); }

  /// Force connection shutdown for this connection.
  void forceClose() {
    appState_ = APP_CLOSE_CONNECTION;
    if (!notifyIOThread()) {
      server_->decrementActiveProcessors();
      close();
      throw TException("TConnection::forceClose: failed write on notify pipe");
    }
  }

  /// return the server this connection was initialized for.
  CustomNonblockingServer* getServer() const { return server_; }

  /// get state of connection.
  TAppState getState() const { return appState_; }

  /// return the TSocket transport wrapping this network connection
  std::shared_ptr<TSocket> getTSocket() const { return tSocket_; }

  /// return the server event handler if any
  std::shared_ptr<apache::thrift::server::TServerEventHandler> getServerEventHandler() { return serverEventHandler_; }

  /// return the Thrift connection context if any
  void* getConnectionContext() { return connectionContext_; }
};

class CustomNonblockingServer::TConnection::Task : public Runnable {
public:
  Task(std::shared_ptr<TProcessor> processor,
       std::shared_ptr<TProtocol> input,
       std::shared_ptr<TProtocol> output,
       TConnection* connection)
    : processor_(processor),
      input_(input),
      output_(output),
      connection_(connection),
      serverEventHandler_(connection_->getServerEventHandler()),
      connectionContext_(connection_->getConnectionContext()) {}

  void run() {
    try {
      for (;;) {
        if (serverEventHandler_) {
          serverEventHandler_->processContext(connectionContext_, connection_->getTSocket());
        }
        if (!processor_->process(input_, output_, connectionContext_)
            || !input_->getTransport()->peek()) {
          break;
        }
      }
    } catch (const TTransportException& ttx) {
      GlobalOutput.printf("CustomNonblockingServer: client died: %s", ttx.what());
    } catch (const std::bad_alloc&) {
      GlobalOutput("CustomNonblockingServer: caught bad_alloc exception.");
      exit(1);
    } catch (const std::exception& x) {
      GlobalOutput.printf("CustomNonblockingServer: process() exception: %s: %s",
                          typeid(x).name(),
                          x.what());
    } catch (...) {
      GlobalOutput.printf("CustomNonblockingServer: unknown exception while processing.");
    }

    // Signal completion back to the libevent thread via a pipe
    if (!connection_->notifyIOThread()) {
      GlobalOutput.printf("CustomNonblockingServer: failed to notifyIOThread, closing.");
      connection_->server_->decrementActiveProcessors();
      connection_->close();
      throw TException("CustomNonblockingServer::Task::run: failed write on notify pipe");
    }
  }

  TConnection* getTConnection() { return connection_; }

private:
  std::shared_ptr<TProcessor> processor_;
  std::shared_ptr<TProtocol> input_;
  std::shared_ptr<TProtocol> output_;
  TConnection* connection_;
  std::shared_ptr<apache::thrift::server::TServerEventHandler> serverEventHandler_;
  void* connectionContext_;
};

void CustomNonblockingServer::TConnection::init(TNonblockingIOThread* ioThread) {
  ioThread_ = ioThread;
  server_ = ioThread->getServer();
  appState_ = APP_INIT;
  eventFlags_ = 0;

  readBufferPos_ = 0;
  readWant_ = 0;

  writeBuffer_ = NULL;
  writeBufferSize_ = 0;
  writeBufferPos_ = 0;
  largestWriteBufferSize_ = 0;

  socketState_ = SOCKET_RECV_FRAMING;
  callsForResize_ = 0;

  // get input/transports
  factoryInputTransport_ = server_->getInputTransportFactory()->getTransport(inputTransport_);
  factoryOutputTransport_ = server_->getOutputTransportFactory()->getTransport(outputTransport_);

  // Create protocol
  if (server_->getHeaderTransport()) {
    inputProtocol_ = server_->getInputProtocolFactory()->getProtocol(factoryInputTransport_,
                                                                     factoryOutputTransport_);
    outputProtocol_ = inputProtocol_;
  } else {
    inputProtocol_ = server_->getInputProtocolFactory()->getProtocol(factoryInputTransport_);
    outputProtocol_ = server_->getOutputProtocolFactory()->getProtocol(factoryOutputTransport_);
  }

  // Set up for any server event handler
  serverEventHandler_ = server_->getEventHandler();
  if (serverEventHandler_) {
    connectionContext_ = serverEventHandler_->createContext(inputProtocol_, outputProtocol_);
  } else {
    connectionContext_ = NULL;
  }

  // Get the processor
  processor_ = server_->getProcessor(inputProtocol_, outputProtocol_, tSocket_);
}

void CustomNonblockingServer::TConnection::setSocket(std::shared_ptr<TSocket> socket) {
  tSocket_ = socket;
}

void CustomNonblockingServer::TConnection::workSocket() {
  int got = 0, left = 0, sent = 0;
  uint32_t fetch = 0;

  switch (socketState_) {
  case SOCKET_RECV_FRAMING:
    union {
      uint8_t buf[sizeof(uint32_t)];
      uint32_t size;
    } framing;

    // if we've already received some bytes we kept them here
    framing.size = readWant_;
    // determine size of this frame
    try {
      // Read from the socket
      fetch = tSocket_->read(&framing.buf[readBufferPos_],
                             uint32_t(sizeof(framing.size) - readBufferPos_));
      if (fetch == 0) {
        // Whenever we get here it means a remote disconnect
        close();
        return;
      }
      readBufferPos_ += fetch;
    } catch (TTransportException& te) {
      //In Nonblocking SSLSocket some operations need to be retried again.
      //Current approach is parsing exception message, but a better solution needs to be investigated.
      if(!strstr(te.what(), "retry")) {
        GlobalOutput.printf("TConnection::workSocket(): %s", te.what());
        close();

        return;
      }
    }

    if (readBufferPos_ < sizeof(framing.size)) {
      // more needed before frame size is known -- save what we have so far
      readWant_ = framing.size;
      return;
    }

    readWant_ = ntohl(framing.size);
    if (readWant_ > server_->getMaxFrameSize()) {
      // Don't allow giant frame sizes.  This prevents bad clients from
      // causing us to try and allocate a giant buffer.
      GlobalOutput.printf(
          "CustomNonblockingServer: frame size too large "
          "(%" PRIu32 " > %" PRIu64
          ") from client %s. "
          "Remote side not using TFramedTransport?",
          readWant_,
          (uint64_t)server_->getMaxFrameSize(),
          tSocket_->getSocketInfo().c_str());
      close();
      return;
    }
    // size known; now get the rest of the frame
    transition();

    // If the socket has more data than the frame header, continue to work on it. This is not strictly necessary for
    // regular sockets, because if there is more data, libevent will fire the event handler registered for read
    // readiness, which will in turn call workSocket(). However, some socket types (such as TSSLSocket) may have the
    // data sitting in their internal buffers and from libevent's perspective, there is no further data available. In
    // that case, not having this workSocket() call here would result in a hang as we will never get to work the socket,
    // despite having more data.
    if (tSocket_->hasPendingDataToRead())
    {
        workSocket();
    }

    return;

  case SOCKET_RECV:
    // It is an error to be in this state if we already have all the data
    assert(readBufferPos_ < readWant_);

    try {
      // Read from the socket
      fetch = readWant_ - readBufferPos_;
      got = tSocket_->read(readBuffer_ + readBufferPos_, fetch);
    } catch (TTransportException& te) {
      //In Nonblocking SSLSocket some operations need to be retried again.
      //Current approach is parsing exception message, but a better solution needs to be investigated.
      if(!strstr(te.what(), "retry")) {
        GlobalOutput.printf("TConnection::workSocket(): %s", te.what());
        close();
      }

      return;
    }

    if (got > 0) {
      // Move along in the buffer
      readBufferPos_ += got;

      // Check that we did not overdo it
      assert(readBufferPos_ <= readWant_);

      // We are done reading, move onto the next state
      if (readBufferPos_ == readWant_) {
        transition();
      }
      return;
    }

    // Whenever we get down here it means a remote disconnect
    close();

    return;

  case SOCKET_SEND:
    // Should never have position past size
    assert(writeBufferPos_ <= writeBufferSize_);

    // If there is no data to send, then let us move on
    if (writeBufferPos_ == writeBufferSize_) {
      GlobalOutput("WARNING: Send state with no data to send");
      transition();
      return;
    }

    try {
      left = writeBufferSize_ - writeBufferPos_;
      sent = tSocket_->write_partial(writeBuffer_ + writeBufferPos_, left);
    } catch (TTransportException& te) {
      GlobalOutput.printf("TConnection::workSocket(): %s ", te.what());
      close();
      return;
    }

    writeBufferPos_ += sent;

    // Did we overdo it?
    assert(writeBufferPos_ <= writeBufferSize_);

    // We are done!
    if (writeBufferPos_ == writeBufferSize_) {
      transition();
    }

    return;

  default:
    GlobalOutput.printf("Unexpected Socket State %d", socketState_);
    assert(0);
  }
}

bool CustomNonblockingServer::getHeaderTransport() {
  // Currently if there is no output protocol factory,
  // we assume header transport (without having to create
  // a new transport and check)
  return getOutputProtocolFactory() == NULL;
}

/**
 * This is called when the application transitions from one state into
 * another. This means that it has finished writing the data that it needed
 * to, or finished receiving the data that it needed to.
 */
void CustomNonblockingServer::TConnection::transition() {
  // ensure this connection is active right now
  assert(ioThread_);
  assert(server_);

  // Switch upon the state that we are currently in and move to a new state
  switch (appState_) {

  case APP_READ_REQUEST:
    // We are done reading the request, package the read buffer into transport
    // and get back some data from the dispatch function
    if (server_->getHeaderTransport()) {
      // GlobalOutput.printf("getHeaderTransport()");
      inputTransport_->resetBuffer(readBuffer_, readBufferPos_);
      outputTransport_->resetBuffer();
    } else {
      // GlobalOutput.printf("No HeaderTransport()");
      // We saved room for the framing size in case header transport needed it,
      // but just skip it for the non-header case
      inputTransport_->resetBuffer(readBuffer_ + 4, readBufferPos_ - 4);
      outputTransport_->resetBuffer();

      // Prepend four bytes of blank space to the buffer so we can
      // write the frame size there later.
      outputTransport_->getWritePtr(4);
      outputTransport_->wroteBytes(4);
    }

    server_->incrementActiveProcessors();

    if (server_->isThreadPoolProcessing()) {
      // We are setting up a Task to do this work and we will wait on it

      // Create task and dispatch to the thread manager
      std::shared_ptr<Runnable> task = std::shared_ptr<Runnable>(
          new Task(processor_, inputProtocol_, outputProtocol_, this));
      // The application is now waiting on the task to finish
      appState_ = APP_WAIT_TASK;

      // Set this connection idle so that libevent doesn't process more
      // data on it while we're still waiting for the threadmanager to
      // finish this task
      setIdle();

      try {
        server_->addTask(task);
      } catch (IllegalStateException& ise) {
        // The ThreadManager is not ready to handle any more tasks (it's probably shutting down).
        GlobalOutput.printf("IllegalStateException: Server::process() %s", ise.what());
        server_->decrementActiveProcessors();
        close();
      } catch (TimedOutException& to) {
        GlobalOutput.printf("[ERROR] TimedOutException: Server::process() %s", to.what());
        server_->decrementActiveProcessors();
        close();
      }

      return;
    } else {
      try {
        if (serverEventHandler_) {
          serverEventHandler_->processContext(connectionContext_, getTSocket());
        }
        // Invoke the processor
        processor_->process(inputProtocol_, outputProtocol_, connectionContext_);
      } catch (const TTransportException& ttx) {
        GlobalOutput.printf(
            "CustomNonblockingServer transport error in "
            "process(): %s",
            ttx.what());
        server_->decrementActiveProcessors();
        close();
        return;
      } catch (const std::exception& x) {
        GlobalOutput.printf("Server::process() uncaught exception: %s: %s",
                            typeid(x).name(),
                            x.what());
        server_->decrementActiveProcessors();
        close();
        return;
      } catch (...) {
        GlobalOutput.printf("Server::process() unknown exception");
        server_->decrementActiveProcessors();
        close();
        return;
      }
    }
    // fallthrough

  // Intentionally fall through here, the call to process has written into
  // the writeBuffer_

  case APP_WAIT_TASK:
    // We have now finished processing a task and the result has been written
    // into the outputTransport_, so we grab its contents and place them into
    // the writeBuffer_ for actual writing by the libevent thread

    server_->decrementActiveProcessors();
    // Get the result of the operation
    outputTransport_->getBuffer(&writeBuffer_, &writeBufferSize_);

    // If the function call generated return data, then move into the send
    // state and get going
    // 4 bytes were reserved for frame size
    if (writeBufferSize_ > 4) {

      // Move into write state
      writeBufferPos_ = 0;
      socketState_ = SOCKET_SEND;

      // Put the frame size into the write buffer
      int32_t frameSize = (int32_t)htonl(writeBufferSize_ - 4);
      memcpy(writeBuffer_, &frameSize, 4);

      // Socket into write mode
      appState_ = APP_SEND_RESULT;
      setWrite();

      return;
    }

    // In this case, the request was oneway and we should fall through
    // right back into the read frame header state
    goto LABEL_APP_INIT;

  case APP_SEND_RESULT:
    // it's now safe to perform buffer size housekeeping.
    if (writeBufferSize_ > largestWriteBufferSize_) {
      largestWriteBufferSize_ = writeBufferSize_;
    }
    if (server_->getResizeBufferEveryN() > 0
        && ++callsForResize_ >= server_->getResizeBufferEveryN()) {
      checkIdleBufferMemLimit(server_->getIdleReadBufferLimit(),
                              server_->getIdleWriteBufferLimit());
      callsForResize_ = 0;
    }
    // fallthrough

  // N.B.: We also intentionally fall through here into the INIT state!

  LABEL_APP_INIT:
  case APP_INIT:

    // Clear write buffer variables
    writeBuffer_ = NULL;
    writeBufferPos_ = 0;
    writeBufferSize_ = 0;

    // Into read4 state we go
    socketState_ = SOCKET_RECV_FRAMING;
    appState_ = APP_READ_FRAME_SIZE;

    readBufferPos_ = 0;

    // Register read event
    setRead();

    return;

  case APP_READ_FRAME_SIZE:
    readWant_ += 4;

    // We just read the request length
    // Double the buffer size until it is big enough
    if (readWant_ > readBufferSize_) {
      if (readBufferSize_ == 0) {
        readBufferSize_ = 1;
      }
      uint32_t newSize = readBufferSize_;
      while (readWant_ > newSize) {
        newSize *= 2;
      }

      uint8_t* newBuffer = (uint8_t*)std::realloc(readBuffer_, newSize);
      if (newBuffer == NULL) {
        // nothing else to be done...
        throw std::bad_alloc();
      }
      readBuffer_ = newBuffer;
      readBufferSize_ = newSize;
    }

    readBufferPos_ = 4;
    *((uint32_t*)readBuffer_) = htonl(readWant_ - 4);

    // Move into read request state
    socketState_ = SOCKET_RECV;
    appState_ = APP_READ_REQUEST;

    return;

  case APP_CLOSE_CONNECTION:
    server_->decrementActiveProcessors();
    close();
    return;

  default:
    GlobalOutput.printf("Unexpected Application State %d", appState_);
    assert(0);
  }
}

void CustomNonblockingServer::TConnection::setFlags(short eventFlags) {
  // Catch the do nothing case
  if (eventFlags_ == eventFlags) {
    return;
  }

  // Delete a previously existing event
  if (eventFlags_ && event_del(&event_) == -1) {
    GlobalOutput.perror("TConnection::setFlags() event_del", THRIFT_GET_SOCKET_ERROR);
    return;
  }

  // Update in memory structure
  eventFlags_ = eventFlags;

  // Do not call event_set if there are no flags
  if (!eventFlags_) {
    return;
  }

  /*
   * event_set:
   *
   * Prepares the event structure &event to be used in future calls to
   * event_add() and event_del().  The event will be prepared to call the
   * eventHandler using the 'sock' file descriptor to monitor events.
   *
   * The events can be either EV_READ, EV_WRITE, or both, indicating
   * that an application can read or write from the file respectively without
   * blocking.
   *
   * The eventHandler will be called with the file descriptor that triggered
   * the event and the type of event which will be one of: EV_TIMEOUT,
   * EV_SIGNAL, EV_READ, EV_WRITE.
   *
   * The additional flag EV_PERSIST makes an event_add() persistent until
   * event_del() has been called.
   *
   * Once initialized, the &event struct can be used repeatedly with
   * event_add() and event_del() and does not need to be reinitialized unless
   * the eventHandler and/or the argument to it are to be changed.  However,
   * when an ev structure has been added to libevent using event_add() the
   * structure must persist until the event occurs (assuming EV_PERSIST
   * is not set) or is removed using event_del().  You may not reuse the same
   * ev structure for multiple monitored descriptors; each descriptor needs
   * its own ev.
   */
  event_set(&event_, tSocket_->getSocketFD(), eventFlags_, TConnection::eventHandler, this);
  event_base_set(ioThread_->getEventBase(), &event_);

  // Add the event
  if (event_add(&event_, 0) == -1) {
    GlobalOutput.perror("TConnection::setFlags(): could not event_add", THRIFT_GET_SOCKET_ERROR);
  }
}

/**
 * Closes a connection
 */
void CustomNonblockingServer::TConnection::close() {
  setIdle();

  if (serverEventHandler_) {
    serverEventHandler_->deleteContext(connectionContext_, inputProtocol_, outputProtocol_);
  }
  ioThread_ = NULL;

  // Close the socket
  tSocket_->close();

  // close any factory produced transports
  factoryInputTransport_->close();
  factoryOutputTransport_->close();

  // release processor and handler
  processor_.reset();

  // Give this object back to the server that owns it
  server_->returnConnection(this);
}

void CustomNonblockingServer::TConnection::checkIdleBufferMemLimit(size_t readLimit, size_t writeLimit) {
  if (readLimit > 0 && readBufferSize_ > readLimit) {
    free(readBuffer_);
    readBuffer_ = NULL;
    readBufferSize_ = 0;
  }

  if (writeLimit > 0 && largestWriteBufferSize_ > writeLimit) {
    // just start over
    outputTransport_->resetBuffer(static_cast<uint32_t>(server_->getWriteBufferDefaultSize()));
    largestWriteBufferSize_ = 0;
  }
}

CustomNonblockingServer::~CustomNonblockingServer() {
  // Close any active connections (moves them to the idle connection stack)
  while (activeConnections_.size()) {
    activeConnections_.front()->close();
  }
  // Clean up unused TConnection objects in connectionStack_
  while (!connectionStack_.empty()) {
    TConnection* connection = connectionStack_.top();
    connectionStack_.pop();
    delete connection;
  }
  // The TNonblockingIOThread objects have shared_ptrs to the Thread
  // objects and the Thread objects have shared_ptrs to the TNonblockingIOThread
  // objects (as runnable) so these objects will never deallocate without help.
  while (!ioThreads_.empty()) {
    std::shared_ptr<TNonblockingIOThread> iot = ioThreads_.back();
    ioThreads_.pop_back();
    iot->setThread(std::shared_ptr<Thread>());
  }
}

/**
 * Creates a new connection either by reusing an object off the stack or
 * by allocating a new one entirely
 */
CustomNonblockingServer::TConnection* CustomNonblockingServer::createConnection(std::shared_ptr<TSocket> socket) {
  // Check the stack
  Guard g(connMutex_);

  // pick an IO thread to handle this connection -- currently round robin
  assert(nextIOThread_ < ioThreads_.size());
  int selectedThreadIdx = nextIOThread_;
  nextIOThread_ = static_cast<uint32_t>((nextIOThread_ + 1) % ioThreads_.size());

  TNonblockingIOThread* ioThread = ioThreads_[selectedThreadIdx].get();

  // Check the connection stack to see if we can re-use
  TConnection* result = NULL;
  if (connectionStack_.empty()) {
    result = new TConnection(socket, ioThread);
    ++numTConnections_;
  } else {
    result = connectionStack_.top();
    connectionStack_.pop();
    result->setSocket(socket);
    result->init(ioThread);
  }
  activeConnections_.push_back(result);
  return result;
}

/**
 * Returns a connection to the stack
 */
void CustomNonblockingServer::returnConnection(TConnection* connection) {
  Guard g(connMutex_);

  activeConnections_.erase(std::remove(activeConnections_.begin(),
                                       activeConnections_.end(),
                                       connection),
                           activeConnections_.end());

  if (connectionStackLimit_ && (connectionStack_.size() >= connectionStackLimit_)) {
    delete connection;
    --numTConnections_;
  } else {
    connection->checkIdleBufferMemLimit(idleReadBufferLimit_, idleWriteBufferLimit_);
    connectionStack_.push(connection);
  }
}

/**
 * Server socket had something happen.  We accept all waiting client
 * connections on fd and assign TConnection objects to handle those requests.
 */
void CustomNonblockingServer::handleEvent(THRIFT_SOCKET fd, short which) {
  (void)which;
  // Make sure that libevent didn't mess up the socket handles
  assert(fd == serverSocket_);

  // Going to accept a new client socket
  std::shared_ptr<TSocket> clientSocket;

  clientSocket = serverTransport_->accept();
  if (clientSocket) {
    // If we're overloaded, take action here
    if (overloadAction_ != T_OVERLOAD_NO_ACTION && serverOverloaded()) {
      Guard g(connMutex_);
      nConnectionsDropped_++;
      nTotalConnectionsDropped_++;
      if (overloadAction_ == T_OVERLOAD_CLOSE_ON_ACCEPT) {
        clientSocket->close();
        return;
      } else if (overloadAction_ == T_OVERLOAD_DRAIN_TASK_QUEUE) {
        if (!drainPendingTask()) {
          // Nothing left to discard, so we drop connection instead.
          clientSocket->close();
          return;
        }
      }
    }

    // Create a new TConnection for this client socket.
    TConnection* clientConnection = createConnection(clientSocket);

    // Fail fast if we could not create a TConnection object
    if (clientConnection == NULL) {
      GlobalOutput.printf("thriftServerEventHandler: failed TConnection factory");
      clientSocket->close();
      return;
    }

    /*
     * Either notify the ioThread that is assigned this connection to
     * start processing, or if it is us, we'll just ask this
     * connection to do its initial state change here.
     *
     * (We need to avoid writing to our own notification pipe, to
     * avoid possible deadlocks if the pipe is full.)
     *
     * The IO thread #0 is the only one that handles these listen
     * events, so unless the connection has been assigned to thread #0
     * we know it's not on our thread.
     */
    if (clientConnection->getIOThreadNumber() == 0) {
      clientConnection->transition();
    } else {
      if (!clientConnection->notifyIOThread()) {
        GlobalOutput.perror("[ERROR] notifyIOThread failed on fresh connection, closing", errno);
        clientConnection->close();
      }
    }
  }
}

/**
 * Creates a socket to listen on and binds it to the local port.
 */
void CustomNonblockingServer::createAndListenOnSocket() {
  serverTransport_->listen();
  serverSocket_ = serverTransport_->getSocketFD();
}



void CustomNonblockingServer::setThreadManager(std::shared_ptr<CustomThreadManager> threadManager) {
  threadManager_ = threadManager;
  if (threadManager) {
    threadManager->setExpireCallback(
        std::bind(&CustomNonblockingServer::expireClose,
                                     this,
                                     std::placeholders::_1));
    threadPoolProcessing_ = true;
  } else {
    threadPoolProcessing_ = false;
  }
}

bool CustomNonblockingServer::changeIOCpuset(const std::vector<int>& cpuIds) {
    if (ioThreads_.empty()) {
        return false;
    }

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    
    // Add each CPU ID to the set
    for (int cpu : cpuIds) {
        CPU_SET(cpu, &cpuset);
    }

    // Update thread factory's CPU affinity if it exists
    if (ioThreadFactory_) {
        std::shared_ptr<CustomThreadFactory> customFactory = 
            std::dynamic_pointer_cast<CustomThreadFactory>(ioThreadFactory_);
        if (customFactory) {
            customFactory->changeCpuset(&cpuset);
        }
    }

    // Update each IO thread's CPU affinity
    for (auto& ioThread : ioThreads_) {
        if (ioThread && ioThread->getThread()) {
            std::shared_ptr<PthreadThread> pthreadThread = 
                std::dynamic_pointer_cast<PthreadThread>(ioThread->getThread());
            if (pthreadThread) {
                pthreadThread->changeCpuset(&cpuset);
            }
        }
    }

    return true;
}

bool CustomNonblockingServer::serverOverloaded() {
  size_t activeConnections = numTConnections_ - connectionStack_.size();
  if (numActiveProcessors_ > maxActiveProcessors_ || activeConnections > maxConnections_) {
    if (!overloaded_) {
      GlobalOutput.printf("CustomNonblockingServer: overload condition begun.");
      overloaded_ = true;
    }
  } else {
    if (overloaded_ && (numActiveProcessors_ <= overloadHysteresis_ * maxActiveProcessors_)
        && (activeConnections <= overloadHysteresis_ * maxConnections_)) {
      GlobalOutput.printf(
          "CustomNonblockingServer: overload ended; "
          "%u dropped (%llu total)",
          nConnectionsDropped_,
          nTotalConnectionsDropped_);
      nConnectionsDropped_ = 0;
      overloaded_ = false;
    }
  }

  return overloaded_;
}

bool CustomNonblockingServer::drainPendingTask() {
  if (threadManager_) {
    std::shared_ptr<Runnable> task = threadManager_->removeNextPending();
    if (task) {
      TConnection* connection = static_cast<TConnection::Task*>(task.get())->getTConnection();
      assert(connection && connection->getServer() && connection->getState() == APP_WAIT_TASK);
      connection->forceClose();
      return true;
    }
  }
  return false;
}

void CustomNonblockingServer::expireClose(std::shared_ptr<Runnable> task) {
  TConnection* connection = static_cast<TConnection::Task*>(task.get())->getTConnection();
  assert(connection && connection->getServer() && connection->getState() == APP_WAIT_TASK);
  connection->forceClose();
}

void CustomNonblockingServer::stop() {
  // Breaks the event loop in all threads so that they end ASAP.
  for (uint32_t i = 0; i < ioThreads_.size(); ++i) {
    ioThreads_[i]->stop();
  }
}

void CustomNonblockingServer::registerEvents(event_base* user_event_base) {
  userEventBase_ = user_event_base;

  // init listen socket
  if (serverSocket_ == THRIFT_INVALID_SOCKET)
    createAndListenOnSocket();

  // set up the IO threads
  assert(ioThreads_.empty());
  if (!numIOThreads_) {
    numIOThreads_ = DEFAULT_IO_THREADS;
  }
  // User-provided event-base doesn't works for multi-threaded servers
  assert(numIOThreads_ == 1 || !userEventBase_);

  for (uint32_t id = 0; id < numIOThreads_; ++id) {
    // the first IO thread also does the listening on server socket
    THRIFT_SOCKET listenFd = (id == 0 ? serverSocket_ : THRIFT_INVALID_SOCKET);

    shared_ptr<TNonblockingIOThread> thread(
        new TNonblockingIOThread(this, id, listenFd, useHighPriorityIOThreads_));
    ioThreads_.push_back(thread);
  }

  // Notify handler of the preServe event
  if (eventHandler_) {
    eventHandler_->preServe();
  }

  // Start all of our helper IO threads. Note that the threads run forever,
  // only terminating if stop() is called.
  assert(ioThreads_.size() == numIOThreads_);
  assert(ioThreads_.size() > 0);

  GlobalOutput.printf("CustomNonblockingServer: Serving with %d io threads.",
                      ioThreads_.size());

  // Launch all the secondary IO threads in separate threads
  if (ioThreads_.size() > 1) {

    if(ioThreadFactory_.get() == nullptr) {
      ioThreadFactory_.reset(new PlatformThreadFactory(
  #if !USE_BOOST_THREAD && !USE_STD_THREAD
          PlatformThreadFactory::OTHER,  // scheduler
          PlatformThreadFactory::NORMAL, // priority
          1,                             // stack size (MB)
  #endif
          false // detached
          ));
    }

    assert(ioThreadFactory_.get());

    // intentionally starting at thread 1, not 0
    for (uint32_t i = 1; i < ioThreads_.size(); ++i) {
      shared_ptr<Thread> thread = ioThreadFactory_->newThread(ioThreads_[i]);
      ioThreads_[i]->setThread(thread);
      thread->start();
    }
  }

  // Register the events for the primary (listener) IO thread
  ioThreads_[0]->registerEvents();
}

/**
 * Main workhorse function, starts up the server listening on a port and
 * loops over the libevent handler.
 */
void CustomNonblockingServer::serve() {

  // GlobalOutput.printf("Inside serve");
  if (ioThreads_.empty())
    registerEvents(NULL);

  // GlobalOutput.printf("Registered events");

  // Run the primary (listener) IO thread loop in our main thread; this will
  // only return when the server is shutting down.
  ioThreads_[0]->run();

  // GlobalOutput.printf("run() called");

  // Ensure all threads are finished before exiting serve()
  for (uint32_t i = 0; i < ioThreads_.size(); ++i) {
    ioThreads_[i]->join();
    GlobalOutput.printf("TNonblocking: join done for IO thread #%d", i);
  }
}

TNonblockingIOThread::TNonblockingIOThread(CustomNonblockingServer* server,
                                           int number,
                                           THRIFT_SOCKET listenSocket,
                                           bool useHighPriority)
  : server_(server),
    number_(number),
    listenSocket_(listenSocket),
    useHighPriority_(useHighPriority),
    eventBase_(NULL),
    ownEventBase_(false) {
  notificationPipeFDs_[0] = -1;
  notificationPipeFDs_[1] = -1;
}

TNonblockingIOThread::~TNonblockingIOThread() {
  // make sure our associated thread is fully finished
  join();

  if (eventBase_ && ownEventBase_) {
    event_base_free(eventBase_);
    ownEventBase_ = false;
  }

  if (listenSocket_ != THRIFT_INVALID_SOCKET) {
    if (0 != ::THRIFT_CLOSESOCKET(listenSocket_)) {
      GlobalOutput.perror("TNonblockingIOThread listenSocket_ close(): ", THRIFT_GET_SOCKET_ERROR);
    }
    listenSocket_ = THRIFT_INVALID_SOCKET;
  }

  for (int i = 0; i < 2; ++i) {
    if (notificationPipeFDs_[i] >= 0) {
      if (0 != ::THRIFT_CLOSESOCKET(notificationPipeFDs_[i])) {
        GlobalOutput.perror("TNonblockingIOThread notificationPipe close(): ",
                            THRIFT_GET_SOCKET_ERROR);
      }
      notificationPipeFDs_[i] = THRIFT_INVALID_SOCKET;
    }
  }
}

void TNonblockingIOThread::createNotificationPipe() {
  if (evutil_socketpair(AF_LOCAL, SOCK_STREAM, 0, notificationPipeFDs_) == -1) {
    GlobalOutput.perror("CustomNonblockingServer::createNotificationPipe ", EVUTIL_SOCKET_ERROR());
    throw TException("can't create notification pipe");
  }
  if (evutil_make_socket_nonblocking(notificationPipeFDs_[0]) < 0
      || evutil_make_socket_nonblocking(notificationPipeFDs_[1]) < 0) {
    ::THRIFT_CLOSESOCKET(notificationPipeFDs_[0]);
    ::THRIFT_CLOSESOCKET(notificationPipeFDs_[1]);
    throw TException("CustomNonblockingServer::createNotificationPipe() THRIFT_O_NONBLOCK");
  }
  for (int i = 0; i < 2; ++i) {
#if LIBEVENT_VERSION_NUMBER < 0x02000000
    int flags;
    if ((flags = THRIFT_FCNTL(notificationPipeFDs_[i], F_GETFD, 0)) < 0
        || THRIFT_FCNTL(notificationPipeFDs_[i], F_SETFD, flags | FD_CLOEXEC) < 0) {
#else
    if (evutil_make_socket_closeonexec(notificationPipeFDs_[i]) < 0) {
#endif
      ::THRIFT_CLOSESOCKET(notificationPipeFDs_[0]);
      ::THRIFT_CLOSESOCKET(notificationPipeFDs_[1]);
      throw TException(
          "CustomNonblockingServer::createNotificationPipe() "
          "FD_CLOEXEC");
    }
  }
}

/**
 * Register the core libevent events onto the proper base.
 */
void TNonblockingIOThread::registerEvents() {
  threadId_ = Thread::get_current();

  assert(eventBase_ == 0);
  eventBase_ = getServer()->getUserEventBase();
  if (eventBase_ == NULL) {
    eventBase_ = event_base_new();
    ownEventBase_ = true;
  }

  // Print some libevent stats
  if (number_ == 0) {
    GlobalOutput.printf("CustomNonblockingServer: using libevent %s method %s",
                        event_get_version(),
                        event_base_get_method(eventBase_));
  }

  if (listenSocket_ != THRIFT_INVALID_SOCKET) {
    // Register the server event
    event_set(&serverEvent_,
              listenSocket_,
              EV_READ | EV_PERSIST,
              TNonblockingIOThread::listenHandler,
              server_);
    event_base_set(eventBase_, &serverEvent_);

    // Add the event and start up the server
    if (-1 == event_add(&serverEvent_, 0)) {
      throw TException(
          "CustomNonblockingServer::serve(): "
          "event_add() failed on server listen event");
    }
    GlobalOutput.printf("TNonblocking: IO thread #%d registered for listen.", number_);
  }

  createNotificationPipe();

  // Create an event to be notified when a task finishes
  event_set(&notificationEvent_,
            getNotificationRecvFD(),
            EV_READ | EV_PERSIST,
            TNonblockingIOThread::notifyHandler,
            this);

  // Attach to the base
  event_base_set(eventBase_, &notificationEvent_);

  // Add the event and start up the server
  if (-1 == event_add(&notificationEvent_, 0)) {
    throw TException(
        "CustomNonblockingServer::serve(): "
        "event_add() failed on task-done notification event");
  }
  GlobalOutput.printf("TNonblocking: IO thread #%d registered for notify.", number_);
}

bool TNonblockingIOThread::notify(CustomNonblockingServer::TConnection* conn) {
  THRIFT_SOCKET fd = getNotificationSendFD();
  if (fd < 0) {
    return false;
  }

  int ret = -1;
  long kSize = sizeof(conn);
  const char * pos = (const char *)const_cast_sockopt2(&conn);

#if defined(HAVE_POLL_H) || defined(HAVE_SYS_POLL_H)
  struct pollfd pfd = {fd, POLLOUT, 0};

  while (kSize > 0) {
    pfd.revents = 0;
    ret = poll(&pfd, 1, -1);
    if (ret < 0) {
      return false;
    } else if (ret == 0) {
      continue;
    }

    if (pfd.revents & POLLHUP || pfd.revents & POLLERR) {
      ::THRIFT_CLOSESOCKET(fd);
      return false;
    }

    if (pfd.revents & POLLOUT) {
      ret = send(fd, pos, kSize, 0);
      if (ret < 0) {
        if (errno == EAGAIN) {
          continue;
        }

        ::THRIFT_CLOSESOCKET(fd);
        return false;
      }

      kSize -= ret;
      pos += ret;
    }
  }
#else
  fd_set wfds, efds;

  while (kSize > 0) {
    FD_ZERO(&wfds);
    FD_ZERO(&efds);
    FD_SET(fd, &wfds);
    FD_SET(fd, &efds);
    ret = select(static_cast<int>(fd + 1), NULL, &wfds, &efds, NULL);
    if (ret < 0) {
      return false;
    } else if (ret == 0) {
      continue;
    }

    if (FD_ISSET(fd, &efds)) {
      ::THRIFT_CLOSESOCKET(fd);
      return false;
    }

    if (FD_ISSET(fd, &wfds)) {
      ret = send(fd, pos, kSize, 0);
      if (ret < 0) {
        if (errno == EAGAIN) {
          continue;
        }

        ::THRIFT_CLOSESOCKET(fd);
        return false;
      }

      kSize -= ret;
      pos += ret;
    }
  }
#endif

  return true;
}

/* static */
void TNonblockingIOThread::notifyHandler(evutil_socket_t fd, short which, void* v) {
  TNonblockingIOThread* ioThread = (TNonblockingIOThread*)v;
  assert(ioThread);
  (void)which;

  while (true) {
    CustomNonblockingServer::TConnection* connection = 0;
    const int kSize = sizeof(connection);
    long nBytes = recv(fd, cast_sockopt2(&connection), kSize, 0);
    if (nBytes == kSize) {
      if (connection == NULL) {
        // this is the command to stop our thread, exit the handler!
        ioThread->breakLoop(false);
        return;
      }
      connection->transition();
    } else if (nBytes > 0) {
      // throw away these bytes and hope that next time we get a solid read
      GlobalOutput.printf("notifyHandler: Bad read of %d bytes, wanted %d", nBytes, kSize);
      ioThread->breakLoop(true);
      return;
    } else if (nBytes == 0) {
      GlobalOutput.printf("notifyHandler: Notify socket closed!");
      ioThread->breakLoop(false);
      // exit the loop
      break;
    } else { // nBytes < 0
      if (THRIFT_GET_SOCKET_ERROR != THRIFT_EWOULDBLOCK
          && THRIFT_GET_SOCKET_ERROR != THRIFT_EAGAIN) {
        GlobalOutput.perror("TNonblocking: notifyHandler read() failed: ", THRIFT_GET_SOCKET_ERROR);
        ioThread->breakLoop(true);
        return;
      }
      // exit the loop
      break;
    }
  }
}

void TNonblockingIOThread::breakLoop(bool error) {
  if (error) {
    GlobalOutput.printf("CustomNonblockingServer: IO thread #%d exiting with error.", number_);
    // TODO: figure out something better to do here, but for now kill the
    // whole process.
    GlobalOutput.printf("CustomNonblockingServer: aborting process.");
    ::abort();
  }

  // If we're running in the same thread, we can't use the notify(0)
  // mechanism to stop the thread, but happily if we're running in the
  // same thread, this means the thread can't be blocking in the event
  // loop either.
  if (!Thread::is_current(threadId_)) {
    notify(NULL);
  } else {
    // cause the loop to stop ASAP - even if it has things to do in it
    event_base_loopbreak(eventBase_);
  }
}

void TNonblockingIOThread::setCurrentThreadHighPriority(bool value) {
#ifdef HAVE_SCHED_H
  // Start out with a standard, low-priority setup for the sched params.
  struct sched_param sp;
  bzero((void*)&sp, sizeof(sp));
  int policy = SCHED_OTHER;

  // If desired, set up high-priority sched params structure.
  if (value) {
    // FIFO scheduler, ranked above default SCHED_OTHER queue
    policy = SCHED_FIFO;
    // The priority only compares us to other SCHED_FIFO threads, so we
    // just pick a random priority halfway between min & max.
    const int priority = (sched_get_priority_max(policy) + sched_get_priority_min(policy)) / 2;

    sp.sched_priority = priority;
  }

  // Actually set the sched params for the current thread.
  if (0 == pthread_setschedparam(pthread_self(), policy, &sp)) {
    GlobalOutput.printf("TNonblocking: IO Thread #%d using high-priority scheduler!", number_);
  } else {
    GlobalOutput.perror("TNonblocking: pthread_setschedparam(): ", THRIFT_GET_SOCKET_ERROR);
  }
#else
  THRIFT_UNUSED_VARIABLE(value);
#endif
}

void TNonblockingIOThread::run() {
  if (eventBase_ == NULL) {
    registerEvents();
  }
  if (useHighPriority_) {
    setCurrentThreadHighPriority(true);
  }

  if (eventBase_ != NULL)
  {
    GlobalOutput.printf("CustomNonblockingServer: IO thread #%d entering loop...", number_);
    // Run libevent engine, never returns, invokes calls to eventHandler
    event_base_loop(eventBase_, 0);

    if (useHighPriority_) {
      setCurrentThreadHighPriority(false);
    }

    // cleans up our registered events
    cleanupEvents();
  }

  GlobalOutput.printf("CustomNonblockingServer: IO thread #%d run() done!", number_);
}

void TNonblockingIOThread::cleanupEvents() {
  // stop the listen socket, if any
  if (listenSocket_ != THRIFT_INVALID_SOCKET) {
    if (event_del(&serverEvent_) == -1) {
      GlobalOutput.perror("TNonblockingIOThread::stop() event_del: ", THRIFT_GET_SOCKET_ERROR);
    }
  }

  event_del(&notificationEvent_);
}

void TNonblockingIOThread::stop() {
  // This should cause the thread to fall out of its event loop ASAP.
  breakLoop(false);
}

void TNonblockingIOThread::join() {
  // If this was a thread created by a factory (not the thread that called
  // serve()), we join() it to make sure we shut down fully.
  if (thread_) {
    try {
      // Note that it is safe to both join() ourselves twice, as well as join
      // the current thread as the pthread implementation checks for deadlock.
      thread_->join();
    } catch (...) {
      // swallow everything
    }
  }
}
// }
// }
// } // apache::thrift::server

#endif // #ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_CUSTOMNONBLOCKINGSERVER_H_




