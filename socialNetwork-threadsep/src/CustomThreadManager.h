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

#ifndef _THRIFT_CONCURRENCY_CUSTOMTHREADMANAGER_H_
#define _THRIFT_CONCURRENCY_CUSTOMTHREADMANAGER_H_ 1

#include <sys/types.h>
#include <thrift/concurrency/Thread.h>
#include "CustomThreadFactory.h"
// #include <thrift/std.h>

// namespace apache {
// namespace thrift {
// namespace concurrency {

/**
 * Thread Pool Manager and related classes
 *
 * @version $Id:$
 */
class CustomThreadManager;

/**
 * CustomThreadManager class
 *
 * This class manages a pool of threads. It uses a ThreadFactory to create
 * threads. It never actually creates or destroys worker threads, rather
 * it maintains statistics on number of idle threads, number of active threads,
 * task backlog, and average wait and service times and informs the PoolPolicy
 * object bound to instances of this manager of interesting transitions. It is
 * then up the PoolPolicy object to decide if the thread pool size needs to be
 * adjusted and call this object addWorker and removeWorker methods to make
 * changes.
 *
 * This design allows different policy implementations to use this code to
 * handle basic worker thread management and worker task execution and focus on
 * policy issues. The simplest policy, StaticPolicy, does nothing other than
 * create a fixed number of threads.
 */
class CustomThreadManager {

protected:
  CustomThreadManager() {}

public:
  typedef std::function<void(std::shared_ptr<apache::thrift::concurrency::Runnable>)> ExpireCallback;

  virtual ~CustomThreadManager() {}

  /**
   * Starts the thread manager. Verifies all attributes have been properly
   * initialized, then allocates necessary resources to begin operation
   */
  virtual void start() = 0;

  /**
   * Stops the thread manager. Aborts all remaining unprocessed task, shuts
   * down all created worker threads, and releases all allocated resources.
   * This method blocks for all worker threads to complete, thus it can
   * potentially block forever if a worker thread is running a task that
   * won't terminate.
   *
   * Worker threads will be joined depending on the threadFactory's detached
   * disposition.
   */
  virtual void stop() = 0;

  enum STATE { UNINITIALIZED, STARTING, STARTED, JOINING, STOPPING, STOPPED };

  virtual STATE state() const = 0;

  /**
   * \returns the current thread factory
   */
  virtual std::shared_ptr<CustomThreadFactory> threadFactory() const = 0;

  /**
   * Set the thread factory.
   * \throws apache::thrift::concurrency::InvalidArgumentException if the new thread factory has a different
   *                                  detached disposition than the one replacing it
   */
  virtual void threadFactory(std::shared_ptr<CustomThreadFactory> value) = 0;

  /**
   * Adds worker thread(s).
   */
  virtual void addWorker(size_t value = 1) = 0;


  virtual void changeCpuset(cpu_set_t *cpuset) = 0;
  // void CustomThreadManager::Impl::changeCpuset(cpu_set_t *cpuset) {
  //     threadFactory_->changeCpuset(cpuset);
  //     for (const auto& worker : workers_) {
  //         worker->changeCpuset(cpuset);
  //     }
  //     cpuset_ = cpuset;
  // }
  /**
   * Removes worker thread(s).
   * Threads are joined if the thread factory detached disposition allows it.
   * Blocks until the number of worker threads reaches the new limit.
   * \param[in]  value  the number to remove
   * \throws apache::thrift::concurrency::InvalidArgumentException if the value is greater than the number
   *                                  of workers
   */
  virtual void removeWorker(size_t value = 1) = 0;

  /**
   * Gets the current number of idle worker threads
   */
  virtual size_t idleWorkerCount() const = 0;

  /**
   * Gets the current number of total worker threads
   */
  virtual size_t workerCount() const = 0;

  /**
   * Gets the current number of pending tasks
   */
  virtual size_t pendingTaskCount() const = 0;

  /**
   * Gets the current number of pending and executing tasks
   */
  virtual size_t totalTaskCount() const = 0;

  /**
   * Gets the maximum pending task count.  0 indicates no maximum
   */
  virtual size_t pendingTaskCountMax() const = 0;

  /**
   * Gets the number of tasks which have been expired without being run
   * since start() was called.
   */
  virtual size_t expiredTaskCount() = 0;

  /**
   * Adds a task to be executed at some time in the future by a worker thread.
   *
   * This method will block if pendingTaskCountMax() in not zero and pendingTaskCount()
   * is greater than or equalt to pendingTaskCountMax().  If this method is called in the
   * context of a CustomThreadManager worker thread it will throw a
   * apache::thrift::concurrency::TooManyPendingTasksException
   *
   * @param task  The task to queue for execution
   *
   * @param timeout Time to wait in milliseconds to add a task when a pending-task-count
   * is specified. Specific cases:
   * timeout = 0  : Wait forever to queue task.
   * timeout = -1 : Return immediately if pending task count exceeds specified max
   * @param expiration when nonzero, the number of milliseconds the task is valid
   * to be run; if exceeded, the task will be dropped off the queue and not run.
   *
   * @throws apache::thrift::concurrency::TooManyPendingTasksException Pending task count exceeds max pending task count
   */
  virtual void add(std::shared_ptr<apache::thrift::concurrency::Runnable> task,
                   int64_t timeout = 0LL,
                   int64_t expiration = 0LL) = 0;

  /**
   * Removes a pending task
   */
  virtual void remove(std::shared_ptr<apache::thrift::concurrency::Runnable> task) = 0;

  /**
   * Remove the next pending task which would be run.
   *
   * @return the task removed.
   */
  virtual std::shared_ptr<apache::thrift::concurrency::Runnable> removeNextPending() = 0;

  /**
   * Remove tasks from front of task queue that have expired.
   */
  virtual void removeExpiredTasks() = 0;

  /**
   * Set a callback to be called when a task is expired and not run.
   *
   * @param expireCallback a function called with the shared_ptr<apache::thrift::concurrency::Runnable> for
   * the expired task.
   */
  virtual void setExpireCallback(ExpireCallback expireCallback) = 0;

  static std::shared_ptr<CustomThreadManager> newCustomThreadManager();

  /**
   * Creates a simple thread manager the uses count number of worker threads and has
   * a pendingTaskCountMax maximum pending tasks. The default, 0, specified no limit
   * on pending tasks
   */
  static std::shared_ptr<CustomThreadManager> newSimpleCustomThreadManager(size_t count = 4,
                                                                 size_t pendingTaskCountMax = 0);

  class Task;

  class Worker;

  class Impl;
};
// }
// }
// } // apache::thrift::concurrency





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

#include <thrift/thrift-config.h>

// #include <thrift/concurrency/CustomThreadManager.h>
#include <thrift/concurrency/Exception.h>
#include <thrift/concurrency/Monitor.h>
#include <thrift/concurrency/Util.h>
// #include <thrift/concurrency/ThreadFactory.h>
// #include <thrift/concurrency/PlatformThreadFactory.h>
// #include <thrift/concurrency/Thread.h>

// #include <thrift/std.h>

#include <stdexcept>
#include <deque>
#include <set>

// namespace apache {
// namespace thrift {
// namespace concurrency {

using std::shared_ptr;
using std::dynamic_pointer_cast;

/**
 * CustomThreadManager class
 *
 * This class manages a pool of threads. It uses a ThreadFactory to create
 * threads.  It never actually creates or destroys worker threads, rather
 * it maintains statistics on number of idle threads, number of active threads,
 * task backlog, and average wait and service times.
 *
 * There are three different monitors used for signaling different conditions
 * however they all share the same mutex_.
 *
 * @version $Id:$
 */
class CustomThreadManager::Impl : public CustomThreadManager {

public:
  Impl()
    : workerCount_(0),
      workerMaxCount_(0),
      idleCount_(0),
      pendingTaskCountMax_(0),
      expiredCount_(0),
      state_(CustomThreadManager::UNINITIALIZED),
      monitor_(&mutex_),
      maxMonitor_(&mutex_),
      workerMonitor_(&mutex_),
      cpuset_(0) {}

  ~Impl() { stop(); }

  void start();
  void stop();

  CustomThreadManager::STATE state() const { return state_; }

  std::shared_ptr<CustomThreadFactory> threadFactory() const {
    apache::thrift::concurrency::Guard g(mutex_);
    return threadFactory_;
  }

  void threadFactory(std::shared_ptr<CustomThreadFactory> value) {
    apache::thrift::concurrency::Guard g(mutex_);
    if (threadFactory_ && threadFactory_->isDetached() != value->isDetached()) {
      throw apache::thrift::concurrency::InvalidArgumentException();
    }
    threadFactory_ = value;
  }

  void addWorker(size_t value);

  void changeCpuset(cpu_set_t *cpuset);

  void removeWorker(size_t value);

  size_t idleWorkerCount() const { return idleCount_; }

  size_t workerCount() const {
    apache::thrift::concurrency::Guard g(mutex_);
    return workerCount_;
  }

  size_t pendingTaskCount() const {
    apache::thrift::concurrency::Guard g(mutex_);
    return tasks_.size();
  }

  size_t totalTaskCount() const {
    apache::thrift::concurrency::Guard g(mutex_);
    return tasks_.size() + workerCount_ - idleCount_;
  }

  size_t pendingTaskCountMax() const {
    apache::thrift::concurrency::Guard g(mutex_);
    return pendingTaskCountMax_;
  }

  size_t expiredTaskCount() {
    apache::thrift::concurrency::Guard g(mutex_);
    return expiredCount_;
  }

  void pendingTaskCountMax(const size_t value) {
    apache::thrift::concurrency::Guard g(mutex_);
    pendingTaskCountMax_ = value;
  }

  void add(shared_ptr<apache::thrift::concurrency::Runnable> value, int64_t timeout, int64_t expiration);

  void remove(shared_ptr<apache::thrift::concurrency::Runnable> task);

  shared_ptr<apache::thrift::concurrency::Runnable> removeNextPending();

  void removeExpiredTasks() {
    removeExpired(false);
  }

  void setExpireCallback(ExpireCallback expireCallback);

  void setCpuset(const cpu_set_t* cpuset) {
    cpuset_ = cpuset;
  };

private:
  /**
   * Remove one or more expired tasks.
   * \param[in]  justOne  if true, try to remove just one task and return
   */
  void removeExpired(bool justOne);

  /**
   * \returns whether it is acceptable to block, depending on the current thread id
   */
  bool canSleep() const;

  /**
   * Lowers the maximum worker count and blocks until enough worker threads complete
   * to get to the new maximum worker limit.  The caller is responsible for acquiring
   * a lock on the class mutex_.
   */
  void removeWorkersUnderLock(size_t value);

  size_t workerCount_;
  size_t workerMaxCount_;
  size_t idleCount_;
  size_t pendingTaskCountMax_;
  size_t expiredCount_;
  ExpireCallback expireCallback_;
  const cpu_set_t *cpuset_;

  CustomThreadManager::STATE state_;
  std::shared_ptr<CustomThreadFactory> threadFactory_;

  friend class CustomThreadManager::Task;
  typedef std::deque<shared_ptr<Task> > TaskQueue;
  TaskQueue tasks_;
  apache::thrift::concurrency::Mutex mutex_;
  apache::thrift::concurrency::Monitor monitor_;
  apache::thrift::concurrency::Monitor maxMonitor_;
  apache::thrift::concurrency::Monitor workerMonitor_;       // used to synchronize changes in worker count

  friend class CustomThreadManager::Worker;
  std::set<shared_ptr<apache::thrift::concurrency::Thread> > workers_;
  std::set<shared_ptr<apache::thrift::concurrency::Thread> > deadWorkers_;
  std::map<const apache::thrift::concurrency::Thread::id_t, shared_ptr<apache::thrift::concurrency::Thread> > idMap_;

  
};

class CustomThreadManager::Task : public apache::thrift::concurrency::Runnable {

public:
  enum STATE { WAITING, EXECUTING, TIMEDOUT, COMPLETE };

  Task(shared_ptr<apache::thrift::concurrency::Runnable> runnable, int64_t expiration = 0LL)
    : runnable_(runnable),
      state_(WAITING),
      expireTime_(expiration != 0LL ? apache::thrift::concurrency::Util::currentTime() + expiration : 0LL) {}

  ~Task() {}

  void run() {
    if (state_ == EXECUTING) {
      runnable_->run();
      state_ = COMPLETE;
    }
  }

  shared_ptr<apache::thrift::concurrency::Runnable> getRunnable() { return runnable_; }

  int64_t getExpireTime() const { return expireTime_; }

private:
  shared_ptr<apache::thrift::concurrency::Runnable> runnable_;
  friend class CustomThreadManager::Worker;
  STATE state_;
  int64_t expireTime_;
};

class CustomThreadManager::Worker : public apache::thrift::concurrency::Runnable {
  enum STATE { UNINITIALIZED, STARTING, STARTED, STOPPING, STOPPED };

public:
  Worker(CustomThreadManager::Impl* manager) : manager_(manager), state_(UNINITIALIZED) {}

  ~Worker() {}

private:
  bool isActive() const {
    return (manager_->workerCount_ <= manager_->workerMaxCount_)
           || (manager_->state_ == JOINING && !manager_->tasks_.empty());
  }

public:
  /**
   * Worker entry point
   *
   * As long as worker thread is running, pull tasks off the task queue and
   * execute.
   */
  void run() {
    apache::thrift::concurrency::Guard g(manager_->mutex_);

    /**
     * This method has three parts; one is to check for and account for
     * admitting a task which happens under a lock.  Then the lock is released
     * and the task itself is executed.  Finally we do some accounting
     * under lock again when the task completes.
     */

    /**
     * Admitting
     */

    /**
     * Increment worker semaphore and notify manager if worker count reached
     * desired max
     */
    bool active = manager_->workerCount_ < manager_->workerMaxCount_;
    if (active) {
      if (++manager_->workerCount_ == manager_->workerMaxCount_) {
        manager_->workerMonitor_.notify();
      }
    }

    while (active) {
      /**
        * While holding manager monitor block for non-empty task queue (Also
        * check that the thread hasn't been requested to stop). Once the queue
        * is non-empty, dequeue a task, release monitor, and execute. If the
        * worker max count has been decremented such that we exceed it, mark
        * ourself inactive, decrement the worker count and notify the manager
        * (technically we're notifying the next blocked thread but eventually
        * the manager will see it.
        */
      active = isActive();

      while (active && manager_->tasks_.empty()) {
        manager_->idleCount_++;
        manager_->monitor_.wait();
        active = isActive();
        manager_->idleCount_--;
      }

      shared_ptr<CustomThreadManager::Task> task;

      if (active) {
        if (!manager_->tasks_.empty()) {
          task = manager_->tasks_.front();
          manager_->tasks_.pop_front();
          if (task->state_ == CustomThreadManager::Task::WAITING) {
            // If the state is changed to anything other than EXECUTING or TIMEDOUT here
            // then the execution loop needs to be changed below.
            task->state_ =
                (task->getExpireTime() && task->getExpireTime() < apache::thrift::concurrency::Util::currentTime()) ?
                    CustomThreadManager::Task::TIMEDOUT :
                    CustomThreadManager::Task::EXECUTING;
          }
        }

        /* If we have a pending task max and we just dropped below it, wakeup any
            thread that might be blocked on add. */
        if (manager_->pendingTaskCountMax_ != 0
            && manager_->tasks_.size() <= manager_->pendingTaskCountMax_ - 1) {
          manager_->maxMonitor_.notify();
        }
      }

      /**
       * Execution - not holding a lock
       */
      if (task) {
        if (task->state_ == CustomThreadManager::Task::EXECUTING) {

          // Release the lock so we can run the task without blocking the thread manager
          manager_->mutex_.unlock();

          try {
            task->run();
          } catch (const std::exception& e) {
            apache::thrift::GlobalOutput.printf("[ERROR] task->run() raised an exception: %s", e.what());
          } catch (...) {
            apache::thrift::GlobalOutput.printf("[ERROR] task->run() raised an unknown exception");
          }

          // Re-acquire the lock to proceed in the thread manager
          manager_->mutex_.lock();

        } else if (manager_->expireCallback_) {
          // The only other state the task could have been in is TIMEDOUT (see above)
          manager_->expireCallback_(task->getRunnable());
          manager_->expiredCount_++;
        }
      }
    }

    /**
     * Final accounting for the worker thread that is done working
     */
    manager_->deadWorkers_.insert(this->thread());
    if (--manager_->workerCount_ == manager_->workerMaxCount_) {
      manager_->workerMonitor_.notify();
    }
  }

private:
  CustomThreadManager::Impl* manager_;
  friend class CustomThreadManager::Impl;
  STATE state_;
};

void CustomThreadManager::Impl::changeCpuset(cpu_set_t *cpuset) {
    threadFactory_->changeCpuset(cpuset);
    for (const auto& worker : workers_) {
      std::shared_ptr<PthreadThread> pthreadWorker = std::dynamic_pointer_cast<PthreadThread>(worker);
      pthreadWorker->changeCpuset(cpuset);
    }
    cpuset_ = cpuset;
}

void CustomThreadManager::Impl::addWorker(size_t value) {
  std::set<shared_ptr<apache::thrift::concurrency::Thread> > newThreads;
  for (size_t ix = 0; ix < value; ix++) {
    shared_ptr<CustomThreadManager::Worker> worker
        = shared_ptr<CustomThreadManager::Worker>(new CustomThreadManager::Worker(this));
    newThreads.insert(threadFactory_->newThread(worker));
  }

  apache::thrift::concurrency::Guard g(mutex_);
  workerMaxCount_ += value;
  workers_.insert(newThreads.begin(), newThreads.end());

  for (std::set<shared_ptr<apache::thrift::concurrency::Thread> >::iterator ix = newThreads.begin(); ix != newThreads.end();
       ++ix) {
    shared_ptr<CustomThreadManager::Worker> worker
        = dynamic_pointer_cast<CustomThreadManager::Worker, apache::thrift::concurrency::Runnable>((*ix)->runnable());
    worker->state_ = CustomThreadManager::Worker::STARTING;
    (*ix)->start();
    idMap_.insert(std::pair<const apache::thrift::concurrency::Thread::id_t, shared_ptr<apache::thrift::concurrency::Thread> >((*ix)->getId(), *ix));
  }

  while (workerCount_ != workerMaxCount_) {
    workerMonitor_.wait();
  }
}

void CustomThreadManager::Impl::start() {
  apache::thrift::concurrency::Guard g(mutex_);
  if (state_ == CustomThreadManager::STOPPED) {
    return;
  }

  if (state_ == CustomThreadManager::UNINITIALIZED) {
    if (!threadFactory_) {
      throw apache::thrift::concurrency::InvalidArgumentException();
    }
    state_ = CustomThreadManager::STARTED;
    monitor_.notifyAll();
  }

  while (state_ == STARTING) {
    monitor_.wait();
  }
}

void CustomThreadManager::Impl::stop() {
  apache::thrift::concurrency::Guard g(mutex_);
  bool doStop = false;

  if (state_ != CustomThreadManager::STOPPING && state_ != CustomThreadManager::JOINING
      && state_ != CustomThreadManager::STOPPED) {
    doStop = true;
    state_ = CustomThreadManager::JOINING;
  }

  if (doStop) {
    removeWorkersUnderLock(workerCount_);
  }

  state_ = CustomThreadManager::STOPPED;
}

void CustomThreadManager::Impl::removeWorker(size_t value) {
  apache::thrift::concurrency::Guard g(mutex_);
  removeWorkersUnderLock(value);
}

void CustomThreadManager::Impl::removeWorkersUnderLock(size_t value) {
  if (value > workerMaxCount_) {
    throw apache::thrift::concurrency::InvalidArgumentException();
  }

  workerMaxCount_ -= value;

  if (idleCount_ > value) {
    // There are more idle workers than we need to remove,
    // so notify enough of them so they can terminate.
    for (size_t ix = 0; ix < value; ix++) {
      monitor_.notify();
    }
  } else {
    // There are as many or less idle workers than we need to remove,
    // so just notify them all so they can terminate.
    monitor_.notifyAll();
  }

  while (workerCount_ != workerMaxCount_) {
    workerMonitor_.wait();
  }

  for (std::set<shared_ptr<apache::thrift::concurrency::Thread> >::iterator ix = deadWorkers_.begin();
       ix != deadWorkers_.end();
       ++ix) {

    // when used with a joinable thread factory, we join the threads as we remove them
    if (!threadFactory_->isDetached()) {
      (*ix)->join();
    }

    idMap_.erase((*ix)->getId());
    std::shared_ptr<PthreadThread> pthreadThread = std::dynamic_pointer_cast<PthreadThread>(*ix);
    workers_.erase(pthreadThread);
  }

  deadWorkers_.clear();
}

bool CustomThreadManager::Impl::canSleep() const {
  const apache::thrift::concurrency::Thread::id_t id = threadFactory_->getCurrentThreadId();
  return idMap_.find(id) == idMap_.end();
}

void CustomThreadManager::Impl::add(shared_ptr<apache::thrift::concurrency::Runnable> value, int64_t timeout, int64_t expiration) {
  apache::thrift::concurrency::Guard g(mutex_, timeout);

  if (!g) {
    throw apache::thrift::concurrency::TimedOutException();
  }

  if (state_ != CustomThreadManager::STARTED) {
    throw apache::thrift::concurrency::IllegalStateException(
        "CustomThreadManager::Impl::add CustomThreadManager "
        "not started");
  }

  // if we're at a limit, remove an expired task to see if the limit clears
  if (pendingTaskCountMax_ > 0 && (tasks_.size() >= pendingTaskCountMax_)) {
    removeExpired(true);
  }

  if (pendingTaskCountMax_ > 0 && (tasks_.size() >= pendingTaskCountMax_)) {
    if (canSleep() && timeout >= 0) {
      while (pendingTaskCountMax_ > 0 && tasks_.size() >= pendingTaskCountMax_) {
        // This is thread safe because the mutex is shared between monitors.
        maxMonitor_.wait(timeout);
      }
    } else {
      throw apache::thrift::concurrency::TooManyPendingTasksException();
    }
  }

  tasks_.push_back(shared_ptr<CustomThreadManager::Task>(new CustomThreadManager::Task(value, expiration)));

  // If idle thread is available notify it, otherwise all worker threads are
  // running and will get around to this task in time.
  if (idleCount_ > 0) {
    monitor_.notify();
  }
}

void CustomThreadManager::Impl::remove(shared_ptr<apache::thrift::concurrency::Runnable> task) {
  apache::thrift::concurrency::Guard g(mutex_);
  if (state_ != CustomThreadManager::STARTED) {
    throw apache::thrift::concurrency::IllegalStateException(
        "CustomThreadManager::Impl::remove CustomThreadManager not "
        "started");
  }

  for (TaskQueue::iterator it = tasks_.begin(); it != tasks_.end(); ++it)
  {
    if ((*it)->getRunnable() == task)
    {
      tasks_.erase(it);
      return;
    }
  }
}

std::shared_ptr<apache::thrift::concurrency::Runnable> CustomThreadManager::Impl::removeNextPending() {
  apache::thrift::concurrency::Guard g(mutex_);
  if (state_ != CustomThreadManager::STARTED) {
    throw apache::thrift::concurrency::IllegalStateException(
        "CustomThreadManager::Impl::removeNextPending "
        "CustomThreadManager not started");
  }

  if (tasks_.empty()) {
    return std::shared_ptr<apache::thrift::concurrency::Runnable>();
  }

  shared_ptr<CustomThreadManager::Task> task = tasks_.front();
  tasks_.pop_front();

  return task->getRunnable();
}

void CustomThreadManager::Impl::removeExpired(bool justOne) {
  // this is always called under a lock
  int64_t now = 0LL;

  for (TaskQueue::iterator it = tasks_.begin(); it != tasks_.end(); )
  {
    if (now == 0LL) {
      now = apache::thrift::concurrency::Util::currentTime();
    }

    if ((*it)->getExpireTime() > 0LL && (*it)->getExpireTime() < now) {
      if (expireCallback_) {
        expireCallback_((*it)->getRunnable());
      }
      it = tasks_.erase(it);
      ++expiredCount_;
      if (justOne) {
        return;
      }
    }
    else
    {
      ++it;
    }
  }
}

void CustomThreadManager::Impl::setExpireCallback(ExpireCallback expireCallback) {
  apache::thrift::concurrency::Guard g(mutex_);
  expireCallback_ = expireCallback;
}

class SimpleCustomThreadManager : public CustomThreadManager::Impl {

public:
  SimpleCustomThreadManager(size_t workerCount = 4, size_t pendingTaskCountMax = 0, const cpu_set_t *cpuset = 0)
    : workerCount_(workerCount), pendingTaskCountMax_(pendingTaskCountMax) {
      // setCpuset(cpuset);
    }

  void start() {
    CustomThreadManager::Impl::pendingTaskCountMax(pendingTaskCountMax_);
    CustomThreadManager::Impl::start();
    addWorker(workerCount_);
  }

private:
  const size_t workerCount_;
  const size_t pendingTaskCountMax_;
  // const cpu_set_t* cpuset_;
};

shared_ptr<CustomThreadManager> CustomThreadManager::newCustomThreadManager() {
  return shared_ptr<CustomThreadManager>(new CustomThreadManager::Impl());
}

shared_ptr<CustomThreadManager> CustomThreadManager::newSimpleCustomThreadManager(size_t count,
                                                                size_t pendingTaskCountMax) {
  return shared_ptr<CustomThreadManager>(new SimpleCustomThreadManager(count, pendingTaskCountMax));
}
// }
// }
// } // apache::thrift::concurrency



#endif // #ifndef _THRIFT_CONCURRENCY_CUSTOMTHREADMANAGER_H_