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

#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_CUSTOMTHREADFACTORY_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_CUSTOMTHREADFACTORY_H_

#include <thrift/concurrency/Thread.h>

/**
 * A thread factory to create posix threads
 *
 * @version $Id:$
 */
class CustomThreadFactory : public apache::thrift::concurrency::ThreadFactory {

public:
  /**
   * POSIX Thread scheduler policies
   */
  enum POLICY { OTHER, FIFO, ROUND_ROBIN };

  /**
   * POSIX Thread scheduler relative priorities,
   *
   * Absolute priority is determined by scheduler policy and OS. This
   * enumeration specifies relative priorities such that one can specify a
   * priority within a giving scheduler policy without knowing the absolute
   * value of the priority.
   */
  enum PRIORITY {
    LOWEST = 0,
    LOWER = 1,
    LOW = 2,
    NORMAL = 3,
    HIGH = 4,
    HIGHER = 5,
    HIGHEST = 6,
    INCREMENT = 7,
    DECREMENT = 8
  };

  /**
   * Posix thread (pthread) factory.  All threads created by a factory are reference-counted
   * via std::shared_ptr.  The factory guarantees that threads and the Runnable tasks 
   * they host will be properly cleaned up once the last strong reference to both is
   * given up.
   *
   * Threads are created with the specified policy, priority, stack-size and detachable-mode
   * detached means the thread is free-running and will release all system resources the
   * when it completes.  A detachable thread is not joinable.  The join method
   * of a detachable thread will return immediately with no error.
   *
   * By default threads are not joinable.
   */
  CustomThreadFactory(POLICY policy = ROUND_ROBIN,
                     PRIORITY priority = NORMAL,
                     int stackSize = 1,
                     bool detached = true,
                     const cpu_set_t *cpuset = NULL);

  /**
   * Provide a constructor compatible with the other factories
   * The default policy is POLICY::ROUND_ROBIN.
   * The default priority is PRIORITY::NORMAL.
   * The default stackSize is 1.
   */
  CustomThreadFactory(bool detached, const cpu_set_t *cpuset);

  // From ThreadFactory;
  std::shared_ptr<apache::thrift::concurrency::Thread> newThread(std::shared_ptr<apache::thrift::concurrency::Runnable> runnable) const;

  // From ThreadFactory;
  apache::thrift::concurrency::Thread::id_t getCurrentThreadId() const;

  /**
   * Gets stack size for newly created threads
   *
   * @return int size in megabytes
   */
  virtual int getStackSize() const;

  /**
   * Sets stack size for newly created threads
   *
   * @param value size in megabytes
   */
  virtual void setStackSize(int value);

  /**
   * Gets priority relative to current policy
   */
  virtual PRIORITY getPriority() const;

  /**
   * Sets priority relative to current policy
   */
  virtual void setPriority(PRIORITY priority);

  void changeCpuset(const cpu_set_t *cpuset);

private:
  POLICY policy_;
  PRIORITY priority_;
  int stackSize_;
  const cpu_set_t *cpuset_;
};


/**
 * The POSIX thread class.
 *
 * @version $Id:$
 */
class PthreadThread : public apache::thrift::concurrency::Thread {
public:
  enum STATE { uninitialized, starting, started, stopping, stopped };

  static const int MB = 1024 * 1024;

  static void* threadMain(void* arg);
  void changeCpuset(const cpu_set_t *cpuset);

private:
  pthread_t pthread_;
  apache::thrift::concurrency::Monitor monitor_;		// guard to protect state_ and also notification
  STATE state_;         // to protect proper thread start behavior
  int policy_;
  int priority_;
  int stackSize_;
  std::weak_ptr<PthreadThread> self_;
  bool detached_;
  const cpu_set_t *cpuset_;

public:
  PthreadThread(int policy,
                int priority,
                int stackSize,
                bool detached,
                std::shared_ptr<apache::thrift::concurrency::Runnable> runnable,
                const cpu_set_t *cpuset)
    :

#ifndef _WIN32
      pthread_(0),
#endif // _WIN32
      state_(uninitialized),
      policy_(policy),
      priority_(priority),
      stackSize_(stackSize),
      detached_(detached),
      cpuset_(cpuset) {

    this->Thread::runnable(runnable);

    // cpu_set_t cpuset;
    // CPU_ZERO(&cpuset);
    // for (int i = 0; i <= 10; ++i) {
    //     CPU_SET(i, &cpuset);
    // }
    // pthread_setaffinity_np(this->pthread_, sizeof(cpu_set_t), &cpuset);
  }

  ~PthreadThread() {
    /* Nothing references this thread, if is is not detached, do a join
       now, otherwise the thread-id and, possibly, other resources will
       be leaked. */
    if (!detached_) {
      try {
        join();
      } catch (...) {
        // We're really hosed.
      }
    }
  }

  STATE getState() const
  {
    apache::thrift::concurrency::Synchronized sync(monitor_);
    return state_;
  }

  void setState(STATE newState)
  {
    apache::thrift::concurrency::Synchronized sync(monitor_);
    state_ = newState;

    // unblock start() with the knowledge that the thread has actually
    // started running, which avoids a race in detached threads.
    if (newState == started) {
	  monitor_.notify();
    }
  }




  void start() {
    // std::cout << "start" << std::endl;
    if (getState() != uninitialized) {
      return;
    }

    pthread_attr_t thread_attr;
    if (pthread_attr_init(&thread_attr) != 0) {
      throw apache::thrift::concurrency::SystemResourceException("pthread_attr_init failed");
    }

    if (pthread_attr_setdetachstate(&thread_attr,
                                    detached_ ? PTHREAD_CREATE_DETACHED : PTHREAD_CREATE_JOINABLE)
        != 0) {
      throw apache::thrift::concurrency::SystemResourceException("pthread_attr_setdetachstate failed");
    }

    // Set thread stack size
    if (pthread_attr_setstacksize(&thread_attr, MB * stackSize_) != 0) {
      throw apache::thrift::concurrency::SystemResourceException("pthread_attr_setstacksize failed");
    }

// Set thread policy
#ifdef _WIN32
    // WIN32 Pthread implementation doesn't seem to support sheduling policies other then
    // CustomThreadFactory::OTHER - runtime error
    policy_ = CustomThreadFactory::OTHER;
#endif

#if _POSIX_THREAD_PRIORITY_SCHEDULING > 0
    if (pthread_attr_setschedpolicy(&thread_attr, policy_) != 0) {
      throw apache::thrift::concurrency::SystemResourceException("pthread_attr_setschedpolicy failed");
    }
#endif

    struct sched_param sched_param;
    sched_param.sched_priority = priority_;

    // Set thread priority
    if (pthread_attr_setschedparam(&thread_attr, &sched_param) != 0) {
      throw apache::thrift::concurrency::SystemResourceException("pthread_attr_setschedparam failed");
    }

    // Create reference
    std::shared_ptr<PthreadThread>* selfRef = new std::shared_ptr<PthreadThread>();
    *selfRef = self_.lock();

    setState(starting);

	  apache::thrift::concurrency::Synchronized sync(monitor_);
	
    if (pthread_create(&pthread_, &thread_attr, threadMain, (void*)selfRef) != 0) {
      throw apache::thrift::concurrency::SystemResourceException("pthread_create failed");
    }
  
    // cpu_set_t cpuset;
    // CPU_ZERO(&cpuset);
    // for (int i = 0; i <= 7; ++i) {
    //     CPU_SET(i, &cpuset);
    // }

    pthread_setaffinity_np(pthread_, sizeof(cpu_set_t), cpuset_);

    // The caller may not choose to guarantee the scope of the Runnable
    // being used in the thread, so we must actually wait until the thread
    // starts before we return.  If we do not wait, it would be possible
    // for the caller to start destructing the Runnable and the Thread,
    // and we would end up in a race.  This was identified with valgrind.
    monitor_.wait();
    // std::cout << "start exit" << std::endl;
  }

  void join() {
    if (!detached_ && getState() != uninitialized) {
      void* ignore;
      /* XXX
         If join fails it is most likely due to the fact
         that the last reference was the thread itself and cannot
         join.  This results in leaked threads and will eventually
         cause the process to run out of thread resources.
         We're beyond the point of throwing an exception.  Not clear how
         best to handle this. */
      int res = pthread_join(pthread_, &ignore);
      detached_ = (res == 0);
      if (res != 0) {
        apache::thrift::GlobalOutput.printf("PthreadThread::join(): fail with code %d", res);
      }
    }
  }

  // void pin(int min_core_num, int max_core_num) {
  //   cpu_set_t cpuset;
  //   CPU_ZERO(&cpuset);
  //   for (int i = min_core_num; i <= max_core_num; ++i) {
  //       CPU_SET(i, &cpuset);
  //   }
  //   // pthread_setaffinity_np(pthread_, sizeof(cpu_set_t), &cpuset);
  // }

  apache::thrift::concurrency::Thread::id_t getId() {

#ifndef _WIN32
    return (apache::thrift::concurrency::Thread::id_t)pthread_;
#else
    return (apache::thrift::concurrency::Thread::id_t)pthread_.p;
#endif // _WIN32
  }

  std::shared_ptr<apache::thrift::concurrency::Runnable> runnable() const { return apache::thrift::concurrency::Thread::runnable(); }

  void runnable(std::shared_ptr<apache::thrift::concurrency::Runnable> value) { apache::thrift::concurrency::Thread::runnable(value); }

  void weakRef(std::shared_ptr<PthreadThread> self) {
    assert(self.get() == this);
    self_ = std::weak_ptr<PthreadThread>(self);
  }
};


void PthreadThread::changeCpuset(const cpu_set_t* cpuset) {
  cpuset_ = cpuset;
  int num_cpus = CPU_COUNT(cpuset_);
  std::cout << "Number of CPUs in the set: " << num_cpus << std::endl;
  std::cout << "CPU IDs in the set: ";
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
      if (CPU_ISSET(cpu, cpuset_)) {
          std::cout << " - " <<  cpu << " - ";
      }
  }
  std::cout << std::endl;
  pthread_setaffinity_np(pthread_, sizeof(cpu_set_t), cpuset_);
}

void* PthreadThread::threadMain(void* arg) {
  // std::cout << "Called threadMain .... " << std::endl;
  std::shared_ptr<PthreadThread> thread = *(std::shared_ptr<PthreadThread>*)arg;
  delete reinterpret_cast<std::shared_ptr<PthreadThread>*>(arg);

#if GOOGLE_PERFTOOLS_REGISTER_THREAD
  ProfilerRegisterThread();
#endif

  thread->setState(started);

  thread->runnable()->run();

  STATE _s = thread->getState();
  if (_s != stopping && _s != stopped) {
    thread->setState(stopping);
  }

  // std::cout << "Exiting threadMain .... " << std::endl;
  return (void*)0;

}

/**
 * Converts generic posix thread schedule policy enums into pthread
 * API values.
 */
static int toPthreadPolicy(CustomThreadFactory::POLICY policy) {
  switch (policy) {
  case CustomThreadFactory::OTHER:
    return SCHED_OTHER;
  case CustomThreadFactory::FIFO:
    return SCHED_FIFO;
  case CustomThreadFactory::ROUND_ROBIN:
    return SCHED_RR;
  }
  return SCHED_OTHER;
}

/**
 * Converts relative thread priorities to absolute value based on posix
 * thread scheduler policy
 *
 *  The idea is simply to divide up the priority range for the given policy
 * into the correpsonding relative priority level (lowest..highest) and
 * then pro-rate accordingly.
 */
static int toPthreadPriority(CustomThreadFactory::POLICY policy, CustomThreadFactory::PRIORITY priority) {
  int pthread_policy = toPthreadPolicy(policy);
  int min_priority = 0;
  int max_priority = 0;
#ifdef HAVE_SCHED_GET_PRIORITY_MIN
  min_priority = sched_get_priority_min(pthread_policy);
#endif
#ifdef HAVE_SCHED_GET_PRIORITY_MAX
  max_priority = sched_get_priority_max(pthread_policy);
#endif
  int quanta = (CustomThreadFactory::HIGHEST - CustomThreadFactory::LOWEST) + 1;
  float stepsperquanta = (float)(max_priority - min_priority) / quanta;

  if (priority <= CustomThreadFactory::HIGHEST) {
    return (int)(min_priority + stepsperquanta * priority);
  } else {
    // should never get here for priority increments.
    assert(false);
    return (int)(min_priority + stepsperquanta * CustomThreadFactory::NORMAL);
  }
}

CustomThreadFactory::CustomThreadFactory(POLICY policy,
                                       PRIORITY priority,
                                       int stackSize,
                                       bool detached,
                                       const cpu_set_t *cpuset)
  : ThreadFactory(detached),
    policy_(policy),
    priority_(priority),
    stackSize_(stackSize),
    cpuset_(cpuset) {
}

CustomThreadFactory::CustomThreadFactory(bool detached, const cpu_set_t *cpuset)
  : ThreadFactory(detached),
    policy_(ROUND_ROBIN),
    priority_(NORMAL),
    stackSize_(1),
    cpuset_(cpuset) {
}

std::shared_ptr<apache::thrift::concurrency::Thread> CustomThreadFactory::newThread(std::shared_ptr<apache::thrift::concurrency::Runnable> runnable) const {
  // std::cout << "newThread called" << std::endl;
  std::shared_ptr<PthreadThread> result
      = std::shared_ptr<PthreadThread>(new PthreadThread(toPthreadPolicy(policy_),
                                                    toPthreadPriority(policy_, priority_),
                                                    stackSize_,
                                                    isDetached(),
                                                    runnable, cpuset_));
  result->weakRef(result);
  runnable->thread(result);
  return result;
}

int CustomThreadFactory::getStackSize() const {
  return stackSize_;
}

void CustomThreadFactory::setStackSize(int value) {
  stackSize_ = value;
}

CustomThreadFactory::PRIORITY CustomThreadFactory::getPriority() const {
  return priority_;
}

void CustomThreadFactory::setPriority(PRIORITY value) {
  priority_ = value;
}

void CustomThreadFactory::changeCpuset(const cpu_set_t* cpuset) {
  // cpuset_ = cpuset;
  CPU_ZERO(const_cast<cpu_set_t*>(cpuset_));
  int num_cpus = CPU_COUNT(cpuset);
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
      if (CPU_ISSET(cpu, cpuset)) {
          CPU_SET(cpu, cpuset_);
      }
  }
  // std::cout << std::endl;

  // int num_cpus = CPU_COUNT(cpuset_);
  // std::cout << "CC : Number of CPUs in the set: " << num_cpus << std::endl;
  // std::cout << "CC : CPU IDs in the set: ";
  // for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
  //     if (CPU_ISSET(cpu, cpuset_)) {
  //         std::cout << " # " <<  cpu << " # ";
  //     }
  // }
  // std::cout << std::endl;
  
}

apache::thrift::concurrency::Thread::id_t CustomThreadFactory::getCurrentThreadId() const {
#ifndef _WIN32
  return (apache::thrift::concurrency::Thread::id_t)pthread_self();
#else
  return (apache::thrift::concurrency::Thread::id_t)pthread_self().p;
#endif // _WIN32
}

#endif // #ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_CUSTOMTHREADFACTORY_H_