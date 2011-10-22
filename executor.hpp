#ifndef __EXECUTOR_H__
#define __EXECUTOR_H__

#include <cassert>
#include <iostream>
#include <vector>

#include <pthread.h>
#include <tbb/concurrent_queue.h>

namespace exec_service {

template <typename T>
class JobRef {
public:
  virtual T * ref() = 0;
};

template <typename T>
class JobRefContainer : public JobRef<T> {
public:
  JobRefContainer(const T &t) : m_t(t) {}
  virtual T * ref() { return &m_t; }
private:
  T m_t;
};

template <typename T>
class JobRefEnd : public JobRef<T> {
public:
  virtual T * ref() { return NULL; }
};

template <typename T, typename H>
class Executor;

template <typename T, typename H>
class Worker {
public:
  Worker(Executor<T, H> *executor,
         unsigned int id,
         tbb::concurrent_bounded_queue< JobRef<T>* > *queue,
         H *errHandler) :
    m_executor(executor),
    m_id(id),
    m_queue(queue),
    m_errHandler(errHandler),
    m_running(false) {}

  inline void start() {
    pthread_create(&m_thd, NULL, Worker::StartBodyStub, this);
  }

  inline void join() { pthread_join(m_thd, NULL); }
  inline void stop() { m_running = false; }
private:

  static void* StartBodyStub(void *self) {
    static_cast<Worker<T,H>*>(self)->startBody();
    pthread_exit(NULL);
    return NULL;
  }

  void startBody();

  Executor<T, H> *m_executor;
  unsigned int m_id;
  tbb::concurrent_bounded_queue< JobRef<T>* > *m_queue;
  H *m_errHandler;
  bool m_running;

  pthread_t m_thd;
};

template <typename T>
class default_err_handler {
public:
  inline void operator()(unsigned int workerId, const T& job) const {
    std::cerr << "Worker " << workerId
              << " caught error in job" << std::endl;
  }
};

template <typename T, typename H>
class Executor {
public:
  Executor(size_t numWorkers,
           size_t capacity = 1U << 16,
           const H &errHandler = H()) :
    m_numWorkers(numWorkers),
    m_errHandler(errHandler),
    m_running(false) {
    m_queue.set_capacity(capacity);
    m_workers.reserve(numWorkers);
    for (size_t id = 0; id < numWorkers; id++) {
      m_workers.push_back(Worker<T, H>(this, id, &m_queue, &m_errHandler));
    }
  }

  ~Executor() {
    JobRef<T> *job;
    while (m_queue.try_pop(job)) {
      assert(job != NULL);
      delete job;
    }
  }

  virtual void onWorkerStart(unsigned int workerId)  {}
  virtual void onWorkerFinish(unsigned int workerId) {}

  void start() {
    assert(!m_running);
    m_running = true;
    for (typename worker_vec::iterator it = m_workers.begin();
         it != m_workers.end(); ++it) {
      (*it).start();
    }
  }

  void submit(const T &job) {
    assert(m_running);
    m_queue.push(new JobRefContainer<T>(job));
  }

  void stop() {
    assert(m_running);
    m_running = false;
    for (typename worker_vec::iterator it = m_workers.begin();
         it != m_workers.end(); ++it) {
      (*it).stop();
    }
    for (size_t id = 0; id < m_numWorkers; id++) {
      JobRefEnd<T> *j = new JobRefEnd<T>();
      if (!m_queue.try_push(j)) {
        // queue already full
        delete j;
      }
    }
    for (typename worker_vec::iterator it = m_workers.begin();
         it != m_workers.end(); ++it) {
      (*it).join();
    }
  }
private:
  // non-copyable
  Executor(const Executor&);

  size_t m_numWorkers;
  H m_errHandler;
  bool m_running;

  typedef std::vector< Worker<T, H> > worker_vec;
  worker_vec m_workers;
  tbb::concurrent_bounded_queue< JobRef<T>* > m_queue;
};

template <typename T, typename H>
void Worker<T, H>::startBody() {
  m_executor->onWorkerStart(m_id);
  m_running = true;
  while (m_running) {
    JobRef<T> *job = NULL;
    m_queue->pop(job);
    assert(job != NULL);
    T * ref = job->ref();
    try {
      if (ref != NULL) {
        T &ref_ = *ref;
        ref_();
      }
    } catch (...) {
      assert(ref != NULL);
      m_errHandler->operator()(m_id, *ref);
    }
    delete job;
  }
  m_executor->onWorkerFinish(m_id);
}

template <typename T>
class Exec {
public:
  typedef Executor<T, default_err_handler<T> > DefaultExecutor;
};

}

#endif /* __EXECUTOR_H__ */
