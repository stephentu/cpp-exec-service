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
  virtual void execJob() {
    T *r = ref();
    if (r != NULL) {
      T &ref_ = *r;
      ref_();
    }
  }
};

template <typename T>
class JobRefContainer : public JobRef<T> {
public:
  JobRefContainer(const T &t) : m_t(t) {}
  virtual T * ref() { return &m_t; }
private:
  T m_t;
};

class ScopedLocker {
public:
  ScopedLocker(pthread_mutex_t *mutex) : m_mutex(mutex) {
    assert(mutex != NULL);
    int r = pthread_mutex_lock(m_mutex);
    if (r) assert(false);
  }
  ~ScopedLocker() {
    int r = pthread_mutex_unlock(m_mutex);
    if (r) assert(false);
  }
private:
  pthread_mutex_t *m_mutex;
};

template <typename T>
class Future {
public:
  Future() : m_done(false) {
    pthread_mutex_init(&m_mutex, NULL);
    pthread_cond_init(&m_cv, NULL);
  }
  ~Future() {
    pthread_cond_destroy(&m_cv);
    pthread_mutex_destroy(&m_mutex);
  }
  void signal(const T &t) {
    // signal mutex
    ScopedLocker lock(&m_mutex);
    assert(!m_done);
    pthread_cond_broadcast(&m_cv);
    m_t    = t;
    m_done = true;
  }
  T waitFor() {
    ScopedLocker lock(&m_mutex);
    while (!m_done) pthread_cond_wait(&m_cv, &m_mutex);
    assert(m_done);
    return m_t;
  }
private:
  pthread_mutex_t m_mutex;
  pthread_cond_t  m_cv;
  T               m_t;
  bool m_done;
};

template <typename T, typename R>
class JobRefContainerWithFuture : public JobRefContainer<T> {
public:
  JobRefContainerWithFuture(const T &t, Future<R> *ftch)
    : JobRefContainer<T>(t), m_ftch(ftch) {
    assert(ftch != NULL);
  }
  inline Future<R> * future() { return m_ftch; }
  virtual void execJob() {
    T *r = this->ref();
    T &ref_ = *r;
    R res = ref_();
    future()->signal(res);
  }
private:
  Future<R> *m_ftch;
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

protected:
  size_t m_numWorkers;
  H m_errHandler;
  bool m_running;

  typedef std::vector< Worker<T, H> > worker_vec;
  worker_vec m_workers;
  tbb::concurrent_bounded_queue< JobRef<T>* > m_queue;
};

template <typename T, typename R, typename H>
class FutureExecutor : public Executor<T, H> {
public:
  FutureExecutor(size_t numWorkers,
                 size_t capacity = 1U << 16,
                 const H &errHandler = H()) :
    Executor<T, H>(numWorkers, capacity, errHandler) {}

  void submit(const T &t, Future<R> *ftch) {
    assert(this->m_running);
    this->m_queue.push(new JobRefContainerWithFuture<T, R>(t, ftch));
  }
};

template <typename T, typename H>
void Worker<T, H>::startBody() {
  m_executor->onWorkerStart(m_id);
  m_running = true;
  while (m_running) {
    JobRef<T> *job = NULL;
    m_queue->pop(job);
    assert(job != NULL);
    try {
      job->execJob();
    } catch (...) {
      m_errHandler->operator()(m_id, *job->ref());
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

template <typename T, typename R>
class FutureExec {
public:
  typedef FutureExecutor<T, R, default_err_handler<T> > DefaultFutureExecutor;
};

}

#endif /* __EXECUTOR_H__ */
