Simple C++ job scheduling library
=================================
This is a bare bones implementation of a job execution library, similar
to `Executors` in Java. It uses the Intel TBB library for high performance
concurrent queues, and the pthread library for concurrent execution.

Usage
=====
All the code lies in a header file, so there is no need to link any libraries.
Simply include `executor.hpp`:

    #include <iostream>
    #include "executor.hpp"

    using namespace exec_service;
    using namespace std;

    class P {
    public:
      inline void operator()() const {
        cout << "TASK" << endl;
      }
    };

    int main(int argc, char **argv) {
      Exec<P>::DefaultExecutor exec(4);
      exec.start();

      exec.submit(P());
      exec.submit(P());
      exec.submit(P());

      exec.stop();
      return 0;
    }

Compile the program as follows:

    g++ -o test_exec -ltbb -lpthread test_exec.cpp
