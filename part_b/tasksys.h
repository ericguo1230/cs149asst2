#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <queue>
#include <unordered_set>
#include <map>
#include <list>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

class TaskDetail {
    public:
        TaskID id;
        IRunnable* runnable;
        int remaining_dependencies;
        int remaining_tasks;
        int tasks_done;
        const int num_total_tasks;
        std::mutex mutex;
        TaskDetail(IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps, TaskID id) : 
        num_total_tasks(num_total_tasks) {
            this->runnable = runnable;
            this->remaining_dependencies = deps.size();
            this->remaining_tasks = num_total_tasks;
            this->tasks_done = 0;
            this->id = id;
        }
        ~TaskDetail() {}
}; 

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        void addTaskToWorkQueue(TaskDetail* task);
        void insertDependent(TaskID dep, TaskDetail* task);
        void updateDependents(TaskID finished_task);
        //Data structures for task management
        std::queue<TaskDetail*> task_queue;
        std::map<TaskID, std::list<TaskDetail*>> task_dependents;
        std::queue<TaskDetail*> waiting_tasks;
        std::unordered_set<TaskID> completed_tasks;
        
        //atomics for task management
        std::atomic<TaskID> next_task_id{0};
        std::atomic<bool> shutdown{false};
        std::atomic<int> incomplete_tasks{0};

        std::vector<std::thread> threads;

        //locks for altering data structures
        std::mutex queue_mutex; //for task_queue and waiting_queue
        std::mutex completed_tasks_mutex; //for completed_tasks
        std::mutex dependents_mutex; //for task_dependents

        //condition variable
        std::condition_variable task_available;
        std::condition_variable task_done;
        
};

#endif
