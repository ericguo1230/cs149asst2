#include "tasksys.h"
#include <thread>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <stdio.h>
#include <condition_variable>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    std::atomic<int> next_task(0);
    std::vector<std::thread> threads;
    // Dynamically assign tasks to worker threads as they become available.
    for (int i = 0; i < this->num_threads; i++) {
        threads.push_back(std::thread([runnable, num_total_tasks, &next_task]() {
            while (true) {
                int task_id = next_task.fetch_add(1);
                if (task_id >= num_total_tasks) {
                    break;
                }
                runnable->runTask(task_id, num_total_tasks);
            }
        }));
    }

    // Join all threads before run() returns
    for (auto& t : threads) {
        t.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

class TaskCounter{
    public:
        int taskID;
        std::mutex* g_taskID_mutex;
        TaskCounter() {
            this->taskID = -1;
            this->g_taskID_mutex = new std::mutex();
        }
        ~TaskCounter() {
            delete this->g_taskID_mutex;
        }
        int get_task();
};

int TaskCounter::get_task(){
    std::lock_guard<std::mutex> lock(*this->g_taskID_mutex); 
    this->taskID++;
    return this->taskID;
} 

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    TaskCounter* taskCounter = new TaskCounter();
    std::vector<std::thread> threads;

    for (int i = 0; i < this->num_threads; i++) {
        threads.push_back(std::thread([runnable, num_total_tasks, taskCounter]() {
            while (true) {
                int current_taskID = taskCounter->get_task();
                if (current_taskID >= num_total_tasks) {
                    break;
                }
                runnable->runTask(current_taskID, num_total_tasks);
            }
        }));
    }

    for (auto& t : threads) {
        t.join();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

//Pseudo code
//1. We use producer-consumer model for thread pool
//2. Use taskID to keep track of next task to execute
//3. Threads execute first task then sleep until they are notified
//4. Once a thread finishes it wakes up main thread to check if there are more tasks to execute, if so it wakes up one sleeping thread to execute the next task

class ConsumerProducerCV{
    public:
        int taskID;
        int completedTasks;
        std::mutex* taskID_mutex_;
        std::mutex* completed_tasks;
        std::condition_variable* prod_cv;
        std::condition_variable* work_done_cv;
        int num_waiting_threads_;
        ConsumerProducerCV(int num_waiting_threads) {
            taskID = -1;
            completedTasks = 0;
            taskID_mutex_ = new std::mutex();
            prod_cv = new std::condition_variable();
            work_done_cv = new std::condition_variable();
            num_waiting_threads_ = num_waiting_threads;
            completed_tasks = new std::mutex();
        }
        ~ConsumerProducerCV() {
            delete taskID_mutex_;
            delete prod_cv;
            delete work_done_cv;
            delete completed_tasks;
        }
};

void signal_fn(ConsumerProducerCV* thread) {
    thread->taskID_mutex_->lock();
    while (thread->num_waiting_threads_ > 0) {
        thread->taskID_mutex_->unlock();
        thread->prod_cv->notify_all();
        thread->taskID_mutex_->lock();
    }
    thread->taskID_mutex_->unlock();
}

void wait_fn(ConsumerProducerCV* thread) {
    std::unique_lock<std::mutex> lk(*thread->taskID_mutex_);
    thread->prod_cv->wait(lk);
    thread->num_waiting_threads_--;
    lk.unlock();
}


TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::thread* threads = new std::thread[this->num_threads];
    ConsumerProducerCV* thread = new ConsumerProducerCV(this->num_threads);

    // Create all worker threads
    for (int i = 0; i < this->num_threads; i++) {
        threads[i] = std::thread([thread, num_total_tasks, runnable]() { 
            wait_fn(thread); 
            thread->taskID_mutex_->lock();
            int completed_tasks = 0;
            while (thread->taskID < num_total_tasks - 1) {
                thread->taskID++;
                int current_taskID = thread->taskID;
                thread->taskID_mutex_->unlock();
                runnable->runTask(current_taskID, num_total_tasks);
                completed_tasks++;
                thread->taskID_mutex_->lock();
            }
            thread->taskID_mutex_->unlock();
            thread->completed_tasks->lock();
            thread->completedTasks += completed_tasks;
            if (thread->completedTasks >= num_total_tasks) {
                thread->work_done_cv->notify_all();
            }
            thread->completed_tasks->unlock();
        });
    }

    signal_fn(thread);
    std::unique_lock<std::mutex> lk(*thread->completed_tasks);
    thread->work_done_cv->wait(lk);
    //Maybe verify all work is done?
    lk.unlock();

    for (int i = 0; i < this->num_threads; i++) {
        threads[i].join();
    }

    delete thread;
    delete[] threads;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
