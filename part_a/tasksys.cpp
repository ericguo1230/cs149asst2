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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    this->current_runnable = nullptr;
    this->workers.reserve(num_threads);

    for (int i = 0; i < num_threads; i++) {
        this->workers.push_back(std::thread([this]() {
            while (!this->shutdown.load()) {
                if (!this->has_work.load()) {
                    std::this_thread::yield();
                    continue;
                }

                int task_id = this->next_task.fetch_add(1);
                int num_total_tasks = this->current_num_total_tasks.load();
                if (task_id >= num_total_tasks) {
                    std::this_thread::yield();
                    continue;
                }

                this->current_runnable->runTask(task_id, num_total_tasks);
                int completed = this->completed_tasks.fetch_add(1) + 1;
                if (completed == num_total_tasks) {
                    this->has_work.store(false);
                }
            }
        }));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    shutdown.store(true);

    for (auto& worker : this->workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) {
        return;
    }

    this->current_runnable = runnable;
    this->current_num_total_tasks.store(num_total_tasks);
    this->next_task.store(0);
    this->completed_tasks.store(0);
    this->has_work.store(true);

    while (this->has_work.load()) {
        std::this_thread::yield();
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    this->current_runnable = nullptr;
    this->workers.reserve(num_threads);
    this->has_work = false;
    this->next_task.store(0);
    this->completed_tasks.store(0);
    this->current_num_total_tasks.store(0);
    this->shutdown.store(false);

    for (int i = 0; i < num_threads; i++) {
        this->workers.push_back(std::thread([this, i]() {
            while (!this->shutdown.load()){
                std::unique_lock<std::mutex> lk(this->mutex);
                this->work_available.wait(lk, [this]() {
                    return this->has_work || this->shutdown.load();
                });

                if (this->shutdown.load()) {
                    break;
                }
                
                int worker_generation = this->generation;
                if (this->next_task.load() >= this->current_num_total_tasks.load()) {
                    this->work_available.wait(lk, [this, worker_generation]() {
                        return this->shutdown.load() ||
                               !this->has_work ||
                               this->generation != worker_generation;
                    });
                    continue;
                }

                IRunnable* runnable = this->current_runnable;
                int num_total_tasks = this->current_num_total_tasks.load();
                int task_id = this->next_task.fetch_add(1);
                lk.unlock();

                runnable->runTask(task_id, num_total_tasks);
                int completed = this->completed_tasks.fetch_add(1) + 1;
                if (completed == num_total_tasks){
                    {
                        std::unique_lock<std::mutex> lk(this->mutex);
                        this->has_work = false;
                    }
                    this->work_done.notify_all();
                }
            }
        }));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    this->shutdown.store(true);

    this->work_available.notify_all();

    for (auto& worker : this->workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) {
        return;
    }
    this->current_num_total_tasks.store(num_total_tasks);
    this->next_task.store(0);
    this->completed_tasks.store(0);
    {
        std::unique_lock<std::mutex> lk(this->mutex);
        this->current_runnable = runnable;
        this->has_work = true;
        this->generation++;
    }
    this->work_available.notify_all();
    std::unique_lock<std::mutex> lk(this->mutex);
    this->work_done.wait(lk, [this]() {
        return !this->has_work;
    });
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
