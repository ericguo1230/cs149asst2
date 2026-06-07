#include "tasksys.h"
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <queue>
#include <unordered_set>
#include <map>


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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

void TaskSystemParallelThreadPoolSleeping::addTaskToWorkQueue(TaskDetail* task) {
    std::lock_guard<std::mutex> lock(this->queue_mutex);
    this->task_queue.push(task);
}

//PRECONDITION: LOCK HAS BEEN OBTAINED
void TaskSystemParallelThreadPoolSleeping::insertDependent(TaskID task_id, TaskDetail* task) {
    this->task_dependents[task_id].push_back(task);
}


void TaskSystemParallelThreadPoolSleeping::updateDependents(TaskID finished_task) {
    std::lock_guard<std::mutex> lk(this->dependents_mutex);

    auto it = this->task_dependents.find(finished_task);
    if (it == this->task_dependents.end()) {
        return;
    }

    for (TaskDetail* dependent : it->second) {
        std::lock_guard<std::mutex> lock(dependent->mutex);
        dependent->remaining_dependencies -= 1;

        if (dependent->remaining_dependencies == 0) {
            addTaskToWorkQueue(dependent);
            this->task_available.notify_all();
        }
    }

    this->task_dependents.erase(it);
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    this->threads.reserve(num_threads);
    std::mutex queue_mutex; 
    std::mutex completed_tasks_mutex;
    std::mutex dependents_mutex;

    for (int i = 0; i < num_threads; i++) {
        this->threads.emplace_back([this]() {
            while (true) {
                TaskDetail* task = nullptr;
                int task_id = -1;
                int num_total_tasks = -1;
                bool completed_whole_task = false;

                {
                    std::unique_lock<std::mutex> lk(this->queue_mutex);

                    this->task_available.wait(lk, [&] {
                        return this->shutdown.load() || !this->task_queue.empty();
                    });

                    if (this->shutdown.load()) {
                        return;
                    }

                    task = this->task_queue.front();

                    task->remaining_tasks--;

                    task_id = task->num_total_tasks - task->remaining_tasks - 1;
                    num_total_tasks = task->num_total_tasks;

                    if (task->remaining_tasks == 0) {
                        this->task_queue.pop();
                    }
                } // release queue_mutex before running task

                task->runnable->runTask(task_id, num_total_tasks);
                
                {
                    std::lock_guard<std::mutex> lock(task->mutex);
                    task->tasks_done++;
                    completed_whole_task = (task->tasks_done == task->num_total_tasks);
                }

                if (completed_whole_task) {
                    {
                        std::lock_guard<std::mutex> lk(this->completed_tasks_mutex);
                        this->completed_tasks.insert(task->id);
                    }

                    this->updateDependents(task->id);
                    this->incomplete_tasks--;
                    this->task_done.notify_all();
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    this->shutdown.store(true);
    this->task_available.notify_all();
    
    for (auto& thread : this->threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    
    // Clean up remaining TaskDetail objects in task_queue
    while (!this->task_queue.empty()) {
        delete this->task_queue.front();
        this->task_queue.pop();
    }
    
    // Clean up remaining TaskDetail objects in waiting_tasks
    while (!this->waiting_tasks.empty()) {
        delete this->waiting_tasks.front();
        this->waiting_tasks.pop();
    }
    
    // Clean up remaining TaskDetail objects in task_dependents
    for (auto& entry : this->task_dependents) {
        for (TaskDetail* task : entry.second) {
            delete task;
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    TaskID task_id = this->next_task_id.fetch_add(1);
    TaskDetail* task = new TaskDetail(runnable, num_total_tasks, deps, task_id);
    this->incomplete_tasks++;
    if (deps.empty()) {
        //No dependencies, add to work queue
        addTaskToWorkQueue(task);
        this->task_available.notify_all();
        return task_id;
    }
    {
        std::lock_guard<std::mutex> lock(this->completed_tasks_mutex);
        std::lock_guard<std::mutex> lock_dependent(this->dependents_mutex);
        std::lock_guard<std::mutex> lock_queue(this->queue_mutex);
        for (const auto& dep : deps) {
            if (this->completed_tasks.count(dep)) {
                task->remaining_dependencies--;
            } else{
                insertDependent(dep, task);
            }
        }
    }
    if (task->remaining_dependencies == 0) {
        addTaskToWorkQueue(task);
        this->task_available.notify_all();
    } else {
        std::lock_guard<std::mutex> lock(this->queue_mutex);
        this->waiting_tasks.push(task);
    }
    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    //Precondition: this assumes we will eventually have all the dependencies to run a task
    std::unique_lock<std::mutex> lk(this->queue_mutex);
    this->task_done.wait(lk, [&] {
        return this->shutdown.load() || this->incomplete_tasks.load() == 0;
    });
    return;
}
