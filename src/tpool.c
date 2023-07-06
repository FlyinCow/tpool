#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

// #define CALC_LOG  // 控制计算线程是否打印debug LOG, 仅供调试使用
// #define REQST_LOG // 控制请求线程是否打印debug LOG, 仅供调试使用

// struct itimerval g_itimer; // for gprof

#define REQUEST_THREAD_NUM 1000 // 请求线程数
#define CALC_THREAD_NUM 1       // 计算线程数

typedef enum OP { ADD, SUB, MUT, DIV } OP; // 计算的操作类型

// 将计算操作类型映射到相应的符号，方便打印
static char op_char_map[] = {'+', '-', '*', '/'};

// 计算任务实现
int calc(int a, int b, OP op) {
  switch (op) {
  case ADD:
    return a + b;
  case SUB:
    return a - b;
  case MUT:
    return a * b;
  case DIV:
    return b == 0 ? 0 : a / b;
  }
}

// 任务数据类型
typedef struct job_t {
  struct job_t *next; // 用于形成任务队列
  OP op;              // 计算操作的类型
  int a;              // 操作数a
  int b;              // 操作数b
  int from;           // 请求线程的编号
} job_t;

// 结果数据类型
typedef struct result_t {
  int a;                 // 操作数a
  int b;                 // 操作数b
  OP op;                 // 操作类型
  int value;             // 结果值
  int request_by;        // 请求线程的编号
  int calculate_by;      // 计算线程的编号
  struct result_t *next; // 用于形成结果队列
} result_t;

// 任务队列数据类型
typedef struct job_queue_t {
  job_t *head;   // 队头
  job_t *tail;   // 队尾
  int job_count; // 队列中的任务数量
} job_queue_t;

// 结果队列数据类型
typedef struct result_queue_t {
  result_t *head;   //队头
  result_t *tail;   // 队尾
  int result_count; // 队列中的结果数量
} result_queue_t;

// 线程数据结构
typedef struct ithread_t {
  pthread_t thread;      // pthread tcb
  pthread_mutex_t mutex; // 保护工作队列的信号量
  pthread_cond_t cond; // 接受“有任务”和“停止线程”信号的条件变量
  job_queue_t job_queue; // 任务队列
  bool shutdown;         // 停止标志位
} ithread_t;

// 线程池数据结构
typedef struct tpool_t {
  int thread_count;   // 总线程数量
  int due;            // 下一个接受任务的线程编号
  pthread_mutex_t _m; // 保护线程数组
  ithread_t *threads; // 线程数组
} tpool_t;

// 用于给线程传递启动参数
typedef struct thread_arg_t {
  int thread_index; // 启动的线程编号
} thread_arg_t;

// 计算线程流程
void calc_thread_exec(thread_arg_t *calc_thread_arg);

static tpool_t pool; // 线程池

// 结果队列，results[i]代表第i个请求线程的结果队列
static result_queue_t *results;

static pthread_mutex_t *results_lock; // 结果队列锁
static pthread_cond_t *results_cond; // 接收“计算完成”信号的条件变量

/**
 * @brief 在 `pool` 处初始化一个有 `num_threads` 个线程的线程池
 *
 * @param pool
 * @param num_threads
 * @return 正常返回0，异常返回-1
 */
int tpool_create(tpool_t *pool, int num_threads) {
  if (num_threads <= 0)
    num_threads = 1; // 至少有一个线程
  printf("init pool for %d threads.\n", num_threads);

  // 创建线程池
  pool->thread_count = num_threads;
  pool->due = 0;
  thread_arg_t *args[num_threads];
  // 创建线程队列锁
  if (0 != pthread_mutex_init(&pool->_m, NULL))
    goto return_error;

  // 初始化线程队列
  if (NULL == (pool->threads = calloc(num_threads, sizeof(ithread_t))))
    goto return_error;
  for (int i = 0; i < num_threads; i++) {
    // 为每个线程创建互斥信号量、条件变量和任务队列
    if (0 != pthread_mutex_init(&pool->threads[i].mutex, NULL))
      goto cleanup_ithreads;
    if (0 != pthread_cond_init(&pool->threads[i].cond, NULL))
      goto cleanup_ithreads;
    pool->threads[i].job_queue.head = pool->threads->job_queue.tail = NULL;
    pool->threads[i].job_queue.job_count = 0;
    pool->threads[i].shutdown = false;

    // 创建每个线程，传入线程编号
    args[i] = (thread_arg_t *)malloc(sizeof(thread_arg_t));
    args[i]->thread_index = i;
#ifdef CALC_LOG
    printf("create thread %d\n", i);
#endif
    // 创建线程
    pthread_create(&pool->threads[i].thread, NULL, (void *)calc_thread_exec,
                   (void *)args[i]);
    pthread_detach(pool->threads[i].thread);
  }
  return 0;

// 统一错误处理：清理已分配内存，返回-1
cleanup_ithreads:
  free(pool->threads);
return_error:
  return -1;
}

/**
 * @brief 关闭线程池
 *
 * @param pool
 */
void tpool_destroy(tpool_t *pool) {
  job_t *pj, *qj;
  // 获取计算线程队列锁,防止后续添加任务
  pthread_mutex_lock(&pool->_m);
  // 给每一个计算线程发送停止信号
  for (int i = 0; i < pool->thread_count; i++) {
    pthread_mutex_lock(&pool->threads[i].mutex);
    pool->threads[i].shutdown = true;
    pthread_cond_signal(&pool->threads[i].cond);
    pthread_mutex_unlock(&pool->threads[i].mutex);
    // 为每个计算线程销毁任务队列
    pj = pool->threads[i].job_queue.head;
    while (pj != NULL) {
      qj = pj;
      pj = pj->next;
      free(qj);
    }
    pthread_mutex_unlock(&pool->_m);
    // 清理计算线程队列锁
    pthread_mutex_destroy(&pool->_m);
    // 销毁线程任务队列锁和任务队列条件变量
  }
  for (int i = 0; i < pool->thread_count; i++) {
    pthread_mutex_destroy(&pool->threads[i].mutex);
    pthread_cond_destroy(&pool->threads[i].cond);
  }
  // 清理TCB
  free(pool->threads);
}

/**
 * @brief 向下一个该接收任务的线程的工作队列中添加一组任务
 *
 * @param pool
 * @param jobs
 */
void tpool_add_jobs(tpool_t *pool, job_queue_t jobs) {
  // 获取线程池锁以进入tpool_t::due的临界区
  pthread_mutex_lock(&pool->_m);
  // 进入线程池的工作队列临界区
  pthread_mutex_lock(&pool->threads[pool->due].mutex);
  // 将jobs添加到due线程的工作队列中
  if (0 == pool->threads[pool->due].job_queue.job_count) {
    pool->threads[pool->due].job_queue.head = jobs.head;
    pool->threads[pool->due].job_queue.tail = jobs.tail;
    pool->threads[pool->due].job_queue.job_count = jobs.job_count;
  } else {
    pool->threads[pool->due].job_queue.tail->next = jobs.head;
    pool->threads[pool->due].job_queue.job_count += jobs.job_count;
  }
#ifdef CALC_LOG
  printf("thread %d: current job count:%d\n", pool->due,
         pool->threads[pool->due].job_queue.job_count);
#endif
  // 通知due线程任务已分配
  pthread_cond_signal(&pool->threads[pool->due].cond);
  // 退出due线程工作队列临界区
  pthread_mutex_unlock(&pool->threads[pool->due].mutex);
  // 将due线程设为下一个线程
  pool->due = (pool->due + 1) % pool->thread_count;
  // 退出tpool_t::due的临界区
  pthread_mutex_unlock(&pool->_m);
}

/**
 * @brief 请求线程工作流程
 *
 * @param arg
 */
void request_thread_exec(thread_arg_t *arg) {
  // setitimer(ITIMER_PROF, &g_itimer, NULL);
  int me = arg->thread_index;
  // free(arg);
  // 随机生成1~10个任务
  job_queue_t jobs;
  job_t *pj, *qj;
  result_t *pr, *qr;
  // int job_count = rand() % 10 + 1;
  int job_count = 10;
#ifdef REQST_LOG
  printf("spawn %d jobs\n", job_count);
#endif
  pj = (job_t *)malloc(sizeof(job_t));
  pj->a = rand() % 100;
  pj->b = rand() % 100;
  pj->op = ADD + rand() % 4;
  pj->next = NULL;
  pj->from = me;
  jobs.head = jobs.tail = pj;
  for (int i = 0; i < job_count - 1; i++) {
    qj = pj;
    pj = (job_t *)malloc(sizeof(job_t));
    pj->a = rand() % 100;
    pj->b = rand() % 100;
    pj->op = ADD + rand() % 4;
    pj->from = me;
    jobs.tail = pj;
    pj->next = NULL;
    qj->next = pj;
  }
  jobs.job_count = job_count;
  // 将任务派发给计算线程
  tpool_add_jobs(&pool, jobs);
  // 进入结果队列临界区
  pthread_mutex_lock(&results_lock[me]);
#ifdef REQST_LOG
  printf("request %d waiting\n", me);
#endif
  // 等待结果
  // 每次有一个结果返回就会被唤醒，直到派发出的所有任务都计算完成则停止等待
  while (job_count != results[me].result_count) {
    pthread_cond_wait(&results_cond[me], &results_lock[me]);
  }
  // 打印计算结果
  pr = results[me].head;
  while (pr != NULL) {
    qr = pr;
    printf("request %d: %d %c %d = %d from %d\n", me, pr->a,
           op_char_map[pr->op], pr->b, pr->value, pr->calculate_by);
    pr = pr->next;
    results[me].result_count--;
    free(qr);
  }
  results[me].head = results[me].tail = NULL;
  // 退出结果队列临界区
  pthread_mutex_unlock(&results_lock[me]);
}

// 计算线程
void calc_thread_exec(thread_arg_t *args) {
  // setitimer(ITIMER_PROF, &g_itimer, NULL);
  int me = args->thread_index;
  free(args);
  // 工作循环
  while (true) {
    // 进入任务队列临界区
    pthread_mutex_lock(&pool.threads[me].mutex);
#ifdef CALC_LOG
    printf("thread %d waiting\n", me);
#endif
    // 等待工作队列或停止信号
    while (!pool.threads[me].shutdown &&
           0 == pool.threads[me].job_queue.job_count) {
      pthread_cond_wait(&pool.threads[me].cond, &pool.threads[me].mutex);
    }
    // 如果是因为停止信号而结束阻塞,则停机
    if (pool.threads[me].shutdown) {
      pthread_mutex_unlock(&pool.threads[me].mutex);
      printf("thread %d shutdown\n", me);
      pthread_exit(NULL);
    }

    // 不是停止信号,是任务队列有任务
    // 获取工作队列中的所有任务
    job_queue_t jobs;
    jobs.head = pool.threads[me].job_queue.head;
    jobs.tail = pool.threads[me].job_queue.tail;
    jobs.job_count = pool.threads[me].job_queue.job_count;
    pool.threads[me].job_queue.tail = NULL;
    pool.threads[me].job_queue.head = NULL;
    pool.threads[me].job_queue.job_count = 0;
    // 退出任务队列临界区
    pthread_mutex_unlock(&pool.threads[me].mutex);
#ifdef CALC_LOG
    printf("thread %d jobs: %d\n", me, jobs.job_count);
#endif

    // 计算结果
    result_t *res[jobs.job_count];
    job_t *pj = jobs.head;
    job_t *qj;
    for (int i = 0; i < jobs.job_count; i++) {
      res[i] = malloc(sizeof(result_t));
      res[i]->a = pj->a;
      res[i]->b = pj->b;
      res[i]->op = pj->op;
      res[i]->calculate_by = me;
      res[i]->request_by = pj->from;
      res[i]->value = calc(pj->a, pj->b, pj->op);
      res[i]->next = NULL;
      pj = pj->next;
#ifdef CALC_LOG
      printf("calculate %d %c %d = %d from %d by %d\n", res[i]->a,
             op_char_map[res[i]->op], res[i]->b, res[i]->value,
             res[i]->request_by, me);
#endif
    }

    // 将结果填充入来源请求线程的结果队列
    pj = jobs.head;
    for (int i = 0; i < jobs.job_count; i++) {
      // 进入结果队列临界区
      pthread_mutex_lock(&results_lock[pj->from]);
      if (0 == results[pj->from].result_count) {
        results[pj->from].head = results[pj->from].tail = res[i];
      } else {
        results[pj->from].tail->next = res[i];
        results[pj->from].tail = res[i];
      }
      results[pj->from].result_count++;
      // 发送结果计算完成信号
      pthread_cond_signal(&results_cond[pj->from]);
      // 退出结果队列临界区
      pthread_mutex_unlock(&results_lock[pj->from]);
      qj = pj;
      pj = pj->next;
      free(qj);
    }
  }
}

int main() {
  // 创建线程池
  // 创建完毕后每个计算线程会在任务队列上阻塞等待
  // getitimer(ITIMER_PROF, &g_itimer);
  if (0 != tpool_create(&pool, CALC_THREAD_NUM))
    return -1;

  pthread_t request_threads[REQUEST_THREAD_NUM];
  thread_arg_t args[REQUEST_THREAD_NUM];
  // 创建结果队列, 为结果队列锁和结果队列条件变量分配空间
  results = (result_queue_t *)calloc(REQUEST_THREAD_NUM, sizeof(result_t));
  results_lock =
      (pthread_mutex_t *)calloc(REQUEST_THREAD_NUM, sizeof(pthread_mutex_t));
  results_cond =
      (pthread_cond_t *)calloc(REQUEST_THREAD_NUM, sizeof(pthread_cond_t));

  // 创建每个请求线程
  for (int i = 0; i < REQUEST_THREAD_NUM; i++) {
    args[i].thread_index = i;
    results[i].head = results[i].tail = NULL;
    results[i].result_count = 0;
    // 创建结果队列锁和条件变量,开始执行请求线程
    pthread_mutex_init(&results_lock[i], NULL);
    pthread_cond_init(&results_cond[i], NULL);
    pthread_create(&request_threads[i], NULL, (void *)request_thread_exec,
                   (void *)&args[i]);
  }
  // 等待所有请求线程结束
  for (int i = 0; i < REQUEST_THREAD_NUM; i++) {
    pthread_join(request_threads[i], NULL);
  }
  // 关闭线程池
  tpool_destroy(&pool);
  return 0;
}