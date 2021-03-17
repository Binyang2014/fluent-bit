/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2019-2021 The Fluent Bit Authors
 *  Copyright (C) 2015-2018 Treasure Data Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <fluent-bit/flb_log.h>
#include <fluent-bit/flb_output_order.h>
#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_scheduler.h>
#include <fluent-bit/flb_thread_pool.h>

struct pending_task {
    struct flb_task* task;
    struct mk_list _head;
};

/* Cleanup function that runs every 1.5 second */
static void cb_order_manager_sched_timer(struct flb_config *ctx, void *data)
{
    (void)ctx;
    struct flb_output_instance *ins;

    /* Upstream connections timeouts handling */
    ins = (struct flb_output_instance*)data;
    flb_upstream_conn_timeouts(&ins->upstreams);
}

/*
 * For every upstream registered, creates a local mapping for the thread. This is
 * done to provide local queues of connections so we can use our event loop and I/O
 * totally independently without the need of any syncrhonization across threads
 */
static int upstream_thread_create(struct flb_out_order_manager *ord_mgr,
                                  struct flb_output_instance *ins)
{
    struct mk_list *head;
    struct flb_upstream *u;
    struct flb_upstream *th_u;

    mk_list_foreach(head, &ins->upstreams) {
        u = mk_list_entry(head, struct flb_upstream, _head);

        th_u = flb_calloc(1, sizeof(struct flb_upstream));
        if (!th_u) {
            flb_errno();
            return -1;
        }
        th_u->parent_upstream = u;
        flb_upstream_queue_init(&th_u->queue);
        mk_list_add(&th_u->_head, &ord_mgr->upstreams);
    }

    return 0;
}

static int count_upstream_busy_connections(struct flb_out_order_manager *ord_mgr)
{
    int c = 0;
    struct mk_list *head;
    struct flb_upstream *u;

    mk_list_foreach(head, &ord_mgr->upstreams) {
        u = mk_list_entry(head, struct flb_upstream, _head);
        c += mk_list_size(&u->queue.busy_queue);
    }

    return c;
}

static void upstream_thread_destroy(struct flb_out_order_manager *ord_mgr)
{
    struct mk_list *tmp;
    struct mk_list *head;
    struct flb_upstream *th_u;

    mk_list_foreach_safe(head, tmp, &ord_mgr->upstreams) {
        th_u = mk_list_entry(head, struct flb_upstream, _head);
        flb_upstream_destroy(th_u);
    }
}


static inline int handle_output_event(struct flb_config *config,
                                      int ch_parent, flb_pipefd_t fd)
{
    int ret;
    int bytes;
    int out_id;
    uint32_t type;
    uint32_t key;
    uint64_t val;

    bytes = flb_pipe_r(fd, &val, sizeof(val));
    if (bytes == -1) {
        flb_errno();
        return -1;
    }

    /* Get type and key */
    type = FLB_BITS_U64_HIGH(val);
    key = FLB_BITS_U64_LOW(val);

    if (type != FLB_ENGINE_TASK) {
        flb_error("[order manager] invalid event type %i for output handler",
                  type);
        return -1;
    }

    ret = FLB_TASK_RET(key);
    out_id = FLB_TASK_OUT(key);

    /* Destroy the output co-routine context */
    flb_output_flush_finished(config, out_id);

    /*
     * Notify the parent event loop the return status, just forward the same
     * 64 bits value.
     */
    ret = flb_pipe_w(ch_parent, &val, sizeof(val));
    if (ret == -1) {
        flb_errno();
        return -1;
    }

    return 0;
}


static inline void pending_tasks_push_back(struct mk_list *pending_tasks,
                                           struct flb_task *task)
{
    struct pending_task *p_task;
    p_task = flb_malloc(sizeof(struct pending_task));
    if (!p_task) {
        flb_error("[order_manager] failed to push task with task_id=%i to pending list",
                  task->id);
        return;
    }
    p_task->task = task;
    mk_list_add(&p_task->_head, pending_tasks);
}

static inline struct flb_task *pending_tasks_pop_front(struct mk_list *pending_tasks)
{
    struct pending_task *p_task;
    struct flb_task *task;
    
    if (mk_list_is_empty(pending_tasks) == 0) {
        return NULL;
    }
    p_task = mk_list_entry_first(pending_tasks, struct pending_task, _head);
    task = p_task->task;
    mk_list_del(&p_task->_head);
    flb_free(p_task);

    return task;
}

static void pending_tasks_destroy(struct mk_list *pending_list)
{
    struct mk_list *tmp;
    struct mk_list *head;
    struct pending_task *p_task;

    mk_list_foreach_safe(head, tmp, pending_list) {
        p_task = mk_list_entry(head, struct pending_task, _head);
        mk_list_del(&p_task->_head);
        flb_free(p_task);
    }
}

/*
 * TODO: For performance improve, we should merge the tasks with same tag.
 */
static int run_out_task(struct flb_out_order_manager *ord_mgr, struct flb_task *task)
{
    struct mk_list *pending_list = &ord_mgr->pending_tasks;
    struct flb_output_coro *out_coro;
    struct flb_task *out_task = NULL;

    if ((uint64_t)task == 0x30303030 || ord_mgr->pause == FLB_FALSE) {
        out_task = pending_tasks_pop_front(pending_list);
    }
    else if (mk_list_is_empty(&task->retries) != 0) {
        out_task = task;
    }

    if (out_task == NULL) {
        return -1;
    }

    out_coro = flb_output_coro_create(out_task,
                                      out_task->i_ins,
                                      ord_mgr->ins,
                                      ord_mgr->config,
                                      out_task->buf, out_task->size,
                                      out_task->tag,
                                      out_task->tag_len);
    if (!out_coro) {
        return -1;
    }
    flb_coro_resume(out_coro->coro);
    return 0;
}

static void order_manager_thread(void *data)
{
    int n;
    int ret;
    int running = FLB_TRUE;
    int stopping = FLB_FALSE;
    struct mk_event *event;
    struct mk_event event_local;
    struct flb_upstream_conn *u_conn;
    struct flb_output_instance *ins;
    struct flb_out_order_manager *ord_mgr = data;
    struct flb_sched *sched;
    struct flb_task *task;
    struct flb_out_coro_params *params;

    ins = ord_mgr->ins;

    /*
     * Expose the event loop to the I/O interfaces: since we are in a separate
     * thread, the upstream connection interfaces need access to the event
     * loop for event notifications. Invoking the flb_engine_evl_set() function
     * it sets the event loop reference in a TLS (thread local storage) variable
     * of the scope of this thread.
     */
    flb_engine_evl_set(ord_mgr->evl);

    /* Set the upstream queue */
    flb_upstream_list_set(&ord_mgr->upstreams);

    /* Set the upstream queue */
    flb_upstream_list_set(&ord_mgr->upstreams);

    /* Create a scheduler context */
    sched = flb_sched_create(ins->config, ord_mgr->evl);
    if (!sched) {
        flb_plg_error(ins, "could not create thread scheduler");
        return;
    }
    flb_sched_ctx_set(sched);

    /*
     * Sched a permanent callback triggered every 1.5 second to let other
     * components of this thread run tasks at that interval.
     */
    ret = flb_sched_timer_cb_create(sched,
                                    FLB_SCHED_TIMER_CB_PERM,
                                    1500, cb_order_manager_sched_timer, ins);
    if (ret == -1) {
        flb_plg_error(ins, "could not schedule permanent callback");
        return;
    }

    ret = mk_event_channel_create(ord_mgr->evl,
                                  &ord_mgr->ch_plugin_events[0],
                                  &ord_mgr->ch_plugin_events[1],
                                  &event_local);
    if (ret == -1) {
        flb_plg_error(ord_mgr->ins, "could not create order manager channel");
        flb_engine_evl_set(NULL);
        return;
    }
    event_local.type = FLB_ENGINE_EV_OUTPUT;

    while (running)
    {
        mk_event_wait(ord_mgr->evl);
        mk_event_foreach(event, ord_mgr->evl) {
            if (event->type == FLB_ENGINE_EV_CORE) {
            }
            else if (event->type & FLB_ENGINE_EV_SCHED) {
                /*
                 * We use event handler to take care about the
                 * simple timers and retry tasks.
                 */
                flb_sched_event_handler(sched->config, event);
            }
            else if (event->type == FLB_ENGINE_EV_THREAD_OUTPUT) {
                 /* Read the task reference */
                n = flb_pipe_r(event->fd, &task, sizeof(struct flb_task*));
                if (n <= 0) {
                    flb_errno();
                    continue;
                }

                /*
                 * If the address receives 0xdeadbeef, means the thread must
                 * be terminated.
                 */
                if ((uint64_t)task == 0xdeadbeef) {
                    stopping = FLB_TRUE;
                    flb_plg_info(ins, "order manager stopping...");
                    continue;
                }

                /*
                 * If the address receives 0x30303030, means we should resume
                 * output.
                 */
                if ((uint64_t)task == 0x30303030) {
                    ord_mgr->pause = FLB_FALSE;
                    if (run_out_task(ord_mgr, task) == 0) {
                        ord_mgr->pause = FLB_TRUE;
                    }
                    continue;
                }
                
                /* Make sure we only push new created tasks to pengind list*/
                if (mk_list_is_empty(&task->retries) == 0) {
                    pending_tasks_push_back(&ord_mgr->pending_tasks, task);
                }

                if (ord_mgr->pause == FLB_TRUE &&
                    mk_list_is_empty(&task->retries) == 0) {
                    continue;
                }

                /* Pause output if one tasks is running */
                if (run_out_task(ord_mgr, task) == 0) {
                    ord_mgr->pause = FLB_TRUE;
                }
            }
            else if (event->type == FLB_ENGINE_EV_THREAD) {
                /*
                 * Check if we have some co-routine associated to this event,
                 * if so, resume the co-routine
                 */
                u_conn = (struct flb_upstream_conn*)event;
                if (u_conn->coro) {
                    flb_plg_trace(ins, "resuming upstream coroutine=%p", u_conn->coro);
                    flb_coro_resume(u_conn->coro);
                }
            }
            else if (event->type == FLB_ENGINE_EV_OUTPUT) {
                /*
                 * The flush callback has finished working and delivered it
                 * return status. At this intermediary step we cleanup the
                 * co-routine resources created before and then forward
                 * the return message to the parent event loop so the Task
                 * can be updated.
                 */
                handle_output_event(ins->config, ins->ch_events[1], event->fd);
            }
        }
       
        flb_upstream_conn_pending_destroy_list(&ord_mgr->upstreams);
        if (stopping == FLB_TRUE &&
            mk_list_size(&ord_mgr->coros) == 0 &&
            mk_list_size(&ord_mgr->pending_tasks) == 0) {
            if (count_upstream_busy_connections(ord_mgr) == 0) {
                running = FLB_FALSE;
            }
        }
    }

    upstream_thread_destroy(ord_mgr);
    flb_upstream_conn_active_destroy_list(&ord_mgr->upstreams);
    flb_upstream_conn_pending_destroy_list(&ord_mgr->upstreams);
    pending_tasks_destroy(&ord_mgr->pending_tasks);
    flb_sched_destroy(sched);
    params = FLB_TLS_GET(out_coro_params);
    if (params) {
        flb_free(params);
    }
    flb_plg_info(ins, "order manager stopped");
}

int flb_output_order_manager_create(struct flb_config *config,
                                    struct flb_output_instance *ins)
{
    int ret;
    struct flb_tp *tp;
    struct flb_tp_thread *th;
    struct mk_event_loop *evl = NULL;
    struct flb_out_order_manager *ord_mgr = NULL;

    ord_mgr = flb_malloc(sizeof(struct flb_out_order_manager));
    if (!ord_mgr) {
        return -1;
    }
    ins->ord_mgr = ord_mgr;
    ins->is_keep_order = FLB_TRUE;
    ord_mgr->ins = ins;
    ord_mgr->config = config;
    ord_mgr->pause = FLB_FALSE;
    ord_mgr->coro_id = 0;

    /* Create the thread pool context */
    tp = flb_tp_create(config);
    if (!tp) {
        flb_free(ord_mgr);
        return -1;
    }
    ord_mgr->tp = tp;
    
    mk_list_init(&ord_mgr->coros);
    mk_list_init(&ord_mgr->coros_destroy);
    mk_list_init(&ord_mgr->upstreams);
    mk_list_init(&ord_mgr->pending_tasks);
    pthread_mutex_init(&ord_mgr->coro_mutex, NULL);
    ret = upstream_thread_create(ord_mgr, ins);
    if (ret == -1) {
        flb_plg_error(ins, "could not create upstream for order manager thread");
        goto cleanup;
    }

    /* Create event loop for order manager */
    evl = mk_event_loop_create(64);
    if (!evl) {
        flb_plg_error(ins, "could not create order manager event loop");
        ret = -1;
        goto cleanup;
    }
    ord_mgr->evl = evl;

    ret = mk_event_channel_create(ord_mgr->evl,
                                  &ord_mgr->ch_parent_events[0],
                                  &ord_mgr->ch_parent_events[1],
                                  ord_mgr);

    if (ret == -1) {
        flb_plg_error(ins, "could not create thread channel");
        goto cleanup;
    }
    /* Signal type to indicate a "flush" request */
    ord_mgr->event.type = FLB_ENGINE_EV_THREAD_OUTPUT;

    flb_plg_debug(ord_mgr->ins,
                  "order manager created event channels: read=%i write=%i",
                  ord_mgr->ch_parent_events[0], ord_mgr->ch_parent_events[1]);

    /* Spawn the thread */
    th = flb_tp_thread_create(tp, order_manager_thread, ord_mgr, config);
    if (!th) {
        flb_plg_error(ins, "could not register order manager thread");
        ret = -1;
        goto cleanup;
    }
    ord_mgr->th = th;
cleanup:
    if (ret == -1) {
        if (evl != NULL) {
            mk_event_loop_destroy(evl);
        }
        upstream_thread_destroy(ord_mgr);
        flb_tp_destroy(ord_mgr->tp);
        flb_free(ord_mgr);
    }
    return ret;
}

int flb_output_order_manager_coros_size(struct flb_output_instance *ins)
{
    int size = 0;
    struct flb_out_order_manager *ord_mgr = ins->ord_mgr;
    struct flb_tp_thread *th = ord_mgr->th;

    if (th->status != FLB_THREAD_POOL_RUNNING) {
        return 0;
    }

    pthread_mutex_lock(&ord_mgr->coro_mutex);
    size = mk_list_size(&ord_mgr->coros);
    pthread_mutex_unlock(&ord_mgr->coro_mutex);

    return size;
}

void flb_output_order_manager_destroy(struct flb_output_instance *ins)
{
    int n;
    uint64_t stop = 0xdeadbeef;
    struct flb_out_order_manager *ord_mgr = ins->ord_mgr;
    struct flb_tp *tp;
    struct flb_tp_thread *th;

    tp = ord_mgr->tp;
    if (!tp) {
        return;
    }

    th = ord_mgr->th;
    /* Signal order manager thread that needs to stop doing work */
    if (th->status == FLB_THREAD_POOL_RUNNING) {
        n = flb_pipe_w(ord_mgr->ch_parent_events[1], &stop, sizeof(stop));
        if (n < 0) {
            flb_errno();
            flb_plg_error(ins, "could not signal order manager thread");
        }
        else {
            pthread_join(th->tid, NULL);
        }
    }

    flb_tp_destroy(ord_mgr->tp);
    ord_mgr->tp = NULL;
    mk_event_loop_destroy(ord_mgr->evl);
    flb_free(ins->ord_mgr);
    ins->ord_mgr = NULL;
}

int flb_output_order_manager_start(struct flb_output_instance *ins)
{
    struct flb_tp *tp = ins->ord_mgr->tp;

    flb_tp_thread_start_all(tp);
    return 0;
}


int flb_output_order_manager_flush(struct flb_task *task,
                                   struct flb_output_instance *out_ins,
                                   struct flb_config *config)
{
    int n = 0;
    struct flb_out_order_manager *ord_mgr = out_ins->ord_mgr;

    flb_plg_debug(out_ins, "task_id=%i assigned to order manager", task->id);
    n = flb_pipe_write_all(ord_mgr->ch_parent_events[1], &task, sizeof(struct flb_task*));
    if (n == -1) {
        flb_errno();
        return -1;
    }

    return 0;
}

void flb_output_order_manager_resume(struct flb_output_instance *out_ins)
{
    int ret;
    uint64_t resume = 0x30303030;
    struct flb_out_order_manager *ord_mgr = out_ins->ord_mgr;

    ret = flb_pipe_write_all(ord_mgr->ch_parent_events[1], &resume, sizeof(resume));
    if (ret == -1) {
        flb_errno();
    }
}