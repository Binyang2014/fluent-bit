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

#ifndef FLB_OUTPUT_ORDER_H
#define FLB_OUTPUT_ORDER_H

#include <fluent-bit/flb_pipe.h>
#include <fluent-bit/flb_task.h>

struct flb_out_order_manager {
    struct mk_event event;               /* event context to associate events */
    struct mk_event_loop *evl;           /* thread event loop context */
    flb_pipefd_t ch_plugin_events[2];    /* channel to receive plugin notifications */
    flb_pipefd_t ch_parent_events[2];    /* channel to receive parent notifications */
    struct flb_output_instance *ins;     /* output plugin instance */
    int pause;                           /* weather output plugin is paused */

    struct flb_config *config;
    struct flb_tp *tp;
    struct flb_tp_thread *th;

    int coro_id;                         /* coroutine id counter */
    struct mk_list coros;                /* list of co-routines */
    struct mk_list coros_destroy;        /* list of co-routines */
    /* List of mapped 'upstream' contexts */
    struct mk_list upstreams;
    struct mk_list pending_tasks;

    /*
     * If the main engine (parent thread) needs to query the number of active
     * coroutines being used by a threaded instance, the access to the 'coros'
     * list must be protected: we use 'coro_mutex for that purpose.
     */
    pthread_mutex_t coro_mutex;         /* mutex for 'coros' list */
};


int flb_output_order_manager_create(struct flb_config *config,
                                    struct flb_output_instance *ins);
int flb_output_order_manager_start(struct flb_output_instance *ins);
int flb_output_order_manager_flush(struct flb_task *task,
                                   struct flb_output_instance *out_ins,
                                   struct flb_config *config);
void flb_output_order_manager_resume(struct flb_output_instance *ins);
int flb_output_order_manager_coros_size(struct flb_output_instance *ins);
void flb_output_order_manager_destroy(struct flb_output_instance *ins);

#endif