//
// Created by vasilis on 10/09/20.
//

#include "hr_kvs_util.h"
#include <stdio.h>
#include <string.h>
#include "bplus.h"
#include "default_data_config.h"
#include "splinterdb.h"

#define DB_FILE_NAME    "splinterdb_intro_db"
#define USER_MAX_KEY_SIZE ((int)100)
#define DB_FILE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define CACHE_SIZE_MB   64   // Size of cache; can be changed across boots

///* ---------------------------------------------------------------------------
////------------------------------ HELPERS -----------------------------
////---------------------------------------------------------------------------*/

static inline void sw_prefetch_buf_op_keys(context_t *ctx)
{
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    if (hr_ctx->buf_ops->capacity == 0) return;
    for (int op_i = 0; op_i < hr_ctx->buf_ops->capacity; ++op_i) {
        buf_op_t *buf_op = (buf_op_t *) get_fifo_pull_relative_slot(hr_ctx->buf_ops, op_i);
        __builtin_prefetch(buf_op->kv_ptr, 0, 0);
    }
}

static inline void init_w_rob_on_loc_inv(context_t *ctx,
                                         mica_op_t *kv_ptr,
                                         ctx_trace_op_t *op,
                                         uint64_t new_version,
                                         uint32_t write_i)
{
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    hr_w_rob_t *w_rob = (hr_w_rob_t *)
            get_fifo_push_relative_slot(hr_ctx->loc_w_rob, write_i);
    if (ENABLE_ASSERTIONS) {
        assert(w_rob->w_state == INVALID);
        w_rob->l_id = hr_ctx->inserted_w_id[ctx->m_id];
    }
    w_rob->version = new_version;
    w_rob->kv_ptr = kv_ptr;
    w_rob->val_len = op->val_len;
    w_rob->sess_id = op->session_id;
    w_rob->w_state = SEMIVALID;
    if (ENABLE_ASSERTIONS)
        assert(hr_ctx->stalled[w_rob->sess_id]);
    if (DEBUG_INVS)
        my_printf(cyan, "W_rob insert sess %u write %lu, w_rob_i %u\n",
                  w_rob->sess_id, w_rob->l_id,
                  hr_ctx->loc_w_rob->push_ptr);
    fifo_increm_capacity(hr_ctx->loc_w_rob);
}

#if USE_SPLINTERDB

static inline void sdb_init_w_rob_on_loc_inv(context_t *ctx, splinterdb* spl_handle,
                                        ctx_trace_op_t *op, uint64_t new_version, uint32_t write_i) {
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    hr_w_rob_t *w_rob = (hr_w_rob_t *)
            get_fifo_push_relative_slot(hr_ctx->loc_w_rob, write_i);
    if (ENABLE_ASSERTIONS) {
        assert(w_rob->w_state == INVALID);
        w_rob->l_id = hr_ctx->inserted_w_id[ctx->m_id];
    }
    w_rob->version = new_version;
    w_rob->spl_handle = spl_handle;
    w_rob->val_len = op->val_len;
    w_rob->sess_id = op->session_id;
    w_rob->w_state = SEMIVALID;
    if (ENABLE_ASSERTIONS)
        assert(hr_ctx->stalled[w_rob->sess_id]);
    if (DEBUG_INVS)
        my_printf(cyan, "W_rob insert sess %u write %lu, w_rob_i %u\n",
                  w_rob->sess_id, w_rob->l_id,
                  hr_ctx->loc_w_rob->push_ptr);
    fifo_increm_capacity(hr_ctx->loc_w_rob);
}

#endif /* if USE_SPLINTERDB */

static inline void init_w_rob_on_rem_inv(context_t *ctx,
                                         mica_op_t *kv_ptr,
                                         hr_inv_mes_t *inv_mes,
                                         hr_inv_t *inv,
                                         bool inv_applied)
{
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    hr_w_rob_t *w_rob = (hr_w_rob_t *)
            get_fifo_push_slot(&hr_ctx->w_rob[inv_mes->m_id]);
    if (DEBUG_INVS)
        my_printf(cyan, "W_rob %u for inv from %u with l_id %lu -->%lu, inserted w_id = %u\n",
                  w_rob->id,  inv_mes->m_id, inv_mes->l_id,
                  inv_mes->l_id + inv_mes->coalesce_num,
                  hr_ctx->inserted_w_id[inv_mes->m_id]);
    if (ENABLE_ASSERTIONS) {
        assert(w_rob->w_state == INVALID);
        w_rob->l_id = hr_ctx->inserted_w_id[inv_mes->m_id];
    }
    w_rob->w_state = VALID;
    w_rob->inv_applied = inv_applied;

    hr_ctx->inserted_w_id[inv_mes->m_id]++;
    // w_rob capacity is already incremented when polling
    // to achieve back pressure at polling
    if (inv_applied) {
        w_rob->version = inv->version;
        w_rob->m_id = inv_mes->m_id;
        w_rob->kv_ptr = kv_ptr;
    }
    fifo_incr_push_ptr(&hr_ctx->w_rob[inv_mes->m_id]);
}

#if USE_SPLINTERDB

static inline void sdb_init_w_rob_on_rem_inv(context_t * ctx, splinterdb* spl_handle,
                                             hr_inv_mes_t *inv_mes, hr_inv_t *inv) {
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    hr_w_rob_t *w_rob = (hr_w_rob_t *)
            get_fifo_push_slot(&hr_ctx->w_rob[inv_mes->m_id]);
    if (DEBUG_INVS)
        my_printf(cyan, "W_rob %u for inv from %u with l_id %lu -->%lu, inserted w_id = %u\n",
                  w_rob->id,  inv_mes->m_id, inv_mes->l_id,
                  inv_mes->l_id + inv_mes->coalesce_num,
                  hr_ctx->inserted_w_id[inv_mes->m_id]);
    if (ENABLE_ASSERTIONS) {
        assert(w_rob->w_state == INVALID);
        w_rob->l_id = hr_ctx->inserted_w_id[inv_mes->m_id];
    }
    w_rob->w_state = VALID;
    w_rob->inv_applied = true;
    hr_ctx->inserted_w_id[inv_mes->m_id]++;
    // w_rob capacity is already incremented when polling
    // to achieve back pressure at polling
    w_rob->version = inv->version;
    w_rob->m_id = inv_mes->m_id;
    w_rob->spl_handle = spl_handle;
    fifo_incr_push_ptr(&hr_ctx->w_rob[inv_mes->m_id]);
}

#endif /* if USE_SPLINTERDB */
static inline void insert_buffered_op(context_t *ctx,
                                      mica_op_t *kv_ptr,
                                      ctx_trace_op_t *op,
                                      bool inv)
{
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    buf_op_t *buf_op = (buf_op_t *) get_fifo_push_slot(hr_ctx->buf_ops);
    buf_op->op.opcode = op->opcode;
    buf_op->op.key = op->key;
    buf_op->op.session_id = op->session_id;
    buf_op->op.index_to_req_array = op->index_to_req_array;
    buf_op->kv_ptr = kv_ptr;


    if (inv) {
        buf_op->op.value_to_write = op->value_to_write;
    }
    else {
        buf_op->op.value_to_read = op->value_to_read;
    }

    fifo_incr_push_ptr(hr_ctx->buf_ops);
    fifo_increm_capacity(hr_ctx->buf_ops);
}

#if USE_SPLINTERDB

static inline void sdb_insert_buffered_ops(context_t *ctx, splinterdb* spl_handle, ctx_trace_op_t *op, bool inv) {
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    buf_op_t *buf_op = (buf_op_t *) get_fifo_push_slot(hr_ctx->buf_ops);
    buf_op->op.opcode = op->opcode;
    buf_op->op.key = op->key;
    buf_op->op.session_id = op->session_id;
    buf_op->op.index_to_req_array = op->index_to_req_array;
    buf_op->spl_handle = spl_handle;
    if (inv) {
        buf_op->op.value_to_write = op->value_to_write;
    } else {
        buf_op->op.value_to_read = op->value_to_read;
    }
    fifo_incr_push_ptr(hr_ctx->buf_ops);
    fifo_increm_capacity(hr_ctx->buf_ops);
}

#endif /* if USE_SPLINTERDB */
///* ---------------------------------------------------------------------------
////------------------------------ REQ PROCESSING -----------------------------
////---------------------------------------------------------------------------*/

static inline void hr_local_inv(context_t *ctx,
                                mica_op_t *kv_ptr,
                                ctx_trace_op_t *op,
                                uint32_t *write_i)
{
    bool success = false;
    uint64_t new_version;
    read_seqlock_lock_free(&kv_ptr->seqlock);
    if (kv_ptr->state != HR_W &&
        kv_ptr->state != HR_INV_T) {
        lock_seqlock(&kv_ptr->seqlock);
        {
            //! todo: values are written here
            if (kv_ptr->state != HR_W &&
                kv_ptr->state != HR_INV_T) {
                kv_ptr->state = HR_W;
                kv_ptr->version++;
                new_version = kv_ptr->version;
                kv_ptr->m_id = ctx->m_id;
                memcpy(kv_ptr->value, op->value_to_write, VALUE_SIZE);
                success = true;
            }
        }
        unlock_seqlock(&kv_ptr->seqlock);
    }

    if (success) {
        init_w_rob_on_loc_inv(ctx, kv_ptr, op, new_version, *write_i);
        if (INSERT_WRITES_FROM_KVS)
            od_insert_mes(ctx, INV_QP_ID, (uint32_t) INV_SIZE, 1, false, op, 0, 0);
        else {
            hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
            hr_ctx->ptrs_to_inv->ptr_to_ops[*write_i] = (hr_inv_t *) op;
            (*write_i)++;
        }
    }
    else {
        insert_buffered_op(ctx, kv_ptr, op, true);
    }
}

#if USE_SPLINTERDB
static inline void stbetree_insert(context_t *ctx, splinterdb* spl_handle, ctx_trace_op_t *op, uint64_t new_version,
                                   uint32_t *write_i) {
    bool success = false;
    char *val1 = (char*) (op->value_to_write);
    char* val2 = (char*) (op->value_to_write);
    slice key   = slice_create((size_t)strlen(val1), val1);
    slice value = slice_create((size_t)strlen(val2), val2);
    //! insert
    splinterdb_insert(spl_handle, key, value);
    success = true;
    if (success) {
        //! something is happening here
        sdb_init_w_rob_on_loc_inv(ctx, spl_handle, op, 0, *write_i);
        if (INSERT_WRITES_FROM_KVS)
            od_insert_mes(ctx, INV_QP_ID, (uint32_t) INV_SIZE, 1, false, op, 0, 0);
        else {
            hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
            hr_ctx->ptrs_to_inv->ptr_to_ops[*write_i] = (hr_inv_t *) op;
            (*write_i)++;
        }
    } else {
        sdb_insert_buffered_ops(ctx, spl_handle, op, true);
    }
}

//! Own API for range queries. SpliterDB provides a basic impl
//! for range queries having no upper bound. This impl
//! requires an upper bound.
int splinterdb_range_query(splinterdb* spl_handle, uint8_t* range_start, uint8_t* range_end) {
  //! Implementing our own range query API. Cool stuff!
  //! Pointer to iterator
  splinterdb_iterator *it = NULL;
  //! Create slices for our start and end keys.
  char start[4], end[4];
  void* end_ptr;
  sprintf(start, "%hhn", range_start);
  slice start_slice, end_slice;
  if (range_end == NULL) {
    //! no upper bound. Iterate over all keys;
    start_slice = slice_create((size_t)strlen(start), start);
    //! not creating any slice for upper bound.
    end_ptr == NULL;
  } else {
    sprintf(end, "%hhn", range_end);
    start_slice = slice_create((size_t)strlen(start), start);
    end_slice = slice_create((size_t)strlen(end), end);
  }
  end_ptr = &end;
  int rc = splinterdb_iterator_init(spl_handle, &it, start_slice);
  uint32_t count = 0;
  //! Use implementation of iterator from SplinterDB example.
  //! Now we add upper bound as well.
  for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
    if (end_ptr != NULL) {
      //! check if we reached our upper bound;
      slice key, value;
      splinterdb_iterator_get_current(it, &key, &value);
      char* data = slice_data(value);
      //! If we reached our upper bound, break.
      if (strcmp(data, end) == 0) {
        count++;
        break;
      } 
    }
    else count++;    
  }
  rc = splinterdb_iterator_status(it);
  assert(rc == 0);
  splinterdb_iterator_deinit(it);
  return rc;
}



static inline void stbetree_read(context_t *ctx, splinterdb* spl_handle, ctx_trace_op_t *op) {
    bool success = false;
    if (ENABLE_ASSERTIONS) {
        assert(op->value_to_read != NULL);
        assert(spl_handle != NULL);
    }
    splinterdb_lookup_result result;
    splinterdb_lookup_result_init(spl_handle, &result, 0, NULL);
    char key[4];
    sprintf(key, "%hhn", op->value_to_read);
    slice key_slice = slice_create((size_t)strlen(key), key);
    int rc = splinterdb_lookup(spl_handle, key_slice, &result);
    //! handling scenarios where key does or does not exist
    success == !rc ? true : false;
    //! if we succeed
    if (success) {
        hr_ctx_t *hr_ctx = (hr_ctx_t*) ctx->appl_ctx;
        signal_completion_to_client(op->session_id, op->index_to_req_array, ctx->t_id);
        hr_ctx->all_sessions_stalled = false;
        hr_ctx->stalled[op->session_id] = false;
    } else {
        sdb_insert_buffered_ops(ctx, spl_handle, op, false);
    }
}
#endif
static inline void hr_rem_inv(context_t *ctx,
                              mica_op_t *kv_ptr,
                              hr_inv_mes_t *inv_mes,
                              hr_inv_t *inv)
{
    bool inv_applied = false;
    compare_t comp = compare_flat_ts(inv->version, inv_mes->m_id,
                                     kv_ptr->version, kv_ptr->m_id);

    if (comp ==  GREATER) {
        lock_seqlock(&kv_ptr->seqlock);
        comp = compare_flat_ts(inv->version, inv_mes->m_id,
                               kv_ptr->version, kv_ptr->m_id);
        if (comp == GREATER) {
            if (kv_ptr->state != HR_W) {
                kv_ptr->state = HR_INV;
            }
            else kv_ptr->state = HR_INV_T;
            kv_ptr->version = inv->version;
            kv_ptr->m_id = inv_mes->m_id;
            memcpy(kv_ptr->value, inv->value, VALUE_SIZE);
            inv_applied = true;
        }
        unlock_seqlock(&kv_ptr->seqlock);
    }

    init_w_rob_on_rem_inv(ctx, kv_ptr, inv_mes, inv, inv_applied);
}

#if USE_SPLINTERDB

static inline void sdb_hr_rem_inv(context_t *ctx, splinterdb* spl_handle, hr_inv_mes_t* inv_mes, hr_inv_t *inv) {
    sdb_init_w_rob_on_rem_inv(ctx, spl_handle, inv_mes, inv);
}
#endif /* if USE_SPLINTERDB */


static inline void hr_loc_read(context_t *ctx,
                               mica_op_t *kv_ptr,
                               ctx_trace_op_t *op)
{
    if (ENABLE_ASSERTIONS) {
        assert(op->value_to_read != NULL);
        assert(kv_ptr != NULL);
    }
    bool success = false;
    uint32_t debug_cntr = 0;
    uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr->seqlock);
    do {
        debug_stalling_on_lock(&debug_cntr, "local read", ctx->t_id);
        if (kv_ptr->state == HR_V) {
            memcpy(op->value_to_read, kv_ptr->value, (size_t) VALUE_SIZE);
            success = true;
        }
    } while (!(check_seqlock_lock_free(&kv_ptr->seqlock, &tmp_lock)));

    if (success) {
        hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
        signal_completion_to_client(op->session_id, op->index_to_req_array, ctx->t_id);
        hr_ctx->all_sessions_stalled = false;
        hr_ctx->stalled[op->session_id] = false;
    }
    else insert_buffered_op(ctx, kv_ptr, op, false);
}

//! todo
static inline void handle_trace_reqs(context_t *ctx,
                                     mica_op_t *kv_ptr,
                                     ctx_trace_op_t *op,
                                     uint32_t *write_i,
                                     uint16_t op_i)
{
    if (op->opcode == KVS_OP_GET) {
        hr_loc_read(ctx, kv_ptr, op);
    }
    else if (op->opcode == KVS_OP_PUT) {
        hr_local_inv(ctx, kv_ptr, op,  write_i);

    }
    else if (ENABLE_ASSERTIONS) {
        my_printf(red, "wrong Opcode in cache: %d, req %d \n", op->opcode, op_i);
        assert(0);
    }
}

#if USE_SPLINTERDB

static inline void handle_trace_reqs_stb(context_t *ctx, splinterdb *spl_handle, ctx_trace_op_t *op,
                                         uint32_t *write_i, uint16_t op_i) {
    if (op->opcode == KVS_OP_GET) {
        stbetree_read(ctx, spl_handle, op);
    } else if (op->opcode == KVS_OP_PUT) {
        stbetree_insert(ctx, spl_handle, op, 0, write_i);
    } else if (op->opcode == KVS_OP_RANGE) {
        //! range queries go here.   
        splinterdb_range_query(spl_handle, op->range_start, op->range_end);
    } else if (ENABLE_ASSERTIONS) {
        my_printf(red, "wrong Opcode in cache: %d, req %d \n", op->opcode, op_i);
        assert(0);
    }
}
#endif /* if USE_SPLINTERDB */

///* ---------------------------------------------------------------------------
////------------------------------ KVS_API -----------------------------
////---------------------------------------------------------------------------*/

inline void hr_KVS_batch_op_trace(context_t *ctx, uint16_t op_num, kvs_t* kvs)
{
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    ctx_trace_op_t *op = hr_ctx->ops;
    uint16_t op_i;
    uint32_t buf_ops_num = hr_ctx->buf_ops->capacity;
    uint32_t write_i = 0;
    if (ENABLE_ASSERTIONS) {
        assert(op != NULL);
        assert(op_num > 0 && op_num <= HR_TRACE_BATCH);
    }
#if USE_MICA
    unsigned int bkt[HR_TRACE_BATCH];
    struct mica_bkt *bkt_ptr[HR_TRACE_BATCH];
    unsigned int tag[HR_TRACE_BATCH];
    mica_op_t *kv_ptr[HR_TRACE_BATCH];	/* Ptr to KV item in log */
    for(op_i = 0; op_i < op_num; op_i++) {
        KVS_locate_one_bucket(op_i, bkt, &op[op_i].key, bkt_ptr, tag, kv_ptr, KVS);
    }
    KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);
    for (op_i = 0; op_i < buf_ops_num; ++op_i) {
        buf_op_t *buf_op = (buf_op_t *) get_fifo_pull_slot(hr_ctx->buf_ops);
        check_state_with_allowed_flags(3, buf_op->op.opcode, KVS_OP_PUT, KVS_OP_GET);
        // handle_trace_reqs_stb(ctx, spl_handle, &buf_op->op, &write_i, op_i);
        handle_trace_reqs(ctx, buf_op->kv_ptr, &buf_op->op, &write_i, op_i);
        fifo_incr_pull_ptr(hr_ctx->buf_ops);
        fifo_decrem_capacity(hr_ctx->buf_ops);
    }
    for(op_i = 0; op_i < op_num; op_i++) {
        //! this is just checking for the keys
        od_KVS_check_key(kv_ptr[op_i], op[op_i].key, op_i);
        handle_trace_reqs(ctx, kv_ptr[op_i], &op[op_i], &write_i, op_i);
    }
#endif /* if USE_MICA */
#if USE_BPLUS
     for (op_i = 0; op_i < buf_ops_num; ++op_i) {
         buf_op_t *buf_op = (buf_op_t *) get_fifo_pull_slot(hr_ctx->buf_ops);
         check_state_with_allowed_flags(3, buf_op->op.opcode, KVS_OP_PUT, KVS_OP_GET, KVS_OP_RANGE);
         handle_trace_reqs_bt(ctx, kvs->tree, &buf_op->op, &write_i, op_i);
         fifo_incr_pull_ptr(hr_ctx->buf_ops);
         fifo_decrem_capacity(hr_ctx->buf_ops);
     }
     for (op_i = 0; op_i < op_num; op_i++) {
         handle_trace_reqs_bt(ctx, kvs->tree, &op[op_i], &write_i, op_i);
     }
#endif /* if USE_BPLUS */
#if USE_SPLINTERDB
  for (op_i = 0; op_i < buf_ops_num; ++op_i) {
    buf_op_t *buf_op = (buf_op_t *) get_fifo_pull_slot(hr_ctx->buf_ops);
    check_state_with_allowed_flags(3, buf_op->op.opcode, KVS_OP_PUT, KVS_OP_GET, KVS_OP_RANGE);
    handle_trace_reqs_stb(ctx, kvs->spl_handle, &buf_op->op, &write_i, op_i);
    // handle_trace_reqs(ctx, buf_op->kv_ptr, &buf_op->op, &write_i, op_i);
    fifo_incr_pull_ptr(hr_ctx->buf_ops);
    fifo_decrem_capacity(hr_ctx->buf_ops);
  }
  for (op_i = 0; op_i < op_num; op_i++) {
    handle_trace_reqs_stb(ctx, kvs->spl_handle, &op[op_i], &write_i, op_i);
  }
#endif /* if USE_SPLINTERDB */
  if (!INSERT_WRITES_FROM_KVS)
    hr_ctx->ptrs_to_inv->polled_invs = (uint16_t) write_i;

}

#if USE_SPLINTERDB

inline void hr_sdb_batch_op_trace(context_t *ctx, uint16_t op_num, splinterdb* spl_handle) {
    hr_ctx_t *hr_ctx = (hr_ctx_t*) ctx->appl_ctx;
    ctx_trace_op_t *op = hr_ctx->ops;
    uint16_t op_i;
    uint32_t write_i = 0;
    uint32_t buf_ops_num = hr_ctx->buf_ops->capacity;
    if (ENABLE_ASSERTIONS) {
        assert(op != NULL);
        assert(op_num > 0 && op_num <= HR_TRACE_BATCH);
    }
    for (op_i = 0; op_i < buf_ops_num; ++op_i) {
        buf_op_t *buf_op = (buf_op_t *) get_fifo_pull_slot(hr_ctx->buf_ops);
        check_state_with_allowed_flags(3, buf_op->op.opcode, KVS_OP_PUT, KVS_OP_GET);
        handle_trace_reqs_stb(ctx, spl_handle, &buf_op->op, &write_i, op_i);
        // handle_trace_reqs(ctx, buf_op->kv_ptr, &buf_op->op, &write_i, op_i);
        fifo_incr_pull_ptr(hr_ctx->buf_ops);
        fifo_decrem_capacity(hr_ctx->buf_ops);
    }
    for (op_i = 0; op_i < op_num; op_i++) {
        handle_trace_reqs_stb(ctx, spl_handle, &op[op_i], &write_i, op_i);
    }
    if (!INSERT_WRITES_FROM_KVS)
        hr_ctx->ptrs_to_inv->polled_invs = (uint16_t) write_i;
}
#endif /* if USE_SPLINTERDB */


inline void hr_KVS_batch_op_invs(context_t *ctx, kvs_t* kvs)
{
      hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    ptrs_to_inv_t *ptrs_to_inv = hr_ctx->ptrs_to_inv;
    hr_inv_mes_t **inv_mes = hr_ctx->ptrs_to_inv->ptr_to_mes;
    hr_inv_t **invs = ptrs_to_inv->ptr_to_ops;
    uint16_t op_num = ptrs_to_inv->polled_invs;
    uint16_t op_i;
    if (ENABLE_ASSERTIONS) {
        assert(invs != NULL);
        assert(op_num > 0 && op_num <= MAX_INCOMING_INV);
    }
#if USE_MICA
    unsigned int bkt[MAX_INCOMING_INV];
    struct mica_bkt *bkt_ptr[MAX_INCOMING_INV];
    unsigned int tag[MAX_INCOMING_INV];
    mica_op_t *kv_ptr[MAX_INCOMING_INV];	/* Ptr to KV item in log */
    for(op_i = 0; op_i < op_num; op_i++) {
        KVS_locate_one_bucket(op_i, bkt, &invs[op_i]->key, bkt_ptr, tag, kv_ptr, KVS);
    }
    KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

    for(op_i = 0; op_i < op_num; op_i++) {
        od_KVS_check_key(kv_ptr[op_i], invs[op_i]->key, op_i);
        hr_rem_inv(ctx, kv_ptr[op_i], inv_mes[op_i], invs[op_i]);
    }
#endif /* if USE_MICA */
#if USE_BPLUS
     for (op_i = 0; op_i < op_num; op_i++) {
         bt_hr_rem_inv(ctx, kvs->tree, inv_mes[op_i], invs[op_i]);
     }
#endif /* if USE_BPLUS */
#if USE_SPLINTERDB
  for (op_i = 0; op_i < op_num; op_i++) {
    sdb_hr_rem_inv(ctx, kvs->spl_handle, inv_mes[op_i], invs[op_i]);
  }
  #endif /* if USE_SPLINTERDB */
}

#if USE_BPLUS

inline void hr_sdb_batch_op_invs(context_t *ctx, splinterdb* spl_handle) {
    hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
    ptrs_to_inv_t *ptrs_to_inv = hr_ctx->ptrs_to_inv;
    hr_inv_mes_t **inv_mes = hr_ctx->ptrs_to_inv->ptr_to_mes;
    hr_inv_t **invs = ptrs_to_inv->ptr_to_ops;
    uint16_t op_num = ptrs_to_inv->polled_invs;
    uint16_t op_i;
    if (ENABLE_ASSERTIONS) {
        assert(invs != NULL);
        assert(op_num > 0 && op_num <= MAX_INCOMING_INV);
    }
    for (op_i = 0; op_i < op_num; op_i++) {
        sdb_hr_rem_inv(ctx, spl_handle, inv_mes[op_i], invs[op_i]);
    }
}

// Binding the Btree commands to the KVS

// static inline void bt_init_w_rob_on_loc_inv(context_t *ctx, BtDb *bt,
//                                          ctx_trace_op_t *op, uint64_t new_version, uint32_t write_i) {
static inline void bt_init_w_rob_on_loc_inv(context_t *ctx, bp_db_t* tree,
                                         ctx_trace_op_t *op, uint64_t new_version, uint32_t write_i) {
     hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
     hr_w_rob_t *w_rob = (hr_w_rob_t *)
             get_fifo_push_relative_slot(hr_ctx->loc_w_rob, write_i);
     if (ENABLE_ASSERTIONS) {
         assert(w_rob->w_state == INVALID);
         w_rob->l_id = hr_ctx->inserted_w_id[ctx->m_id];
     }
     w_rob->version = new_version;
     w_rob->tree = tree;
     w_rob->val_len = op->val_len;
     w_rob->sess_id = op->session_id;
     w_rob->w_state = SEMIVALID;
     if (ENABLE_ASSERTIONS)
         assert(hr_ctx->stalled[w_rob->sess_id]);
     if (DEBUG_INVS)
         my_printf(cyan, "W_rob insert sess %u write %lu, w_rob_i %u\n",
                   w_rob->sess_id, w_rob->l_id,
                   hr_ctx->loc_w_rob->push_ptr);
     fifo_increm_capacity(hr_ctx->loc_w_rob);
 }

//  static inline void bt_insert_buffered_ops(context_t *ctx, BtDb *bt, ctx_trace_op_t *op, bool inv) {
 static inline void bt_insert_buffered_ops(context_t *ctx, bp_db_t* tree, ctx_trace_op_t *op, bool inv) {
     hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
     buf_op_t *buf_op = (buf_op_t *) get_fifo_push_slot(hr_ctx->buf_ops);
     buf_op->op.opcode = op->opcode;
     buf_op->op.key = op->key;
     buf_op->op.session_id = op->session_id;
     buf_op->op.index_to_req_array = op->index_to_req_array;
     buf_op->tree = tree;
     if (inv) {
         buf_op->op.value_to_write = op->value_to_write;
     } else {
         buf_op->op.value_to_read = op->value_to_read;
     }
     fifo_incr_push_ptr(hr_ctx->buf_ops);
     fifo_increm_capacity(hr_ctx->buf_ops);
 }

 static inline void bt_insert(context_t *ctx, bp_db_t *tree, ctx_trace_op_t *op, uint64_t new_version,
                                    uint32_t *write_i) {
    bool success = false;
    my_printf(yellow, "Values: %d\n", op->value_to_write);
    char* key = (char*)(op->value_to_write);
    char* value = (char*)(op->value_to_write);
    printf("Initiating insert with key and value.\n");
    int return_value_from_insert = bp_sets(tree, key, value);
    printf("Done with insert.\n");
    success = (return_value_from_insert == 0);
     if (success) {
         //! something is happening here
         printf("Success.\n");
         bt_init_w_rob_on_loc_inv(ctx, tree, op, 0, *write_i);
         if (INSERT_WRITES_FROM_KVS)
             od_insert_mes(ctx, INV_QP_ID, (uint32_t) INV_SIZE, 1, false, op, 0, 0);
         else {
             hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
             hr_ctx->ptrs_to_inv->ptr_to_ops[*write_i] = (hr_inv_t *) op;
             (*write_i)++;
         }
     } else {
         bt_insert_buffered_ops(ctx, tree, op, true);
     }
 }

// Adding range query support for b+tree here


static inline void bt_range_query(context_t *ctx, bp_db_t *tree, ctx_trace_op_t *op) {
     bool success = false;
     if (ENABLE_ASSERTIONS) {
         assert(op->value_to_read != NULL);
         assert(&tree != NULL);
     }
    char* start = (char*)(op->value_to_read);
    char* end = (char*)(op->value_to_read + 16);
    bp_range_cb cb_value;
  int return_values_from_range_query = bp_get_ranges(tree, start, end, NULL, NULL);

     //! handling scenarios where key does or does not exist
    //  success = val == op->value_to_read ? false : true;
    success = (return_values_from_range_query == 0);
     //! if we succeed
     if (success) {
         hr_ctx_t *hr_ctx = (hr_ctx_t*) ctx->appl_ctx;
         signal_completion_to_client(op->session_id, op->index_to_req_array, ctx->t_id);
         hr_ctx->all_sessions_stalled = false;
         hr_ctx->stalled[op->session_id] = false;
     } else {
         bt_insert_buffered_ops(ctx, tree, op, false);
     }
 }



//  static inline void bt_read(context_t *ctx, BtDb *bt, ctx_trace_op_t *op) {
 static inline void bt_read(context_t *ctx, bp_db_t *tree, ctx_trace_op_t *op) {
     bool success = false;
     if (ENABLE_ASSERTIONS) {
         assert(op->value_to_read != NULL);
         assert(&tree != NULL);
     }
    char* key = (char*)(op->value_to_read);
    bp_value_t bp_value;
    int return_value_from_read = bp_gets(tree, key, &bp_value);

     //! handling scenarios where key does or does not exist
    //  success = val == op->value_to_read ? false : true;
    success = (return_value_from_read == 0);
     //! if we succeed
     if (success) {
         hr_ctx_t *hr_ctx = (hr_ctx_t*) ctx->appl_ctx;
         signal_completion_to_client(op->session_id, op->index_to_req_array, ctx->t_id);
         hr_ctx->all_sessions_stalled = false;
         hr_ctx->stalled[op->session_id] = false;
     } else {
         bt_insert_buffered_ops(ctx, tree, op, false);
     }
 }

//  static inline void handle_trace_reqs_bt(context_t *ctx, BtDb *bt, ctx_trace_op_t *op,
//                                           uint32_t *write_i, uint16_t op_i) {
 static inline void handle_trace_reqs_bt(context_t *ctx, bp_db_t *tree, ctx_trace_op_t *op,
                                          uint32_t *write_i, uint16_t op_i) {
     if (op->opcode == KVS_OP_GET) {
         bt_read(ctx, tree, op);
     } else if (op->opcode == KVS_OP_PUT) {
         bt_insert(ctx, tree, op, 0, write_i);
     } else if (op->opcode == KVS_OP_RANGE) {
        //call range query function here 
        bt_range_query(ctx, tree, op);
     } else if (ENABLE_ASSERTIONS) {
         my_printf(red, "Wrong Opcode in cache: %d, req %d \n", op->opcode, op_i);
         assert(0);
     }
 }

//  static inline void bt_init_w_rob_on_rem_inv(context_t * ctx, BtDb *bt,
//                                               hr_inv_mes_t *inv_mes, hr_inv_t *inv) {
 static inline void bt_init_w_rob_on_rem_inv(context_t * ctx, bp_db_t* tree,
                                              hr_inv_mes_t *inv_mes, hr_inv_t *inv) {
     hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
     hr_w_rob_t *w_rob = (hr_w_rob_t *)
             get_fifo_push_slot(&hr_ctx->w_rob[inv_mes->m_id]);
     if (DEBUG_INVS)
         my_printf(cyan, "W_rob %u for inv from %u with l_id %lu -->%lu, inserted w_id = %u\n",
                   w_rob->id,  inv_mes->m_id, inv_mes->l_id,
                   inv_mes->l_id + inv_mes->coalesce_num,
                   hr_ctx->inserted_w_id[inv_mes->m_id]);
     if (ENABLE_ASSERTIONS) {
         assert(w_rob->w_state == INVALID);
         w_rob->l_id = hr_ctx->inserted_w_id[inv_mes->m_id];
     }
     w_rob->w_state = VALID;
     w_rob->inv_applied = true;
     hr_ctx->inserted_w_id[inv_mes->m_id]++;
     // w_rob capacity is already incremented when polling
     // to achieve back pressure at polling
     
      w_rob->version = inv->version;
      w_rob->m_id = inv_mes->m_id;
      w_rob->tree = tree;

     fifo_incr_push_ptr(&hr_ctx->w_rob[inv_mes->m_id]);
 }

//  static inline void bt_hr_rem_inv(context_t *ctx, BtDb *bt, hr_inv_mes_t* inv_mes, hr_inv_t *inv) {
 static inline void bt_hr_rem_inv(context_t *ctx, bp_db_t* tree, hr_inv_mes_t* inv_mes, hr_inv_t *inv) {
     bt_init_w_rob_on_rem_inv(ctx, tree, inv_mes, inv);
 }

//  inline void hr_bt_batch_op_trace(context_t *ctx, uint16_t op_num, BtDb *bt) {
 inline void hr_bt_batch_op_trace(context_t *ctx, uint16_t op_num, bp_db_t* tree) {
     hr_ctx_t *hr_ctx = (hr_ctx_t*) ctx->appl_ctx;
     ctx_trace_op_t *op = hr_ctx->ops;
     uint16_t op_i;
     uint32_t write_i = 0;
     uint32_t buf_ops_num = hr_ctx->buf_ops->capacity;
     if (ENABLE_ASSERTIONS) {
         assert(op != NULL);
         assert(op_num > 0 && op_num <= HR_TRACE_BATCH);
     }
     for (op_i = 0; op_i < buf_ops_num; ++op_i) {
         buf_op_t *buf_op = (buf_op_t *) get_fifo_pull_slot(hr_ctx->buf_ops);
         check_state_with_allowed_flags(3, buf_op->op.opcode, KVS_OP_PUT, KVS_OP_GET);
         handle_trace_reqs_bt(ctx, tree, &buf_op->op, &write_i, op_i);
         fifo_incr_pull_ptr(hr_ctx->buf_ops);
         fifo_decrem_capacity(hr_ctx->buf_ops);
     }
     for (op_i = 0; op_i < op_num; op_i++) {
         handle_trace_reqs_bt(ctx, tree, &op[op_i], &write_i, op_i);
     }
     if (!INSERT_WRITES_FROM_KVS)
         hr_ctx->ptrs_to_inv->polled_invs = (uint16_t) write_i;
 }

//  inline void hr_bt_batch_op_invs(context_t *ctx, BtDb *bt) {
 inline void hr_bt_batch_op_invs(context_t *ctx, bp_db_t* tree) {
     hr_ctx_t *hr_ctx = (hr_ctx_t *) ctx->appl_ctx;
     ptrs_to_inv_t *ptrs_to_inv = hr_ctx->ptrs_to_inv;
     hr_inv_mes_t **inv_mes = hr_ctx->ptrs_to_inv->ptr_to_mes;
     hr_inv_t **invs = ptrs_to_inv->ptr_to_ops;
     uint16_t op_num = ptrs_to_inv->polled_invs;
     uint16_t op_i;
     if (ENABLE_ASSERTIONS) {
         assert(invs != NULL);
         assert(op_num > 0 && op_num <= MAX_INCOMING_INV);
     }
     for (op_i = 0; op_i < op_num; op_i++) {
         bt_hr_rem_inv(ctx, tree, inv_mes[op_i], invs[op_i]);
     }
 }
#endif /* if USE_BPLUS */

