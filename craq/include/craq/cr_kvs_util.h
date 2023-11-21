//
// Created by vasilis on 17/09/20.
//

#ifndef ODYSSEY_CR_KVS_UTIL_H
#define ODYSSEY_CR_KVS_UTIL_H

#include <od_network_context.h>
#include <od_netw_func.h>
#include "od_kvs.h"
#include "cr_config.h"
#include "cr_generic_util.h"

static inline void cr_check_opcode_is_read(ctx_trace_op_t *op,
                                           uint16_t op_i)
{
  if (ENABLE_ASSERTIONS && op[op_i].opcode != KVS_OP_GET) {
    my_printf(red, "wrong Opcode in cache: %d, req %d \n", op[op_i].opcode, op_i);
    assert(0);
  }
}

static inline void cr_insert_buffered_op(context_t *ctx,
                                          mica_op_t *kv_ptr,
                                          ctx_trace_op_t *op)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  cr_buf_op_t *buf_op = (cr_buf_op_t *) get_fifo_push_slot(cr_ctx->buf_reads);
  buf_op->op.opcode = op->opcode;
  buf_op->op.key = op->key;
  buf_op->op.session_id = op->session_id;
  buf_op->op.index_to_req_array = op->index_to_req_array;
  buf_op->kv_ptr = kv_ptr;
  buf_op->op.value_to_read = op->value_to_read;

  fifo_incr_push_ptr(cr_ctx->buf_reads);
  fifo_increm_capacity(cr_ctx->buf_reads);
  //printf("capacity %u\n", cr_ctx->buf_reads->capacity);
}


static inline void cr_rem_prep(context_t *ctx,
                               mica_op_t *kv_ptr,
                               cr_prep_t *prep)
{

  if (ENABLE_ASSERTIONS)
    assert(!is_head(ctx));

  lock_seqlock(&kv_ptr->seqlock);
  if (prep->version > kv_ptr->version) {
    if (ctx->m_id != CR_TAIL_NODE) kv_ptr->state = CR_INV;
    kv_ptr->version = prep->version;
    memcpy(kv_ptr->value, prep->value, VALUE_SIZE);
  }
  unlock_seqlock(&kv_ptr->seqlock);

  if (is_tail(ctx)) {
    if (ctx->m_id == prep->m_id) {
      cr_free_session(ctx, prep->sess_id);
    }
  }

}

static inline void cr_head_write(context_t *ctx,
                                 mica_op_t *kv_ptr,
                                 void *source,
                                 cr_w_rob_t *w_rob,
                                 source_t source_flag)
{
  uint8_t *value_ptr = source_flag == CR_LOCAL_PREP ?
                       ((ctx_trace_op_t *) source)->value_to_write :
                       ((cr_prep_t *) source)->value;
  if (ENABLE_ASSERTIONS)
    assert(is_head(ctx));

  lock_seqlock(&kv_ptr->seqlock);
  {
    kv_ptr->state = CR_INV;
    kv_ptr->version++;
    w_rob->version = kv_ptr->version;
    memcpy(kv_ptr->value, value_ptr, VALUE_SIZE);
  }
  unlock_seqlock(&kv_ptr->seqlock);
}

///* ---------------------------------------------------------------------------
////------------------------------ REQ PROCESSING -----------------------------
////---------------------------------------------------------------------------*/




static inline void cr_loc_or_rem_write_or_prep(context_t *ctx,
                                               mica_op_t *kv_ptr,
                                               void *source,
                                               source_t source_flag)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  cr_w_rob_t *w_rob = (cr_w_rob_t *)
    get_fifo_push_slot(cr_ctx->w_rob);

  switch (source_flag) {
    case CR_LOCAL_PREP:
    case STEERED_PREP:
      cr_head_write(ctx, kv_ptr,source, w_rob, source_flag);
      break;
    case CHAIN_PREP:
      cr_rem_prep(ctx, kv_ptr, (cr_prep_t *) source);
      break;
    case NOT_USED:
    default: if (ENABLE_ASSERTIONS) assert(false);
  }

  if (ctx->m_id != CR_TAIL_NODE) {
    if (ENABLE_ASSERTIONS) assert(w_rob->w_state == INVALID);
    //w_rob->owner_m_id = m_id;
    w_rob->kv_ptr = kv_ptr;
    w_rob->w_state = SEMIVALID;

    od_insert_mes(ctx, PREP_QP_ID, sizeof(cr_prep_t), 1,
                  false, source, source_flag, CHAIN_PREP_FIFO_ID);
  }
  //else printf("Reached Tail node \n");
}




static inline void cr_loc_read(context_t *ctx,
                               mica_op_t *kv_ptr,
                               ctx_trace_op_t *op)
{
  if (ENABLE_ASSERTIONS) {
    assert(op->value_to_read != NULL);
    assert(kv_ptr != NULL);
  }
  bool success = false;
  uint32_t debug_cntr = 0;
  uint64_t version;
  uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr->seqlock);
  do {
    debug_stalling_on_lock(&debug_cntr, "local read", ctx->t_id);
    if (kv_ptr->state == CR_V) {
      memcpy(op->value_to_read, kv_ptr->value, (size_t) VALUE_SIZE);
      success = true;
    }
    else if (CR_REMOTE_READS) {
      version = kv_ptr->version;
      memcpy(op->value_to_read, kv_ptr->value, (size_t) VALUE_SIZE);
    }
  } while (!(check_seqlock_lock_free(&kv_ptr->seqlock, &tmp_lock)));

  if (success) {
    //my_printf(green, "Sess %u success \n", op->session_id);
    cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
    signal_completion_to_client(op->session_id, op->index_to_req_array, ctx->t_id);
    cr_ctx->all_sessions_stalled = false;
    cr_ctx->stalled[op->session_id] = false;
  }
  else {
    if (CR_REMOTE_READS) {
      ctx->ctx_tmp->counter = version;
      od_insert_mes(ctx, R_QP_ID, sizeof(cr_read_t),
                    (uint32_t) R_REP_BIG_SIZE, false, op, NOT_USED, 0);
    }
    else
      cr_insert_buffered_op(ctx, kv_ptr, op);
  }
}


///* ---------------------------------------------------------------------------
////------------------------------ KVS_API -----------------------------
////---------------------------------------------------------------------------*/

static inline void cr_KVS_batch_op_trace(context_t *ctx, uint16_t op_num)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *op = cr_ctx->ops;
  uint16_t op_i;
  if (ENABLE_ASSERTIONS) {
    assert(op != NULL);
    assert(op_num <= CR_TRACE_BATCH);
    assert(op_num > 0 || cr_ctx->buf_reads->capacity > 0);
  }

  unsigned int bkt[CR_TRACE_BATCH];
  struct mica_bkt *bkt_ptr[CR_TRACE_BATCH];
  unsigned int tag[CR_TRACE_BATCH];
  mica_op_t *kv_ptr[CR_TRACE_BATCH];	/* Ptr to KV item in log */


  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &op[op_i].key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);


  uint32_t buf_ops_num = cr_ctx->buf_reads->capacity;
  for (op_i = 0; op_i < buf_ops_num; ++op_i) {
    cr_buf_op_t *buf_read = (cr_buf_op_t *) get_fifo_pull_slot(cr_ctx->buf_reads);
    check_state_with_allowed_flags(2, buf_read->op.opcode,  KVS_OP_GET);
    //my_printf(cyan, "Buffered op sess %u \n", buf_read->op.session_id);
    cr_loc_read(ctx, buf_read->kv_ptr, &buf_read->op);
    fifo_incr_pull_ptr(cr_ctx->buf_reads);
    fifo_decrem_capacity(cr_ctx->buf_reads);
  }


  for(op_i = 0; op_i < op_num; op_i++) {
    od_KVS_check_key(kv_ptr[op_i], op[op_i].key, op_i);

    if (op[op_i].opcode == KVS_OP_PUT) {
      cr_loc_or_rem_write_or_prep(ctx, kv_ptr[op_i], &op[op_i], CR_LOCAL_PREP);
    }
    else {
      //my_printf(yellow, "Sess %u starts read \n", op[op_i].session_id);
      cr_check_opcode_is_read(op, op_i);
      //if (CR_REMOTE_READS && !is_tail(ctx))
      //  cr_remote_read(ctx, kv_ptr[op_i], &op[op_i]);
      //else
        cr_loc_read(ctx, kv_ptr[op_i], &op[op_i]);
    }
  }
}

static inline void cr_KVS_batch_op_preps(context_t *ctx)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  cr_ptrs_to_op_t *ptrs_to_prep = cr_ctx->ptrs_to_ops;
  cr_prep_t **preps = (cr_prep_t **) ptrs_to_prep->ops;
  uint16_t op_num = ptrs_to_prep->op_num;

  uint16_t op_i;
  if (ENABLE_ASSERTIONS) {
    assert(preps != NULL);
    assert(op_num > 0 && op_num <= CR_MAX_INCOMING_PREP);
  }

  unsigned int bkt[CR_MAX_INCOMING_PREP];
  struct mica_bkt *bkt_ptr[CR_MAX_INCOMING_PREP];
  unsigned int tag[CR_MAX_INCOMING_PREP];
  mica_op_t *kv_ptr[CR_MAX_INCOMING_PREP];	/* Ptr to KV item in log */

  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &preps[op_i]->key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  for(op_i = 0; op_i < op_num; op_i++) {
    od_KVS_check_key(kv_ptr[op_i], preps[op_i]->key, op_i);
    cr_loc_or_rem_write_or_prep(ctx, kv_ptr[op_i], preps[op_i],
                                is_head(ctx) ? STEERED_PREP : CHAIN_PREP);
  }
}


static inline void cr_KVS_batch_op_reads(context_t *ctx)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  uint16_t op_i;  /* op_i is batch index */
  cr_ptrs_to_op_t *ptrs_to_r = cr_ctx->ptrs_to_ops;
  uint16_t op_num =  ptrs_to_r->op_num;
  if (op_num == 0) return;
  if (ENABLE_ASSERTIONS) {
    assert(op_num > 0 && op_num <= CR_MAX_INCOMING_R);
  }

  unsigned int bkt[CR_MAX_INCOMING_R];
  struct mica_bkt *bkt_ptr[CR_MAX_INCOMING_R];
  unsigned int tag[CR_MAX_INCOMING_R];
  mica_op_t *kv_ptr[CR_MAX_INCOMING_R];	/* Ptr to KV item in log */

  for(op_i = 0; op_i < op_num; op_i++) {
    cr_read_t *read = ptrs_to_r->ops[op_i];
    KVS_locate_one_bucket(op_i, bkt, &read->key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  for(op_i = 0; op_i < op_num; op_i++) {
    cr_read_t *read = ptrs_to_r->ops[op_i];
    od_KVS_check_key(kv_ptr[op_i], read->key, op_i);
    if (ENABLE_ASSERTIONS) assert(read->opcode == KVS_OP_GET);

    od_insert_mes(ctx, R_QP_ID, R_REP_SMALL_SIZE, 0,
                  !ptrs_to_r->coalesce[op_i],
                  (void *) kv_ptr[op_i], op_i, 0);

  }
}


#endif //ODYSSEY_CR_KVS_UTIL_H
