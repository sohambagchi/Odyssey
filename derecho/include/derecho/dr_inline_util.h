//
// Created by vasilis on 20/08/20.
//
#ifndef ODYSSEY_DR_INLINE_UTIL_H
#define ODYSSEY_DR_INLINE_UTIL_H

#include "od_network_context.h"
#include "dr_reserve_stations.h"
#include "dr_kvs_util.h"

/* ---------------------------------------------------------------------------
//------------------------------TRACE --------------------------------
//---------------------------------------------------------------------------*/


static inline void dr_batch_from_trace_to_KVS(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *ops = dr_ctx->ops;
  dr_resp_t *resp = dr_ctx->resp;
  trace_t *trace = dr_ctx->trace;

  uint16_t op_i = 0;
  int working_session = -1;
  if (all_sessions_are_stalled(ctx, dr_ctx->all_sessions_stalled,
                               &dr_ctx->stalled_sessions_dbg_counter))
    return;
  if (!od_find_starting_session(ctx, dr_ctx->last_session,
                                dr_ctx->stalled, &working_session)) return;

  bool passed_over_all_sessions = false;

  /// main loop
  while (op_i < DR_TRACE_BATCH && !passed_over_all_sessions) {

    od_fill_trace_op(ctx, &trace[dr_ctx->trace_iter], &ops[op_i], working_session);
    dr_ctx->stalled[working_session] = ops[op_i].opcode == KVS_OP_PUT;

    passed_over_all_sessions =
        od_find_next_working_session(ctx, &working_session,
                                     dr_ctx->stalled,
                                     dr_ctx->last_session,
                                     &dr_ctx->all_sessions_stalled);
    resp[op_i].type = EMPTY;
    if (!ENABLE_CLIENTS) {
      dr_ctx->trace_iter++;
      if (trace[dr_ctx->trace_iter].opcode == NOP) dr_ctx->trace_iter = 0;
    }
    op_i++;
  }
  //printf("Session %u pulled: ops %u, req_array ptr %u \n",
  //       working_session, op_i, ops[0].index_to_req_array);
  dr_ctx->last_session = (uint16_t) working_session;
  t_stats[ctx->t_id].total_reqs += op_i;
  dr_KVS_batch_op_trace(dr_ctx, op_i, ctx->t_id);

  for (uint16_t i = 0; i < op_i; i++) {
    // my_printf(green, "After: OP_i %u -> session %u \n", i, *(uint32_t *) &ops[i]);
    if (resp[i].type == KVS_MISS)  {
      my_printf(green, "KVS %u: bkt %u, server %u, tag %u \n", i,
                ops[i].key.bkt, ops[i].key.server, ops[i].key.tag);
      assert(false);
      continue;
    }
    else if (resp[i].type == KVS_LOCAL_GET_SUCCESS) {
      signal_completion_to_client(ops[i].session_id, ops[i].index_to_req_array, ctx->t_id);
    }
    else { // WRITE
      od_insert_mes(ctx, PREP_QP_ID, (uint32_t) PREP_SIZE, 1, false, &ops[i], LOCAL_PREP, 0);
    }
  }
}

//
///* ---------------------------------------------------------------------------
////------------------------------UNICASTS -----------------------------
////---------------------------------------------------------------------------*/
static inline void send_acks_helper(context_t *ctx)
{
  ctx_refill_recvs(ctx, COM_QP_ID);
}

///* ---------------------------------------------------------------------------
////------------------------------ GID HANDLING -----------------------------
////---------------------------------------------------------------------------*/

// Propagates Updates that have seen all acks to the KVS
static inline void dr_propagate_updates(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;

  w_rob_t *ptrs_to_w_rob[DR_UPDATE_BATCH];
  uint16_t sess_to_free[SESSIONS_PER_THREAD];
  uint16_t update_op_i = 0, local_op_i = 0;
  // remember the starting point to use it when writing the KVS
  uint64_t committed_g_id = (uint64_t) atomic_load_explicit(&committed_global_w_id, memory_order_relaxed);
  dr_increase_counter_if_waiting_for_commit(dr_ctx, committed_g_id, ctx->t_id);
  while(!get_g_id_rob_pull(dr_ctx)->empty) {
    w_rob_t *w_rob = get_w_rob_from_g_rob_pull(dr_ctx);
    gid_rob_t *gid_rob = get_g_id_rob_pull(dr_ctx);
    if (w_rob->w_state == INVALID)
      print_g_id_rob(ctx, gid_rob->rob_id);
    check_state_with_allowed_flags(4, w_rob->w_state, VALID, SENT, READY);
    if (w_rob->w_state != READY) break;

    check_when_waiting_for_gid(ctx, w_rob, committed_g_id);
    if (w_rob->g_id != committed_g_id) break;
    else {
      if (ENABLE_ASSERTIONS) dr_ctx->wait_for_gid_dbg_counter = 0;
      if (w_rob->is_local) {
        if (ENABLE_ASSERTIONS) assert(local_op_i < SESSIONS_PER_THREAD);
        sess_to_free[local_op_i] = w_rob->session_id;
        local_op_i++;
      }
      if (DEBUG_COMMITS)
        my_printf(yellow, "Wrkr %u committing g_id %lu, update_op_i %u "
                    "from gid_rob %u, resetting w_rob %u \n",
                  ctx->t_id, w_rob->g_id, update_op_i,
                  gid_rob->rob_id, w_rob->w_rob_id);
      //print_w_rob_entry(ctx, w_rob);
      reset_gid_rob(ctx);
      reset_w_rob(w_rob);
      ptrs_to_w_rob[update_op_i] = w_rob;
      update_op_i++;
      committed_g_id++;
    }
  }

  if (update_op_i > 0) {
    dr_KVS_batch_op_updates(ptrs_to_w_rob, update_op_i);
    reset_dr_ctx_meta(ctx, sess_to_free, local_op_i);
    atomic_store_explicit(&committed_global_w_id, committed_g_id,
                          memory_order_relaxed);
  }
}



///* ---------------------------------------------------------------------------
////------------------------------POLL HANDLERS -----------------------------
////---------------------------------------------------------------------------*/

static inline void dr_insert_commits_on_receiving_ack(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  if (ENABLE_ASSERTIONS) {
    assert(dr_ctx->loc_w_rob_ptr->capacity > 0);
  }
  uint16_t com_num = 0;

  w_rob_t *w_rob = *(w_rob_t **) get_fifo_pull_slot(dr_ctx->loc_w_rob_ptr);
  if (ENABLE_ASSERTIONS) assert(w_rob != NULL);
  while(w_rob->w_state == READY) {
    com_num++;
    fifo_decrem_capacity(dr_ctx->loc_w_rob_ptr);
    fifo_incr_pull_ptr(dr_ctx->loc_w_rob_ptr);
    if (dr_ctx->loc_w_rob_ptr->capacity == 0) break;
    w_rob = *(w_rob_t **) get_fifo_pull_slot(dr_ctx->loc_w_rob_ptr);
  }
  ctx_insert_commit(ctx, COM_QP_ID, com_num, dr_ctx->committed_w_id);
  dr_ctx->committed_w_id += com_num;
}


static inline void dr_apply_acks(context_t *ctx,
                                 ctx_ack_mes_t *ack,
                                 uint32_t ack_num, uint32_t ack_ptr)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  uint64_t pull_lid = dr_ctx->committed_w_id;
  for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {

    if (ENABLE_ASSERTIONS && (ack_ptr == dr_ctx->loc_w_rob_ptr->push_ptr)) {
      uint32_t origin_ack_ptr = (uint32_t) (ack_ptr - ack_i + DR_PENDING_WRITES) % DR_PENDING_WRITES;
      my_printf(red, "Origin ack_ptr %u/%u, acks %u/%u, w_pull_ptr %u, w_push_ptr % u, capacity %u \n",
                origin_ack_ptr,  (dr_ctx->loc_w_rob_ptr->pull_ptr + (ack->l_id - pull_lid)) % DR_PENDING_WRITES,
                ack_i, ack_num, dr_ctx->loc_w_rob_ptr->pull_ptr, dr_ctx->loc_w_rob_ptr->push_ptr, dr_ctx->loc_w_rob_ptr->capacity);
    }

    w_rob_t *w_rob = *(w_rob_t **) get_fifo_slot(dr_ctx->loc_w_rob_ptr, ack_ptr);
    w_rob->acks_seen++;
    if (w_rob->acks_seen == REMOTE_QUORUM) {
      if (ENABLE_ASSERTIONS) qp_meta->outstanding_messages--;
//        printf("Worker %d valid ack %u/%u write at ptr %d with g_id %lu is ready \n",
//               t_id, ack_i, ack_num,  ack_ptr, dr_ctx->g_id[ack_ptr]);
      w_rob->w_state = READY;

    }
    MOD_INCR(ack_ptr, DR_PENDING_WRITES);
  }
}

//
static inline bool ack_handler(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_ack_mes_ud_t *incoming_acks = (volatile ctx_ack_mes_ud_t *) recv_fifo->fifo;
  ctx_ack_mes_t *ack = (ctx_ack_mes_t *) &incoming_acks[recv_fifo->pull_ptr].ack;
  uint32_t ack_num = ack->ack_num;
  uint64_t l_id = ack->l_id;
  uint64_t pull_lid = dr_ctx->committed_w_id; // l_id at the pull pointer
  uint32_t ack_ptr; // a pointer in the FIFO, from where ack should be added
  //dr_check_polled_ack_and_print(ack, ack_num, pull_lid, recv_fifo->pull_ptr, ctx->t_id);

  ctx_increase_credits_on_polling_ack(ctx, ACK_QP_ID, ack);

  per_qp_meta_t *com_qp_meta = &ctx->qp_meta[COM_QP_ID];
  com_qp_meta->credits[ack->m_id] = com_qp_meta->max_credits;


  if ((dr_ctx->loc_w_rob_ptr->capacity == 0 ) ||
      (pull_lid >= l_id && (pull_lid - l_id) >= ack_num))
    return true;

  dr_check_ack_l_id_is_small_enough(ctx, ack);
  ack_ptr = ctx_find_when_the_ack_points_acked(ack, dr_ctx->loc_w_rob_ptr, pull_lid, &ack_num);

  // Apply the acks that refer to stored writes
  dr_apply_acks(ctx, ack, ack_num, ack_ptr);
  dr_insert_commits_on_receiving_ack(ctx);

  return true;
}


//
static inline bool prepare_handler(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile dr_prep_mes_ud_t *incoming_preps = (volatile dr_prep_mes_ud_t *) recv_fifo->fifo;
  dr_prep_mes_t *prep_mes = (dr_prep_mes_t *) &incoming_preps[recv_fifo->pull_ptr].prepare;

  uint8_t coalesce_num = prep_mes->coalesce_num;

  dr_check_polled_prep_and_print(ctx, prep_mes);

  ctx_ack_insert(ctx, ACK_QP_ID, coalesce_num,  prep_mes->l_id, prep_mes->m_id);


  for (uint8_t prep_i = 0; prep_i < coalesce_num; prep_i++) {
    dr_check_prepare_and_print(ctx, prep_mes, prep_i);
    fill_dr_ctx_entry(ctx, prep_mes, prep_i);
  }

  if (ENABLE_ASSERTIONS) prep_mes->opcode = 0;

  return true;
}


static inline bool dr_commit_handler(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_com_mes_ud_t *incoming_coms = (volatile ctx_com_mes_ud_t *) recv_fifo->fifo;

  ctx_com_mes_t *com = (ctx_com_mes_t *) &incoming_coms[recv_fifo->pull_ptr].com;
  uint32_t com_num = com->com_num;

  uint64_t l_id = com->l_id;
  //uint64_t pull_lid = dr_ctx->local_w_id; // l_id at the pull pointer
  dr_check_polled_commit_and_print(ctx, com, recv_fifo->pull_ptr);
  // This must always hold: l_id >= pull_lid,
  // because we need the commit to advance the pull_lid
  //uint16_t com_ptr = (uint16_t)
  //  ((dr_ctx->w_rob->pull_ptr + (l_id - pull_lid)) % FLR_PENDING_WRITES);

  uint64_t g_id = get_g_id_from_l_id(com->l_id, ctx->t_id, com->m_id);
  /// loop through each commit
  for (uint16_t com_i = 0; com_i < com_num; com_i++) {
    w_rob_t * w_rob = get_w_rob_with_gid(ctx, g_id);
    if (DEBUG_COMMITS)
      my_printf(yellow, "Wrkr %u, Com %u/%u, g_id %lu \n",
                ctx->t_id, com_i, com_num, g_id);

    if (dr_write_not_ready(ctx, com, w_rob, g_id, com_i)) {
      return false;
    }

    w_rob->w_state = READY;
    g_id++;
    if (g_id % PER_THREAD_G_ID_BATCH == 0)
      g_id = get_g_id_from_l_id(com->l_id + com_i + 1, ctx->t_id, com->m_id);
  } ///

  if (ENABLE_ASSERTIONS) com->opcode = 0;

  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].received_coms += com_num;
    t_stats[ctx->t_id].received_coms_mes_num++;
  }

  return true;
}

/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS -----------------------------
//---------------------------------------------------------------------------*/
static inline void dr_send_commits_helper(context_t *ctx)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  ctx_com_mes_t *com_mes = (ctx_com_mes_t *) get_fifo_pull_slot(send_fifo);

  if (DEBUG_COMMITS)
    my_printf(green, "Wrkr %u, Broadcasting commit %u, lid %lu, com_num %u \n",
              ctx->t_id, com_mes->opcode, com_mes->l_id, com_mes->com_num);

  dr_checks_and_stats_on_bcasting_commits(send_fifo, com_mes, ctx->t_id);
}



//
static inline void send_prepares_helper(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  if (DEBUG_PREPARES)
    printf("Wrkr %d has %u bcasts to send credits %d\n", ctx->t_id,
           send_fifo->net_capacity, qp_meta->credits[1]);
  // Create the broadcast messages
  dr_prep_mes_t *prep_buf = (dr_prep_mes_t *) qp_meta->send_fifo->fifo;
  dr_prep_mes_t *prep = &prep_buf[send_fifo->pull_ptr];

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  prep->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  uint32_t backward_ptr = fifo_get_pull_backward_ptr(send_fifo);

  for (uint16_t i = 0; i < coalesce_num; i++) {
    w_rob_t *w_rob = *(w_rob_t **) get_fifo_slot_mod(dr_ctx->loc_w_rob_ptr, backward_ptr + i);
    if (ENABLE_ASSERTIONS) assert(w_rob->w_state == VALID);
    w_rob->w_state = SENT;
    if (DEBUG_PREPARES)
      printf("Prepare %d, g_id %lu, total message capacity %d\n", i, prep->prepare[i].g_id,
             slot_meta->byte_size);
  }

  if (DEBUG_PREPARES)
    my_printf(green, "Wrkr %d : I BROADCAST a prepare message %d of "
                "%u prepares with total w_size %u,  with  credits: %d, lid: %lu  \n",
              ctx->t_id, prep->opcode, coalesce_num, slot_meta->byte_size,
              qp_meta->credits[0], prep->l_id);
  dr_checks_and_stats_on_bcasting_prepares(ctx, coalesce_num);
}





static inline void dr_main_loop(context_t *ctx)
{
  if (ctx->t_id == 0) my_printf(yellow, "Derecho main loop \n");
  while(true) {

    dr_batch_from_trace_to_KVS(ctx);

    ctx_send_broadcasts(ctx, PREP_QP_ID);

    ctx_poll_incoming_messages(ctx, PREP_QP_ID);

    od_send_acks(ctx, ACK_QP_ID);

    ctx_poll_incoming_messages(ctx, ACK_QP_ID);

    dr_propagate_updates(ctx);

    ctx_send_broadcasts(ctx, COM_QP_ID);

    ctx_poll_incoming_messages(ctx, COM_QP_ID);

  }
}

#endif //ODYSSEY_DR_INLINE_UTIL_H
