//
// Created by vasilis on 16/07/2021.
//

#include <cp_main_loop.h>
#include <cp_netw_generic_util.h>
#include <cp_netw_debug.h>
#include <cp_core_interface.h>
#include <cp_kvs.h>

static inline void cp_apply_acks(context_t *ctx,
                                 ctx_ack_mes_t *ack)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  uint32_t ack_num = ack->ack_num;
  uint64_t pull_lid = cp_ctx->l_ids.applied_com_id;
  uint32_t ack_ptr =
      ctx_find_when_the_ack_points_acked(ack, cp_ctx->com_rob, pull_lid, &ack_num);

  for (uint32_t ack_i = 0; ack_i < ack_num; ack_i++) {
    cp_com_rob_t *com_rob = (cp_com_rob_t *) get_fifo_slot(cp_ctx->com_rob, ack_ptr);
    com_rob->acks_seen++;
    cp_check_ack_and_print(ctx, com_rob, ack, ack_i, ack_ptr, ack_num);
    if (com_rob->acks_seen == REMOTE_QUORUM) {
      act_on_quorum_of_commit_acks(cp_ctx->cp_core_ctx,
                                   com_rob->sess_id);
      com_rob->state = READY_COMMIT;
    }
    MOD_INCR(ack_ptr, COM_ROB_SIZE);
  }
}


static inline void increment_prop_acc_credits(context_t *ctx,
                                              cp_rmw_rep_mes_t *rep_mes,
                                              bool is_accept)
{
  ctx->qp_meta[is_accept ? ACC_QP_ID : PROP_QP_ID].credits[rep_mes->m_id]++;
}

static inline  void signal_in_progress_to_clts_when_filling(trace_op_t *op,
                                                            int working_session,
                                                            uint16_t t_id)
{
  if (ENABLE_CLIENTS) {
    signal_in_progress_to_client(op->session_id, op->index_to_req_array, t_id);
    if (ENABLE_ASSERTIONS) assert(interface[t_id].wrkr_pull_ptr[working_session] == op->index_to_req_array);
    MOD_INCR(interface[t_id].wrkr_pull_ptr[working_session], PER_SESSION_REQ_NUM);
  }
}


// Fill the trace_op to be passed to the KVS. Returns whether no more requests can be processed
static inline bool fill_trace_op(context_t *ctx,
                                 cp_ctx_t *cp_ctx,
                                 trace_op_t *op,
                                 trace_t *trace,
                                 int working_session,
                                 uint16_t t_id)
{
  create_inputs_of_op(&op->value_to_write, &op->value_to_read, &op->real_val_len,
                      &op->opcode, &op->index_to_req_array,
                      &op->key, op->value, trace, working_session, t_id);
  memcpy(&op->key, &cp_ctx->key_per_sess[working_session], sizeof(mica_key_t));

  if (!ENABLE_CLIENTS) check_trace_req(cp_ctx, trace, op, working_session, t_id);


  bool is_rmw = opcode_is_rmw(op->opcode);
  check_when_filling_op(cp_ctx, op, working_session, is_rmw);
  if (is_rmw && ENABLE_ALL_ABOARD) {
    op->attempt_all_aboard = ctx->q_info->missing_num == 0;
  }
  cp_ctx->stall_info.stalled[working_session] = true;
  op->session_id = (uint16_t) working_session;

  if (ENABLE_ASSERTIONS && DEBUG_SESSIONS)
    cp_ctx->debug_loop->ses_dbg->dbg_cnt[working_session] = 0;

  signal_in_progress_to_clts_when_filling(op, working_session, t_id);
}

static inline void batch_requests_to_KVS(context_t *ctx)
{

  cp_ctx_t* cp_ctx = (cp_ctx_t*) ctx->appl_ctx;
  trace_op_t *ops = cp_ctx->ops;
  trace_t *trace = cp_ctx->trace_info.trace;

  uint16_t writes_num = 0, reads_num = 0, op_i = 0;
  int working_session = -1;
  // if there are clients the "all_sessions_stalled" flag is not used,
  // so we need not bother checking it
  if (!ENABLE_CLIENTS && cp_ctx->stall_info.all_stalled) {
    return;
  }
  for (uint16_t i = 0; i < SESSIONS_PER_THREAD; i++) {
    uint16_t sess_i = (uint16_t)((cp_ctx->trace_info.last_session + i) % SESSIONS_PER_THREAD);
    if (od_pull_request_from_this_session(cp_ctx->stall_info.stalled[sess_i], sess_i, ctx->t_id)) {
      working_session = sess_i;
      break;
    }
  }
  //printf("working session = %d\n", working_session);
  if (ENABLE_CLIENTS) {
    if (working_session == -1) return;
  }
  else if (ENABLE_ASSERTIONS ) assert(working_session != -1);

  bool passed_over_all_sessions = false;
  while (op_i < MAX_OP_BATCH && !passed_over_all_sessions) {
    fill_trace_op(ctx, cp_ctx, &ops[op_i], &trace[cp_ctx->trace_info.trace_iter],
                  working_session, ctx->t_id);

    // Find out next session to work on
    passed_over_all_sessions =
        od_find_next_working_session(ctx, &working_session,
                                     cp_ctx->stall_info.stalled,
                                     cp_ctx->trace_info.last_session,
                                     &cp_ctx->stall_info.all_stalled);
    if (!ENABLE_CLIENTS) {
      cp_ctx->trace_info.trace_iter++;
      if (trace[cp_ctx->trace_info.trace_iter].opcode == NOP) cp_ctx->trace_info.trace_iter = 0;
    }
    op_i++;
  }

  cp_ctx->trace_info.last_session = (uint16_t) working_session;
  t_stats[ctx->t_id].total_reqs += op_i;
  cp_KVS_batch_op_trace(op_i, ops, cp_ctx, ctx->t_id);
  for (uint16_t i = 0; i < op_i; i++) {
    insert_rmw(cp_ctx->cp_core_ctx, &ops[i], ctx->t_id);
  }
}


/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS ----------------------------------
//---------------------------------------------------------------------------*/

inline void send_props_helper(context_t *ctx)
{
  send_prop_checks(ctx);
}

inline void send_accs_helper(context_t *ctx)
{
  send_acc_checks(ctx);
}


inline void cp_send_coms_helper(context_t *ctx)
{
  send_com_checks(ctx);
}


/* ---------------------------------------------------------------------------
//------------------------------ UNICASTS-------------------------------------
//---------------------------------------------------------------------------*/

inline void rmw_prop_rep_helper(context_t *ctx)
{
  ctx_refill_recvs(ctx, ACC_QP_ID);
  send_rmw_rep_checks(ctx);
}


inline void cp_send_ack_helper(context_t *ctx)
{
  //ctx_refill_recvs(ctx, COM_QP_ID);
}

inline void cp_send_ack_debug(context_t *ctx,
                              void* ack_ptr,
                              uint32_t m_i)
{
  checks_stats_prints_when_sending_acks(ctx,
                                        (ctx_ack_mes_t *) ack_ptr,
                                        (uint8_t) m_i);
}

/* ---------------------------------------------------------------------------
//------------------------------ POLLING-------------------------------------
//---------------------------------------------------------------------------*/


inline bool prop_recv_handler(context_t* ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PROP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_prop_mes_ud_t *incoming_props =
      (volatile cp_prop_mes_ud_t *) recv_fifo->fifo;
  cp_prop_mes_t *prop_mes = (cp_prop_mes_t *)
                            &incoming_props[recv_fifo->pull_ptr].prop_mes;

  check_when_polling_for_props(ctx, prop_mes);

  uint8_t coalesce_num = prop_mes->coalesce_num;

  cp_ptrs_to_ops_t *ptrs_to_prop = cp_ctx->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_prop->polled_ops = 0;

  for (uint16_t i = 0; i < coalesce_num; i++) {
    cp_prop_t *prop = &prop_mes->prop[i];
    check_state_with_allowed_flags(2, prop->opcode, PROPOSE_OP);
    fill_ptr_to_ops_for_reps(ptrs_to_prop, (void *) prop,
                             (void *) prop_mes, i);
  }

  return true;
}

inline bool acc_recv_handler(context_t* ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACC_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_acc_mes_ud_t *incoming_accs = (volatile cp_acc_mes_ud_t *) recv_fifo->fifo;
  cp_acc_mes_t *acc_mes = (cp_acc_mes_t *) &incoming_accs[recv_fifo->pull_ptr].acc_mes;

  check_when_polling_for_accs(ctx, acc_mes);

  uint8_t coalesce_num = acc_mes->coalesce_num;

  cp_ptrs_to_ops_t *ptrs_to_acc = cp_ctx->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_acc->polled_ops = 0;

  for (uint16_t i = 0; i < coalesce_num; i++) {
    cp_acc_t *acc = &acc_mes->acc[i];
    check_state_with_allowed_flags(2, acc->opcode, ACCEPT_OP);
    fill_ptr_to_ops_for_reps(ptrs_to_acc, (void *) acc,
                             (void *) acc_mes, i);
  }
  return true;
}

inline bool cp_rmw_rep_recv_handler(context_t* ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[RMW_REP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_rmw_rep_mes_ud_t *incoming_reps =
      (volatile cp_rmw_rep_mes_ud_t *) recv_fifo->fifo;
  cp_rmw_rep_mes_t *rep_mes =
      (cp_rmw_rep_mes_t *) &incoming_reps[recv_fifo->pull_ptr].rep_mes;

  bool is_accept = rep_mes->opcode == ACCEPT_REPLY;
  increment_prop_acc_credits(ctx, rep_mes, is_accept);
  handle_rmw_rep_replies(cp_ctx->cp_core_ctx, rep_mes, is_accept);
  return true;
}



 inline bool cp_com_recv_handler(context_t* ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COM_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cp_com_mes_ud_t *com_mes_ud = (volatile cp_com_mes_ud_t *) get_fifo_pull_slot(recv_fifo);
  cp_com_mes_t *com_mes = (cp_com_mes_t *) &com_mes_ud->com_mes;

  check_when_polling_for_coms(ctx, com_mes);
  //printf("received commit \n");

  uint8_t coalesce_num = com_mes->coalesce_num;
  bool can_send_acks = ctx_ack_insert(ctx, ACK_QP_ID, coalesce_num,  com_mes->l_id, com_mes->m_id);
  if (!can_send_acks) return false;

  cp_ptrs_to_ops_t *ptrs_to_com = cp_ctx->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_com->polled_ops = 0;
  uint32_t byte_ptr = 0;
  for (uint16_t i = 0; i < coalesce_num; i++) {
    //cp_com_t *com = &com_mes->com[i];
    cp_com_t *com = (cp_com_t *)(((void *) com_mes->com) + byte_ptr);
    byte_ptr += com->opcode == COMMIT_OP ? COM_SIZE : COMMIT_NO_VAL_SIZE ;
    check_state_with_allowed_flags(3, com->opcode, COMMIT_OP, COMMIT_OP_NO_VAL);
    ptrs_to_com->ptr_to_ops[ptrs_to_com->polled_ops] = (void *) com;
    ptrs_to_com->ptr_to_mes[ptrs_to_com->polled_ops] = (void *) com_mes;
    ptrs_to_com->polled_ops++;
  }
  return true;
}

inline bool cp_ack_recv_handler(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_ack_mes_ud_t *incoming_acks = (volatile ctx_ack_mes_ud_t *) recv_fifo->fifo;
  ctx_ack_mes_t *ack = (ctx_ack_mes_t *) &incoming_acks[recv_fifo->pull_ptr].ack;

  ctx_increase_credits_on_polling_ack(ctx, ACK_QP_ID, ack);

  if (od_is_ack_too_old(ack, cp_ctx->com_rob, cp_ctx->l_ids.applied_com_id))
    return true;

  cp_apply_acks(ctx, ack);
  return true;
}


static inline void cp_bookkeep_commits(context_t *ctx)
{
  uint16_t com_num = 0;
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  cp_com_rob_t *com_rob = (cp_com_rob_t *) get_fifo_pull_slot(cp_ctx->com_rob);

  while (com_rob->state == READY_COMMIT) {
    com_rob->state = INVALID;
    if (DEBUG_COMMITS)
      my_printf(green, "Commit sess %u commit %lu\n",
                com_rob->sess_id, cp_ctx->l_ids.applied_com_id + com_num);

    fifo_incr_pull_ptr(cp_ctx->com_rob);
    fifo_decrem_capacity(cp_ctx->com_rob);
    com_rob = (cp_com_rob_t *) get_fifo_pull_slot(cp_ctx->com_rob);
    com_num++;
  }
  cp_ctx->l_ids.applied_com_id += com_num;
}

static inline void inspect_rmws(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  cp_core_inspect_rmws(cp_ctx->cp_core_ctx);
}


/* ---------------------------------------------------------------------------
//------------------------------ COMMITTING-------------------------------------
//---------------------------------------------------------------------------*/


_Noreturn void cp_main_loop(context_t *ctx)
{
  cp_ctx_t *cp_ctx = (cp_ctx_t *) ctx->appl_ctx;
  cp_ctx->key_per_sess = calloc(SESSIONS_PER_THREAD, sizeof(mica_key_t));
  for (int i = 0; i < SESSIONS_PER_THREAD; i++){
    memcpy(&cp_ctx->key_per_sess[i], cp_ctx->trace_info.trace[i].key_hash, sizeof(mica_key_t));
  }
  while(true) {

    cp_checks_at_loop_start(ctx);

    batch_requests_to_KVS(ctx);
    ctx_send_broadcasts(ctx, PROP_QP_ID);
    for (uint16_t qp_i = 0; qp_i < QP_NUM; qp_i ++)
      ctx_poll_incoming_messages(ctx, qp_i);

    ctx_send_unicasts(ctx, RMW_REP_QP_ID);
    //ctx_send_unicasts(ctx, ACC_REP_QP_ID);
    od_send_acks(ctx, ACK_QP_ID);

    inspect_rmws(ctx);
    ctx_send_broadcasts(ctx, ACC_QP_ID);
    ctx_send_broadcasts(ctx, COM_QP_ID);
    cp_bookkeep_commits(ctx);
  }
}