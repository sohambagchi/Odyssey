//
// Created by vasilis on 20/08/20.
//

#ifndef ODYSSEY_DR_UTIL_H
#define ODYSSEY_DR_UTIL_H

#include "dr_config.h"
#include "od_network_context.h"
#include "od_init_func.h"
#include <dr_inline_util.h>
#include "../../../odlib/include/trace/od_trace_util.h"

atomic_uint_fast64_t global_w_id, committed_global_w_id;

static void dr_static_assert_compile_parameters()
{

  emphatic_print(green, "DERECHO");
  static_assert(PREP_SIZE == sizeof(dr_prepare_t));
  static_assert(PREP_SEND_SIZE == sizeof(dr_prep_mes_t));
}

static void dr_init_globals()
{
  global_w_id = 0;
  committed_global_w_id = 0;
}


static void dr_init_functionality(int argc, char *argv[])
{
  od_generic_static_assert_compile_parameters();
  dr_static_assert_compile_parameters();
  od_generic_init_globals(QP_NUM);
  dr_init_globals();
  od_handle_program_inputs(argc, argv);
}


static void dr_qp_meta_mfs(context_t *ctx)
{
  mf_t *mfs = calloc(QP_NUM, sizeof(mf_t));

  mfs[PREP_QP_ID].recv_handler = prepare_handler;
  mfs[PREP_QP_ID].send_helper = send_prepares_helper;
  mfs[PREP_QP_ID].insert_helper = insert_prep_help;


  mfs[ACK_QP_ID].recv_handler = ack_handler;
  mfs[ACK_QP_ID].send_helper = send_acks_helper;

  mfs[COM_QP_ID].recv_handler = dr_commit_handler;
  mfs[COM_QP_ID].send_helper = dr_send_commits_helper;

  ctx_set_qp_meta_mfs(ctx, mfs);
  free(mfs);
}


static void dr_init_send_fifos(context_t *ctx)
{
  fifo_t *send_fifo = ctx->qp_meta[COM_QP_ID].send_fifo;
  ctx_com_mes_t *commits = (ctx_com_mes_t *) send_fifo->fifo;

  for (uint32_t i = 0; i < COMMIT_FIFO_SIZE; i++) {
    commits[i].opcode = COMMIT_OP;
    commits[i].m_id = ctx->m_id;
  }

  ctx_ack_mes_t *ack_send_buf = (ctx_ack_mes_t *) ctx->qp_meta[ACK_QP_ID].send_fifo->fifo; //calloc(MACHINE_NUM, sizeof(ctx_ack_mes_t));
  assert(ctx->qp_meta[ACK_QP_ID].send_fifo->max_byte_size == CTX_ACK_SIZE * MACHINE_NUM);
  memset(ack_send_buf, 0, ctx->qp_meta[ACK_QP_ID].send_fifo->max_byte_size);
  for (int i = 0; i < MACHINE_NUM; i++) {
    ack_send_buf[i].m_id = (uint8_t) machine_id;
    ack_send_buf[i].opcode = OP_ACK;
  }

  dr_prep_mes_t *preps = (dr_prep_mes_t *) ctx->qp_meta[PREP_QP_ID].send_fifo->fifo;
  for (int i = 0; i < PREP_FIFO_SIZE; i++) {
    preps[i].opcode = KVS_OP_PUT;
    preps[i].m_id = ctx->m_id;
    for (uint16_t j = 0; j < PREP_COALESCE; j++) {
      //preps[i].prepare[j].opcode = KVS_OP_PUT;
     // preps[i].prepare[j].val_len = VALUE_SIZE >> SHIFT_BITS;
    }
  }
}

static void dr_init_qp_meta(context_t *ctx)
{
  per_qp_meta_t *qp_meta = ctx->qp_meta;
///
  create_per_qp_meta(&qp_meta[PREP_QP_ID], MAX_PREP_WRS,
                     MAX_RECV_PREP_WRS, SEND_BCAST_RECV_BCAST, RECV_REQ,
                     ACK_QP_ID,
                     REM_MACH_NUM, REM_MACH_NUM, PREP_BUF_SLOTS,
                     PREP_RECV_SIZE, PREP_SEND_SIZE, ENABLE_MULTICAST, ENABLE_MULTICAST,
                     DR_PREP_MCAST_QP, 0, PREP_FIFO_SIZE,
                     PREP_CREDITS, PREP_MES_HEADER,
                     "send preps", "recv preps");


  create_ack_qp_meta(&qp_meta[ACK_QP_ID],
                     PREP_QP_ID, REM_MACH_NUM,
                     REM_MACH_NUM, PREP_CREDITS);

  create_per_qp_meta(&qp_meta[COM_QP_ID], COM_WRS,
                     RECV_COM_WRS, SEND_BCAST_RECV_BCAST, RECV_SEC_ROUND,
                     COM_QP_ID,
                     REM_MACH_NUM, REM_MACH_NUM, COM_BUF_SLOTS,
                     CTX_COM_RECV_SIZE, CTX_COM_SEND_SIZE, ENABLE_MULTICAST, ENABLE_MULTICAST,
                     COM_MCAST_QP, 0, COMMIT_FIFO_SIZE,
                     COM_CREDITS, CTX_COM_SEND_SIZE,
                     "send commits", "recv commits");


  dr_qp_meta_mfs(ctx);
  dr_init_send_fifos(ctx);

}

static void* set_up_dr_ctx(context_t *ctx)
{
  dr_ctx_t* dr_ctx = (dr_ctx_t*) calloc(1,sizeof(dr_ctx_t));

  dr_ctx->w_rob = calloc(W_ROB_SIZE, sizeof(w_rob_t));
  dr_ctx->loc_w_rob_ptr = fifo_constructor(DR_PENDING_WRITES, sizeof(w_rob_t*), false, 0, 1);

  dr_ctx->index_to_req_array = (uint32_t *) calloc(SESSIONS_PER_THREAD, sizeof(uint32_t));

  dr_ctx->stalled = (bool *) malloc(SESSIONS_PER_THREAD * sizeof(bool));

  dr_ctx->ops = (ctx_trace_op_t *) calloc((size_t) DR_TRACE_BATCH, sizeof(ctx_trace_op_t));
  dr_ctx->resp = (dr_resp_t*) calloc((size_t) DR_TRACE_BATCH, sizeof(dr_resp_t));
  for(int i = 0; i <  DR_TRACE_BATCH; i++) dr_ctx->resp[i].type = EMPTY;

  for (int i = 0; i < SESSIONS_PER_THREAD; i++) dr_ctx->stalled[i] = false;
  for (int i = 0; i < W_ROB_SIZE; i++) {
    w_rob_t *w_rob = &dr_ctx->w_rob[i];
    w_rob->w_state = INVALID;
    w_rob->g_id = 0;
    w_rob->w_rob_id = (uint16_t) i;
  }

  dr_ctx->gid_rob_arr = calloc(1, sizeof(gid_rob_arr_t));
  dr_ctx->gid_rob_arr->gid_rob = calloc(GID_ROB_NUM, sizeof(gid_rob_t));

  uint32_t thread_offset =  (uint32_t) (ctx->t_id * PER_THREAD_G_ID_BATCH);
  for (int i = 0; i < GID_ROB_NUM; ++i) {
    dr_ctx->gid_rob_arr->gid_rob[i].base_gid =
      (i * PER_MACHINE_G_ID_BATCH) + thread_offset;
    dr_ctx->gid_rob_arr->gid_rob[i].valid = calloc(GID_ROB_SIZE, sizeof(bool));
    dr_ctx->gid_rob_arr->gid_rob[i].rob_id = (uint32_t) i;
    dr_ctx->gid_rob_arr->gid_rob[i].empty = true;
    //printf("Wrkr %u GIF_ROB %d: base = %lu \n", ctx->t_id, i, dr_ctx->gid_rob_arr[i].base_gid);
  }


  if (!ENABLE_CLIENTS)
    dr_ctx->trace = trace_init(ctx->t_id);

  return (void *) dr_ctx;
}


typedef struct stats {
  double batch_size_per_thread[WORKERS_PER_MACHINE];
  double com_batch_size[WORKERS_PER_MACHINE];
  double prep_batch_size[WORKERS_PER_MACHINE];
  double ack_batch_size[WORKERS_PER_MACHINE];
  double write_batch_size[WORKERS_PER_MACHINE];
  double stalled_gid[WORKERS_PER_MACHINE];
  double stalled_ack_prep[WORKERS_PER_MACHINE];
  double stalled_com_credit[WORKERS_PER_MACHINE];


  double total_reqs[WORKERS_PER_MACHINE];


  double preps_sent[WORKERS_PER_MACHINE];
  double acks_sent[WORKERS_PER_MACHINE];
  double coms_sent[WORKERS_PER_MACHINE];

  double received_coms[WORKERS_PER_MACHINE];
  double received_acks[WORKERS_PER_MACHINE];
  double received_preps[WORKERS_PER_MACHINE];

  double write_ratio_per_client[WORKERS_PER_MACHINE];
} all_stats_t;

#endif //ODYSSEY_DR_UTIL_H
