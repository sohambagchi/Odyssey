// Created by vasilis on 20/08/20.
//

#ifndef ODYSSEY_DR_CONFIG_H
#define ODYSSEY_DR_CONFIG_H

#include "od_fifo.h"
#include <dr_messages.h>
#include <od_network_context.h>


void dr_stats(stats_ctx_t *ctx);

#define DISABLE_UPDATING_KVS 0

#define USE_QUORUM 1





///Static Partitioning of g_ids
#define PER_THREAD_G_ID_BATCH SESSIONS_PER_THREAD
#define PER_MACHINE_G_ID_BATCH (WORKERS_PER_MACHINE * PER_THREAD_G_ID_BATCH)
#define TOTAL_G_ID_BATCH (MACHINE_NUM * PER_MACHINE_G_ID_BATCH)

#define QP_NUM 3
#define PREP_QP_ID 0
#define ACK_QP_ID 1
#define COM_QP_ID 2

#define DR_PREP_MCAST_QP 0
#define COM_MCAST_QP 1

//#define REMOTE_W_SLOTS (2 * REM_MACH_NUM * PREP_CREDITS * PREP_COALESCE)
#define DR_PENDING_WRITES ((SESSIONS_PER_THREAD + 1))
#define COMMIT_FIFO_SIZE 1

#define GID_ROB_NUM (MACHINE_NUM * 5)
#define GID_ROB_SIZE (PER_THREAD_G_ID_BATCH)
#define W_ROB_SIZE (GID_ROB_NUM * GID_ROB_SIZE)
#define G_ID_BASE_JUMP (GID_ROB_NUM * PER_MACHINE_G_ID_BATCH)

#define DR_TRACE_BATCH SESSIONS_PER_THREAD
#define DR_UPDATE_BATCH (W_ROB_SIZE)

/*------------------------------------------------
 * ----------------KVS----------------------------
 * ----------------------------------------------*/
#define MICA_VALUE_SIZE (VALUE_SIZE + (FIND_PADDING_CUST_ALIGN(VALUE_SIZE, 32)))
#define MICA_OP_SIZE_  (28 + ((MICA_VALUE_SIZE)))
#define MICA_OP_PADDING_SIZE  (FIND_PADDING(MICA_OP_SIZE_))
#define MICA_OP_SIZE  (MICA_OP_SIZE_ + MICA_OP_PADDING_SIZE)
struct mica_op {
  // Cache-line -1
  uint8_t value[MICA_VALUE_SIZE];
  // Cache-line -2
  struct key key;
  seqlock_t seqlock;
  uint64_t g_id;
  uint32_t key_id; // strictly for debug
  uint8_t padding[MICA_OP_PADDING_SIZE];
};



/*------------------------------------------------
 * ----------------TRACE----------------------------
 * ----------------------------------------------*/

typedef struct dr_resp {
  uint8_t type;
} dr_resp_t;


/*------------------------------------------------
 * ----------------RESERVATION STATIONS-----------
 * ----------------------------------------------*/
typedef enum op_state {INVALID, VALID, SENT, READY, SEND_COMMITTS} w_state_t;
typedef enum {NOT_USED, LOCAL_PREP, REMOTE_WRITE} source_t;
//typedef struct ptrs_to_reads {
//  uint16_t polled_reads;
//  dr_read_t **ptr_to_ops;
//  dr_r_mes_t **ptr_to_r_mes;
//  bool *coalesce_r_rep;
//} ptrs_to_r_t;

typedef struct w_rob {
  uint64_t g_id;
  uint16_t session_id;

  w_state_t w_state;
  uint8_t m_id;
  uint8_t acks_seen;
  //uint32_t index_to_req_array;
  bool is_local;
  dr_prepare_t *ptr_to_op;
  mica_key_t key;
  uint8_t value[VALUE_SIZE];
  uint16_t w_rob_id;

} w_rob_t;


typedef struct r_rob {
  bool seen_larger_g_id;
  uint8_t opcode;
  mica_key_t key;
  uint8_t value[VALUE_SIZE]; //
  uint8_t *value_to_read;
  uint32_t state;
  uint32_t log_no;
  uint32_t val_len;
  uint32_t sess_id;
  uint64_t g_id;
  uint64_t l_id;
} r_rob_t ;


typedef struct gid_rob {
  //w_rob_t **w_rob;
  bool *valid;
  uint64_t base_gid;
  uint32_t rob_id;
  uint32_t first_valid;
  bool empty;
} gid_rob_t;

typedef struct g_id_rob_array {
  gid_rob_t *gid_rob;
  uint32_t pull_ptr;
  uint32_t size;
  //bool empty;
} gid_rob_arr_t;

// A data structute that keeps track of the outstanding writes
typedef struct dr_ctx {
  // reorder buffers
  //fifo_t *r_rob;
  /// This is a fifo of ptrs pointing to the local writes inside the w_rob,
  /// which is not a fifo
  fifo_t *loc_w_rob_ptr;
  w_rob_t *w_rob;
  gid_rob_arr_t *gid_rob_arr;


  trace_t *trace;
  uint32_t trace_iter;
  uint16_t last_session;

  ctx_trace_op_t *ops;
  dr_resp_t *resp;

  //ptrs_to_r_t *ptrs_to_r;
  uint64_t inserted_w_id;
  uint64_t committed_w_id;

  uint64_t local_r_id;

  uint32_t *index_to_req_array; // [SESSIONS_PER_THREAD]
  bool *stalled;

  bool all_sessions_stalled;

  uint32_t wait_for_gid_dbg_counter;
  uint32_t stalled_sessions_dbg_counter;
} dr_ctx_t;




/*------------------------------------------------
 * ----------------STATS----------------------------
 * ----------------------------------------------*/


typedef struct thread_stats { // 2 cache lines
  long long total_reqs;
  long long remotes_per_client;
  long long locals_per_client;

  long long preps_sent;
  long long acks_sent;
  long long coms_sent;
  long long writes_sent;
  uint64_t reads_sent;

  long long prep_sent_mes_num;
  long long acks_sent_mes_num;
  long long coms_sent_mes_num;
  long long writes_sent_mes_num;
  uint64_t reads_sent_mes_num;


  long long received_coms;
  long long received_acks;
  long long received_preps;
  long long received_writes;

  long long received_coms_mes_num;
  long long received_acks_mes_num;
  long long received_preps_mes_num;
  long long received_writes_mes_num;


  uint64_t batches_per_thread; // Leader only
  uint64_t total_writes; // Leader only

  uint64_t stalled_gid;
  uint64_t stalled_ack_prep;
  uint64_t stalled_com_credit;
  //long long unused[3]; // padding to avoid false sharing
} t_stats_t;

extern t_stats_t t_stats[WORKERS_PER_MACHINE];
extern atomic_uint_fast64_t global_w_id, committed_global_w_id;

#endif //ODYSSEY_DR_CONFIG_H
