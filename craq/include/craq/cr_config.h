//
// Created by vasilis on 17/09/20.
//

#ifndef ODYSSEY_CR_CONFIG_H
#define ODYSSEY_CR_CONFIG_H


#include "od_fifo.h"
#include <cr_messages.h>
#include <od_network_context.h>


#define CR_HEAD_NODE 0
#define CR_TAIL_NODE (MACHINE_NUM - 1)



#define QP_NUM 3
#define PREP_QP_ID 0
#define ACK_QP_ID 1
#define R_QP_ID 2


#define CR_TRACE_BATCH SESSIONS_PER_THREAD
#define CR_PREP_POLL_LIMIT ((MACHINE_NUM ) * SESSIONS_PER_THREAD)
#define CR_W_ROB_SIZE ((MACHINE_NUM  + 1) * SESSIONS_PER_THREAD)
#define CR_PREP_FIFO_SIZE (CR_W_ROB_SIZE)
#define CR_MAX_INCOMING_PREP CR_W_ROB_SIZE
#define CR_MAX_INCOMING_R CR_W_ROB_SIZE

#define CR_REMOTE_READS 1

#define MAX_RECV_PREP_WRS (CR_MAX_INCOMING_PREP)
#define PREP_BUF_SLOTS (MAX_RECV_PREP_WRS)

#define R_FIFO_SIZE (SESSIONS_PER_THREAD + 1)



/// Non head nodes need to send-fifos for prepares
/// one for the chain propagation and one to send to the head
#define CHAIN_PREP_FIFO_ID 0
#define STEER_TO_HEAD_FIFO_ID 1

/*------------------------------------------------
 * ----------------KVS----------------------------
 * ----------------------------------------------*/
#define MICA_VALUE_SIZE (VALUE_SIZE + (FIND_PADDING_CUST_ALIGN(VALUE_SIZE, 32)))
#define MICA_OP_SIZE_ (32 + ((MICA_VALUE_SIZE)))
#define MICA_OP_PADDING_SIZE  (FIND_PADDING(MICA_OP_SIZE_))
#define MICA_OP_SIZE  (MICA_OP_SIZE_ + MICA_OP_PADDING_SIZE)


struct mica_op {
  uint8_t value[MICA_VALUE_SIZE];
  struct key key;
  seqlock_t seqlock;
  uint64_t version;
  uint8_t state;
  uint8_t unused[3];
  uint32_t key_id; // strictly for debug

  uint8_t padding[MICA_OP_PADDING_SIZE];
};


/*------------------------------------------------
 * -----------------------------------------------
 * ----------------------------------------------*/
typedef enum{CR_V = 0, CR_INV} key_state_t;


typedef enum op_state {INVALID, SEMIVALID,
  VALID, SENT,
  READY, SEMI_INVALID} cr_state_t;

typedef enum {NOT_USED, CR_LOCAL_PREP, STEERED_PREP, CHAIN_PREP} source_t;

typedef struct cr_r_rob {
  bool seen_larger_g_id;
  uint8_t opcode;
  mica_key_t key;
  //uint8_t value[VALUE_SIZE]; //
  uint8_t *value_to_read;
  uint32_t state;
  uint32_t log_no;
  uint32_t val_len;
  uint32_t sess_id;
  uint64_t version;
  uint64_t l_id;
} cr_r_rob_t;


typedef struct cr_w_rob {

  uint64_t l_id;
  uint64_t version;
  mica_op_t *kv_ptr;
  uint16_t sess_id;
  uint16_t id;

  cr_state_t w_state;
  uint8_t owner_m_id;
  uint8_t val_len;
} cr_w_rob_t;

typedef struct cr_buf_op {
  ctx_trace_op_t op;
  mica_op_t *kv_ptr;
} cr_buf_op_t;

typedef struct cr_ptrs_to_w {
  void **writes;
  uint16_t write_num;
} cr_ptrs_to_w_t;

typedef struct ptrs_to_prep {
  uint16_t op_num;
  void **ops;
  void **ptr_to_mes;
  bool *coalesce;
} cr_ptrs_to_op_t;

// A data structute that keeps track of the outstanding writes
typedef struct cr_ctx {
  // reorder buffers
  fifo_t *w_rob;
  fifo_t *r_rob;
  //fifo_t *loc_w_rob; //points in the w_rob


  cr_ptrs_to_op_t *ptrs_to_ops;

  trace_t *trace;
  uint32_t trace_iter;
  uint16_t last_session;
  uint8_t prev_node;
  uint8_t next_node;

  ctx_trace_op_t *ops;

  fifo_t *buf_reads;

  uint64_t inserted_w_id;
  uint64_t committed_w_id;
  uint64_t inserted_r_id;
  uint64_t committed_r_id;

  uint32_t *index_to_req_array; // [SESSIONS_PER_THREAD]
  bool *stalled;

  bool all_sessions_stalled;

  uint32_t stalled_sessions_dbg_counter;
} cr_ctx_t;


typedef struct thread_stats { // 2 cache lines
  long long total_reqs;
  long long remotes_per_client;
  long long locals_per_client;

  uint64_t steered_writes[MACHINE_NUM];

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
} thread_stats_t;

extern t_stats_t t_stats[WORKERS_PER_MACHINE];

#endif //ODYSSEY_CR_CONFIG_H
