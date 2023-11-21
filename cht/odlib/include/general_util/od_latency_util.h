//
// Created by vasilis on 22/05/20.
//

#ifndef OD_LATENCY_UTIL_H
#define OD_LATENCY_UTIL_H

#include "od_top.h"
#include "od_generic_inline_util.h"

/* ---------------------------------------------------------------------------
//------------------------------ LATENCY MEASUREMENTS-------------------------
//---------------------------------------------------------------------------*/

static inline char *latency_req_to_str(req_type_t rt)
{
  switch (rt){
    case NO_REQ:
      return "NO_REQ";
    case RELEASE_REQ:
      return "RELEASE_REQ";
    case ACQUIRE_REQ:
      return "ACQUIRE_REQ";
    case WRITE_REQ:
      return "WRITE_REQ";
    case READ_REQ:
      return "READ_REQ";
    case RMW_REQ:
      return "RMW_REQ";
    default: assert(false);
  }
}


//Add latency to histogram (in microseconds)
static inline void bookkeep_latency(uint64_t useconds, req_type_t rt)
{

  check_state_with_allowed_flags(6, rt, RELEASE_REQ, ACQUIRE_REQ, READ_REQ, WRITE_REQ, RMW_REQ);
  latency_count.total_measurements++;
  latency_count.req_meas_num[rt]++;
  if (useconds > latency_count.max_req_lat[rt])
    latency_count.max_req_lat[rt] = (uint32_t) useconds;

  if (useconds > MAX_LATENCY)
    latency_count.requests[rt][LATENCY_BUCKETS]++;
  else
    latency_count.requests[rt][useconds / (MAX_LATENCY / LATENCY_BUCKETS)]++;
}




//
static inline void report_latency(latency_info_t* latency_info)
{
  struct timespec end;
  clock_gettime(CLOCK_MONOTONIC, &end);
  uint64_t useconds = (uint64_t)((end.tv_sec - latency_info->start.tv_sec) * MILLION) +
                 ((end.tv_nsec - latency_info->start.tv_nsec) / 1000);  //(end.tv_nsec - start->tv_nsec) / 1000;
  //if (ENABLE_ASSERTIONS) assert(useconds > 0);

  if (DEBUG_LATENCY) {
    printf("Latency of a req of type %s is %lu us, sess %u , measured reqs: %lu \n",
           latency_req_to_str(latency_info->measured_req_flag),
           useconds, latency_info->measured_sess_id,
           (latency_count.total_measurements + 1));
    //if (useconds > 1000)


  }
  bookkeep_latency(useconds, latency_info->measured_req_flag);
  latency_info->measured_req_flag = NO_REQ;
}

static inline req_type_t map_opcodes_to_req_type(uint8_t opcode)
{
  switch(opcode){
    case OP_RELEASE:
      return RELEASE_REQ;
    case OP_ACQUIRE:
      return ACQUIRE_REQ;
    case KVS_OP_GET:
      return READ_REQ;
    case KVS_OP_PUT:
      return WRITE_REQ;
    case FETCH_AND_ADD:
    case COMPARE_AND_SWAP_WEAK:
    case COMPARE_AND_SWAP_STRONG:
      return RMW_REQ;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}

// Necessary bookkeeping to initiate the latency measurement
static inline void start_measurement(latency_info_t* latency_info, uint32_t sess_id,
                                     uint8_t opcode, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS)assert(latency_info->measured_req_flag == NO_REQ);


  latency_info->measured_req_flag = map_opcodes_to_req_type(opcode);
  latency_info->measured_sess_id = sess_id;
  if (DEBUG_LATENCY)
    my_printf(green, "Measuring a req , opcode %s, sess_id %u,  flag %s op_i %d \n",
  					 opcode_to_str(opcode), sess_id, latency_req_to_str(latency_info->measured_req_flag),
            latency_info->measured_sess_id);
  if (ENABLE_ASSERTIONS) assert(latency_info->measured_req_flag != NO_REQ);

  clock_gettime(CLOCK_MONOTONIC, &latency_info->start);


}

//// A condition to be used to trigger periodic (but rare) measurements
//static inline bool trigger_measurement(uint16_t local_client_id)
//{
//  return t_stats[local_client_id].total_reqs % K_32 > 0 &&
//         t_stats[local_client_id].total_reqs % K_32 <= 500 &&
//         local_client_id == 0 && machine_id == MACHINE_NUM -1;
//}


#endif //OD_LATENCY_UTIL_H
