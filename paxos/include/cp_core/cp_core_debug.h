#ifndef CP_PAXOS_DEBUG_UTIL_H
#define CP_PAXOS_DEBUG_UTIL_H

#include <cp_config.h>
#include <cp_core_generic_util.h>
#include <cp_core_structs.h>
//#include "cp_main.h"
#include <cp_messages.h>
#include "od_debug_util.h"
#include "od_network_context.h"


static inline void update_commit_logs(uint16_t t_id, uint32_t bkt, uint32_t log_no, uint8_t *old_value,
                                      uint8_t *value, const char* message, uint8_t flag)
{
  if (COMMIT_LOGS) { /*
    if (flag == LOG_COMS) {
      struct top *top = (struct top *) old_value;
      struct top *new_top = (struct top *) value;
      bool pushing = new_top->push_counter == top->push_counter + 1;
      bool popping = new_top->pop_counter == top->pop_counter + 1;
      fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: %s: push/pop poitner %u/%u, "
                "key_ptr %u/%u/%u/%u %s - t = %lu\n",
              bkt, log_no, pushing ? "Pushing" : "Pulling",
              new_top->push_counter, new_top->pop_counter, new_top->key_id,
              new_top->sec_key_id, new_top->third_key_id, new_top->fourth_key_id, message,
              time_approx);
    }
    else if (flag == LOG_WS){
      struct node *node = (struct node *) old_value;
      struct node *new_node = (struct node *) value;
      fprintf(rmw_verify_fp[t_id], "Key: %u, %u/%u/%u/%u, "
                "old: %u/%u/%u/%u version %u -- %s - t = %lu\n",
              bkt, new_node->key_id,
              new_node->stack_id, new_node->push_counter, new_node->next_key_id,
              node->key_id,
              node->stack_id, node->push_counter, node->next_key_id, log_no, message,
              time_approx);
    }*/
  }
}



static inline void check_rmw_ids_of_kv_ptr(mica_op_t *kv_ptr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state != INVALID_RMW) {
      if (kv_ptr->last_committed_rmw_id.id == kv_ptr->rmw_id.id) {
        my_printf(red, "Wrkr %u Last committed rmw id is equal to current, kv_ptr state %u, com log/log %u/%u "
                       "rmw id %u/%u,  \n",
                  t_id, kv_ptr->state, kv_ptr->last_committed_log_no, kv_ptr->log_no,
                  kv_ptr->last_committed_rmw_id.id, kv_ptr->rmw_id.id);
        assert(false);
      }
    }
  }
}

static inline void check_log_nos_of_kv_ptr(mica_op_t *kv_ptr, const char *message, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    bool equal_plus_one = kv_ptr->last_committed_log_no + 1 == kv_ptr->log_no;
    bool equal = kv_ptr->last_committed_log_no == kv_ptr->log_no;
    assert((equal_plus_one) ||
           (equal && kv_ptr->state == INVALID_RMW));

    if (kv_ptr->state != INVALID_RMW) {
            if (kv_ptr->last_committed_log_no >= kv_ptr->log_no) {
        my_printf(red, "Wrkr %u t_id, kv_ptr state %u, com log/log %u/%u : %s \n",
                  t_id, kv_ptr->state, kv_ptr->last_committed_log_no, kv_ptr->log_no, message);
        assert(false);
      }
    }
  }
}

static inline void check_kv_ptr_invariants(mica_op_t *kv_ptr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    check_log_nos_of_kv_ptr(kv_ptr, "check_kv_ptr_invariants", t_id);
    check_rmw_ids_of_kv_ptr(kv_ptr, t_id);
  }
}

static inline void checks_before_resetting_accept(loc_entry_t *loc_entry)
{
  check_state_with_allowed_flags(6, (int) loc_entry->state,
                                 INVALID_RMW,          // already committed -- no broadcasts
                                 RETRY_WITH_BIGGER_TS, //log-too-high
                                 NEEDS_KV_PTR,         // log-too-small
                                 MUST_BCAST_COMMITS_FROM_HELP, // ack-quorum for help
                                 MUST_BCAST_COMMITS            // ack-quorum or already committed
  );

  check_state_with_allowed_flags(4, (int) loc_entry->helping_flag,
                                 NOT_HELPING,
                                 HELPING);
}

static inline void checks_before_resetting_prop(loc_entry_t *loc_entry)
{
  check_state_with_allowed_flags(7, (int) loc_entry->state,
                                 INVALID_RMW,          // Already-committed, no need to send commits
                                 RETRY_WITH_BIGGER_TS, // Log-too-high
                                 ACCEPTED,             // Acks or prop_locally_accepted or helping
                                 NEEDS_KV_PTR,         // log-too-small, failed to accept or failed to help because kv_ptr is taken
                                 MUST_BCAST_COMMITS,   //  already-committed, accept attempt found it's already committd
                                 MUST_BCAST_COMMITS_FROM_HELP // log-too-hig timeout
  );
  check_state_with_allowed_flags(6, (int) loc_entry->helping_flag,
                                 NOT_HELPING,
                                 HELPING,
                                 PROPOSE_NOT_LOCALLY_ACKED,
                                 PROPOSE_LOCALLY_ACCEPTED,
                                 HELP_PREV_COMMITTED_LOG_TOO_HIGH);
}

static inline void check_loc_entry_help_flag(loc_entry_t *loc_entry,
                                             uint8_t state,
                                             bool expected_to_be)
{
  if (ENABLE_ASSERTIONS) {
    bool state_is_correct = expected_to_be ?
                 loc_entry->helping_flag == state :
                 loc_entry->helping_flag != state;
    if (!state_is_correct){
      my_printf(red,"Expecting helping flag to %s be %s, flag is %s \n",
                expected_to_be ? "" : "not",
                help_state_to_str(state),
                help_state_to_str(loc_entry->helping_flag));
      assert(false);
    }
  }
}

static inline void check_loc_entry_help_flag_is(loc_entry_t *loc_entry,
                                                uint8_t expected_state)
{
  check_loc_entry_help_flag(loc_entry, expected_state, true);
}

static inline void check_loc_entry_help_flag_is_not(loc_entry_t *loc_entry,
                                                    uint8_t expected_state)
{
  check_loc_entry_help_flag(loc_entry, expected_state, false);
}

static inline void check_loc_entry_is_not_helping(loc_entry_t *loc_entry)
{
  check_loc_entry_help_flag_is(loc_entry, NOT_HELPING);
}

static inline void check_loc_entry_is_helping(loc_entry_t *loc_entry)
{
  check_loc_entry_help_flag_is(loc_entry, HELPING);
}

static inline void check_after_inspecting_accept(loc_entry_t *loc_entry)
{

  check_state_with_allowed_flags(7, (int) loc_entry->state, ACCEPTED, INVALID_RMW, RETRY_WITH_BIGGER_TS,
                                 NEEDS_KV_PTR, MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
  if (ENABLE_ASSERTIONS && loc_entry->rmw_reps.ready_to_inspect)
    assert(loc_entry->state == ACCEPTED && loc_entry->all_aboard);
}


static inline void check_after_inspecting_prop(loc_entry_t *loc_entry)
{
  check_state_with_allowed_flags(7, (int) loc_entry->state, INVALID_RMW, RETRY_WITH_BIGGER_TS,
                                 NEEDS_KV_PTR, ACCEPTED, MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
  if (ENABLE_ASSERTIONS) assert(!loc_entry->rmw_reps.ready_to_inspect);
  if (loc_entry->state != ACCEPTED) assert(loc_entry->rmw_reps.tot_replies == 0);
  else
    assert(loc_entry->rmw_reps.tot_replies == 1);
}

// Check that the counter for propose replies add up (SAME FOR ACCEPTS AND PROPS)
static inline void check_sum_of_reps(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_reps.tot_replies == sum_of_reps(&loc_entry->rmw_reps));
    assert(loc_entry->rmw_reps.tot_replies <= MACHINE_NUM);
  }
}

static inline void check_when_inspecting_rmw(loc_entry_t* loc_entry,
                                             sess_stall_t *stall_info,
                                             uint16_t sess_i)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->sess_id == sess_i);
    if (loc_entry->state != INVALID_RMW)
      assert(stall_info->stalled[sess_i]);
  }
}

static inline void print_commit_latest_committed(loc_entry_t* loc_entry,
                                                 uint16_t t_id)
{
  if (loc_entry->state == MUST_BCAST_COMMITS_FROM_HELP &&
      loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED) {
    my_printf(green, "Wrkr %u sess %u will bcast commits for the latest committed RMW,"
                     " after learning its proposed RMW has already been committed \n",
              t_id, loc_entry->sess_id);
  }
}

static inline void check_global_sess_id(uint8_t machine_id, uint16_t t_id,
                                        uint16_t session_id, uint64_t rmw_id)
{
  uint32_t glob_sess_id = (uint32_t) (rmw_id % GLOBAL_SESSION_NUM);
  assert(glob_ses_id_to_m_id(glob_sess_id) == machine_id);
  assert(glob_ses_id_to_t_id(glob_sess_id) == t_id);
  assert(glob_ses_id_to_sess_id(glob_sess_id) == session_id);
}

static inline void check_version(uint32_t version, const char *message) {
  if (ENABLE_ASSERTIONS) {


    //    if (version == 0 || version % 2 != 0) {
    //      my_printf(red, "Version %u %s\n", version, message);
    //    }
    assert(version >= ALL_ABOARD_TS);
    //    assert(version % 2 == 0);
  }
}

static inline void check_when_filling_loc_entry(loc_entry_t *loc_entry,
                                                uint16_t sess_i,
                                                uint32_t version,
                                                uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    check_global_sess_id((uint8_t) machine_id, t_id,
                         (uint16_t) sess_i, loc_entry->rmw_id.id);
    check_version(version, "fill_loc_rmw_entry_on_grabbing_global");
    assert(!loc_entry->rmw_reps.ready_to_inspect);
    assert(loc_entry->rmw_reps.tot_replies == 0);
  }
}

static inline void check_when_init_loc_entry(loc_entry_t* loc_entry,
                                             trace_op_t *op)
{
  if (ENABLE_ASSERTIONS) {
    assert(op->real_val_len <= RMW_VALUE_SIZE);
    assert(!loc_entry->rmw_reps.ready_to_inspect);
    assert(loc_entry->rmw_reps.tot_replies == 0);
    assert(loc_entry->state == INVALID_RMW);
  }
}

static inline void check_loc_entry_init_rmw_id(loc_entry_t* loc_entry,
                                               uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_id.id % GLOBAL_SESSION_NUM == loc_entry->glob_sess_id);
    assert(glob_ses_id_to_t_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == t_id &&
           glob_ses_id_to_m_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == machine_id);
  }
}

static inline void check_loc_entry_if_already_committed(loc_entry_t* loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry != NULL);
    assert(loc_entry->state != INVALID_RMW);
    assert(loc_entry->helping_flag != HELPING);
  }
}

static inline void check_act_on_quorum_of_commit_acks(loc_entry_t* loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry != NULL);
    assert(loc_entry->state == COMMITTED);
    if (loc_entry->helping_flag == HELPING &&
        rmw_ids_are_equal(&loc_entry->help_loc_entry->rmw_id, &loc_entry->rmw_id)) {
      my_printf(red, "Helping myself, but should not\n");
    }
  }
}


static inline void check_free_session_from_rmw(loc_entry_t* loc_entry,
                                               sess_stall_t *stall_info,
                                               uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->sess_id < SESSIONS_PER_THREAD);
    assert(loc_entry->state == INVALID_RMW);
    if(!stall_info->stalled[loc_entry->sess_id]) {
      my_printf(red, "Wrkr %u sess %u should be stalled \n", t_id, loc_entry->sess_id);
      assert(false);
    }
  }
}

static inline void print_clean_up_after_retrying(mica_op_t *kv_ptr,
                                                 loc_entry_t *loc_entry,
                                                 uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(cyan, "Wrkr %u: session %u gets/regains the kv_ptr log %u to do its propose \n",
              t_id, loc_entry->sess_id, kv_ptr->log_no);
}


static inline void check_clean_up_after_retrying(mica_op_t *kv_ptr,
                                                 loc_entry_t *loc_entry,
                                                 bool help_locally_acced,
                                                 uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->accepted_log_no == 0);
    assert(loc_entry->killable);
    assert(!help_locally_acced);
    assert(ENABLE_CAS_CANCELLING);
    //printf("Cancelling on needing kv_ptr Wrkr%u, sess %u, entry %u rmw_failing \n",
    //     t_id, loc_entry->sess_id, loc_entry->index_to_rmw);
  }
}


static inline void checks_acting_on_quorum_of_prop_ack(loc_entry_t *loc_entry, uint16_t t_id)
{
  check_state_with_allowed_flags(4, loc_entry->state, ACCEPTED, NEEDS_KV_PTR, MUST_BCAST_COMMITS);
  if (ENABLE_ASSERTIONS) {
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED) {
      assert(loc_entry->rmw_reps.tot_replies >= QUORUM_NUM);
      assert(loc_entry->rmw_reps.already_accepted >= 0);
      assert(loc_entry->rmw_reps.seen_higher_prop_acc == 0);
      assert(glob_ses_id_to_t_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == t_id &&
             glob_ses_id_to_m_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == machine_id);

    }
  }
}


static inline void
checks_and_prints_proposed_but_not_locally_acked(sess_stall_t *stall_info,
                                                 mica_op_t *kv_ptr,
                                                 loc_entry_t * loc_entry,
                                                 uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(cyan, "Wrkr %u, session %u helps RMW id %u with version %u, m_id %u,"
                    " kv_ptr log/help log %u/%u kv_ptr committed log %u , "
                    " stashed rmw_id: %u state %u \n",
              t_id, loc_entry->sess_id, loc_entry->rmw_id.id,
              loc_entry->new_ts.version, loc_entry->new_ts.m_id,
              kv_ptr->log_no, loc_entry->log_no, kv_ptr->last_committed_log_no,
              loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->state);

  if (ENABLE_ASSERTIONS) {
    assert(stall_info->stalled);
    assert(loc_entry->rmw_reps.tot_replies == 0);
  }
}

static inline void check_the_rmw_has_committed(uint64_t glob_sess_id)
{
  if (ENABLE_ASSERTIONS) assert(glob_sess_id < GLOBAL_SESSION_NUM);
}

static inline void print_is_log_too_high(uint32_t log_no,
                                         mica_op_t *kv_ptr,
                                         uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wkrk %u Log number is too high %u/%u entry state %u \n",
              t_id, log_no, kv_ptr->last_committed_log_no,
              kv_ptr->state);
}

static inline void check_is_log_too_high(uint32_t log_no, mica_op_t *kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    assert(log_no == kv_ptr->log_no);
    if (log_no != kv_ptr->accepted_log_no)
      printf("log_no %u, kv_ptr accepted_log_no %u, kv_ptr log no %u, kv_ptr->state %u \n",
             log_no, kv_ptr->accepted_log_no, kv_ptr->log_no, kv_ptr->state);
    //assert(log_no == kv_ptr->accepted_log_no);
    //assert(kv_ptr->state == ACCEPTED);
  }
}

static inline void print_log_too_small(uint32_t log_no,
                                       mica_op_t *kv_ptr,
                                       uint64_t rmw_l_id,
                                       uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wkrk %u Log number is too small %u/%u entry state %u, propose/accept with rmw_lid %u,"
                      " \n", t_id, log_no, kv_ptr->last_committed_log_no,
              kv_ptr->state, rmw_l_id);

}

static inline void print_if_log_is_higher_than_local(uint32_t log_no,
                                                     mica_op_t *kv_ptr,
                                                     uint64_t rmw_l_id,
                                                     uint16_t t_id)
{
  if (DEBUG_RMW) { // remote log is higher than the locally stored!
    if (kv_ptr->log_no < log_no && log_no > 1)
      my_printf(yellow, "Wkrk %u Log number is higher than expected %u/%u, entry state %u, "
                        "propose/accept with rmw_lid %u\n",
                t_id, log_no, kv_ptr->log_no,
                kv_ptr->state, rmw_l_id);
  }
}

static inline void check_search_prop_entries_with_l_id(uint16_t entry)
{
  if (ENABLE_ASSERTIONS) assert(entry < LOCAL_PROP_NUM);
}

static inline void check_activate_kv_pair(uint8_t state, mica_op_t *kv_ptr,
                                          uint32_t log_no, const char *message)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->log_no == log_no && kv_ptr->state == ACCEPTED && state != ACCEPTED) {
      printf("%s \n", message);
      assert(false);
    }
    assert(kv_ptr->log_no <= log_no);
  }
}

static inline void check_activate_kv_pair_accepted(mica_op_t *kv_ptr,
                                                   uint32_t new_version,
                                                   uint8_t new_ts_m_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr->prop_ts.version == new_version);
    assert(kv_ptr->prop_ts.m_id == new_ts_m_id);
    kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
  }
}

static inline void check_after_activate_kv_pair(mica_op_t *kv_ptr,
                                                const char *message,
                                                uint8_t state,
                                                uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (committed_glob_sess_rmw_id[kv_ptr->rmw_id.id % GLOBAL_SESSION_NUM] >= kv_ptr->rmw_id.id) {
      //my_printf(red, "Wrkr %u, attempts to activate with already committed RMW id %u/%u glob_sess id %u, state %u: %s \n",
      //           t_id, kv_ptr->rmw_id.id, committed_glob_sess_rmw_id[kv_ptr->rmw_id.id % GLOBAL_SESSION_NUM],
      //           kv_ptr->rmw_id.id % GLOBAL_SESSION_NUM, state, message);
    }
    assert(state == PROPOSED || state == ACCEPTED);
    assert(kv_ptr->last_committed_log_no < kv_ptr->log_no);
  }
}

static inline void checks_after_local_accept(mica_op_t *kv_ptr,
                                             loc_entry_t *loc_entry,
                                             uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->accepted_log_no == loc_entry->log_no);
    assert(loc_entry->log_no == kv_ptr->last_committed_log_no + 1);
    assert(compare_ts(&kv_ptr->prop_ts, &kv_ptr->accepted_ts) != SMALLER);
    kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
  }
  if (ENABLE_DEBUG_RMW_KV_PTR) {
    //kv_ptr->dbg->proposed_ts = loc_entry->new_ts;
    //kv_ptr->dbg->proposed_log_no = loc_entry->log_no;
    //kv_ptr->dbg->proposed_rmw_id = loc_entry->rmw_id;
  }
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept and succeed", t_id);
}


static inline void checks_after_failure_to_locally_accept(mica_op_t *kv_ptr,
                                                          loc_entry_t *loc_entry,
                                                          uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u failed to get rmw id %u, accepted locally "
                     "kv_ptr rmw id %u, state %u \n",
              t_id, loc_entry->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);
  // --CHECKS--
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state == PROPOSED || kv_ptr->state == ACCEPTED) {
      if(!(compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == GREATER ||
           kv_ptr->log_no > loc_entry->log_no)) {
        my_printf(red, "State: %s,  loc-entry-helping %d, Kv prop/base_ts %u/%u -- loc-entry base_ts %u/%u, "
                       "kv-log/loc-log %u/%u kv-rmw_id/loc-rmw-id %u/%u\n",
                  kv_ptr->state == ACCEPTED ? "ACCEPTED" : "PROPOSED",
                  loc_entry->helping_flag,
                  kv_ptr->prop_ts.version, kv_ptr->prop_ts.m_id,
                  loc_entry->new_ts.version, loc_entry->new_ts.m_id,
                  kv_ptr->log_no, loc_entry->log_no,
                  kv_ptr->rmw_id.id, loc_entry->rmw_id.id);
        assert(false);
      }
    }
    else if (kv_ptr->state == INVALID_RMW) // some other rmw committed
      // with cancelling it is possible for some other RMW to stole and then cancelled itself
      if (!ENABLE_CAS_CANCELLING) assert(kv_ptr->last_committed_log_no >= loc_entry->log_no);
  }


  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept and fail", t_id);
}

static inline void checks_after_local_accept_help(mica_op_t *kv_ptr,
                                                  loc_entry_t *loc_entry,
                                                  uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(compare_ts(&kv_ptr->prop_ts, &kv_ptr->accepted_ts) != SMALLER);
    kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
    check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept_to_help and succeed", t_id);
  }
}


static inline void checks_after_failure_to_locally_accept_help(mica_op_t *kv_ptr,
                                                               loc_entry_t *loc_entry,
                                                               uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u sess %u failed to get rmw id %u, accepted locally "
                     "kv_ptr rmw id %u, state %u \n",
              t_id, loc_entry->sess_id, loc_entry->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);


  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept_to_help and fail", t_id);
}


static inline void print_commit_info(commit_info_t * com_info,
                                     color_t color, uint16_t t_id)
{
  my_printf(color, "WORKER %u -------Commit info------------ \n", t_id);
  my_printf(color, "State %s \n", committing_flag_to_str(com_info->flag));
  my_printf(color, "Log no %u\n", com_info->log_no);
  my_printf(color, "Rmw %u\n", com_info->rmw_id.id);
  print_ts(com_info->base_ts, "Base base_ts:", color);
  my_printf(color, "No-value : %u \n", com_info->no_value);
  my_printf(color, "Overwrite-kv %u/%u \n", com_info->overwrite_kv);
}


static inline void check_state_before_commit_algorithm(mica_op_t *kv_ptr,
                                                       commit_info_t *com_info,
                                                       uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (com_info->flag == FROM_LOCAL || com_info->flag == FROM_LOCAL_HELP) {
      // make sure that if we are on the same log
      if (kv_ptr->log_no == com_info->log_no) {
        if (!rmw_ids_are_equal(&com_info->rmw_id, &kv_ptr->rmw_id)) {
          my_printf(red, "kv_ptr is on same log as what is about to be committed but on different rmw-id \n");
          print_commit_info(com_info, yellow, t_id);
          print_kv_ptr(kv_ptr, cyan, t_id);
          // this is a hard error
          assert(false);
        }
        if (kv_ptr->state != INVALID_RMW) {
          if (kv_ptr->state != ACCEPTED) {
            my_printf(red, "Committing: Logs are equal, rmw-ids are equal "
                           "but state is not accepted \n");
            print_commit_info(com_info, yellow, t_id);
            print_kv_ptr(kv_ptr, cyan, t_id);
            assert(false);
          }
        }
      }
      else {
        // if the log has moved on then the RMW has been helped,
        // it has been committed in the other machines so there is no need to change its state
        check_log_nos_of_kv_ptr(kv_ptr, "commit_helped_or_local_from_loc_entry", t_id);
        if (ENABLE_ASSERTIONS) {
          if (kv_ptr->state != INVALID_RMW)
            assert(!rmw_ids_are_equal(&kv_ptr->rmw_id, &com_info->rmw_id));
        }
      }
    }
    else if (com_info->flag == FROM_REMOTE_COMMIT_NO_VAL) {
      if (kv_ptr->last_committed_log_no < com_info->log_no) {
        if (ENABLE_ASSERTIONS) {
          assert(kv_ptr->state == ACCEPTED);
          assert(kv_ptr->log_no == com_info->log_no);
          assert(kv_ptr->accepted_rmw_id.id == com_info->rmw_id.id);
        }
      }
    }
  }
}

// Returns true if the incoming key and the entry key are equal
static inline bool check_entry_validity_with_key(struct key *incoming_key, mica_op_t * kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    struct key *entry_key = &kv_ptr->key;
    return keys_are_equal(incoming_key, entry_key);
  }
  return true;
}


static inline void check_propose_snoops_entry(cp_prop_t *prop,
                                              mica_op_t *kv_ptr)
{
  if (ENABLE_ASSERTIONS)  {
    assert(prop->opcode == PROPOSE_OP);
    assert(prop->log_no > kv_ptr->last_committed_log_no);
    assert(prop->log_no == kv_ptr->log_no);
    assert(check_entry_validity_with_key(&prop->key, kv_ptr));
  }
}



static inline void check_accept_snoops_entry(cp_acc_t *acc,
                                             mica_op_t *kv_ptr)
{
  if (ENABLE_ASSERTIONS)  {
    assert(acc->opcode == ACCEPT_OP);
    assert(acc->log_no > kv_ptr->last_committed_log_no);
    assert(acc->log_no == kv_ptr->log_no);
    assert(check_entry_validity_with_key(&acc->key, kv_ptr));
  }
}

static inline void print_accept_snoops_entry(cp_acc_t *acc,
                                             mica_op_t *kv_ptr,
                                             compare_t ts_comp,
                                             uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (DEBUG_RMW && ts_comp == EQUAL && kv_ptr->state == ACCEPTED)
      my_printf(red, "Wrkr %u Received Accept for the same TS as already accepted, "
                     "version %u/%u m_id %u/%u, rmw_id %u/%u\n",
                t_id, acc->ts.version, kv_ptr->prop_ts.version,
                acc->ts.m_id,
                kv_ptr->prop_ts.m_id, acc->t_rmw_id,
                kv_ptr->rmw_id.id);
  }
}

static inline void print_check_after_accept_snoops_entry(cp_acc_t *acc,
                                                         mica_op_t *kv_ptr,
                                                         cp_rmw_rep_t *rep,
                                                         uint8_t return_flag,
                                                         uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u: %s Accept with rmw_id %u, "
                      "log_no: %u, base_ts.version: %u, ts_m_id %u,"
                      "locally stored state: %u, "
                      "locally stored base_ts: version %u, m_id %u \n",
              t_id, return_flag == RMW_ACK ? "Acks" : "Nacks",
              acc->t_rmw_id, acc->log_no,
              acc->ts.version, acc->ts.m_id, kv_ptr->state,
              kv_ptr->prop_ts.version,
              kv_ptr->prop_ts.m_id);

  if (ENABLE_ASSERTIONS)
    assert(return_flag == RMW_ACK || rep->ts.version > 0);
}


static inline void check_log_no_on_ack_remote_prop_acc(mica_op_t *kv_ptr,
                                                       uint32_t log_no)
{
  if (ENABLE_ASSERTIONS) {
    assert(log_no == kv_ptr->last_committed_log_no + 1);
    assert(kv_ptr->log_no == kv_ptr->last_committed_log_no);
  }
}

// Check the key of the cache_op and  the KV
static inline void check_keys_with_one_trace_op(struct key *com_key, mica_op_t *kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    struct key *kv_key = &kv_ptr->key;
    if (!keys_are_equal(kv_key, com_key)) {
      print_key(kv_key);
      print_key(com_key);
      assert(false);
    }
  }
}

static inline void check_create_prop_rep(cp_prop_t *prop,
                                         mica_op_t *kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr->prop_ts.version >= prop->ts.version);
    check_keys_with_one_trace_op(&prop->key, kv_ptr);
  }
}

static inline uint64_t dbg_kv_ptr_create_acc_prop_rep(mica_op_t *kv_ptr,
                                                      uint64_t *number_of_reqs)
{
  if (ENABLE_DEBUG_RMW_KV_PTR) {
    // kv_ptr->dbg->prop_acc_num++;
    // number_of_reqs = kv_ptr->dbg->prop_acc_num;
  }
}


// After registering, make sure the registered is bigger/equal to what is saved as registered
static inline void check_registered_against_kv_ptr_last_committed(mica_op_t *kv_ptr,
                                                                  uint64_t committed_id,
                                                                  const char *message, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    uint32_t committed_glob_ses_id = (uint32_t)(committed_id % GLOBAL_SESSION_NUM);
    MY_ASSERT(committed_id <= committed_glob_sess_rmw_id[committed_glob_ses_id],
              "After registering: rmw_id/registered %u/%u glob sess_id %u \n",
              committed_id, committed_glob_sess_rmw_id[committed_glob_ses_id], committed_glob_ses_id);

    uint32_t glob_sess_id = (uint32_t)(kv_ptr->last_committed_rmw_id.id % GLOBAL_SESSION_NUM);
    uint64_t id = kv_ptr->last_committed_rmw_id.id;
    assert(glob_sess_id < GLOBAL_SESSION_NUM);
    if (committed_glob_sess_rmw_id[glob_sess_id] < id) {
      my_printf(yellow, "Committing %s rmw_id: %u glob_sess_id: %u \n", message, committed_id, committed_glob_ses_id);
      my_printf(red, "Wrkr %u: %s rmw_id: kv_ptr last committed %lu, "
                     "glob_sess_id :kv_ptr last committed %u,"
                     "committed_glob_sess_rmw_id %lu,   \n", t_id, message,
                kv_ptr->last_committed_rmw_id.id,
                glob_sess_id,
                committed_glob_sess_rmw_id[glob_sess_id]);
      //assert(false);
    }
  }
}

static inline void check_fill_com_info(uint32_t log_no)
{
  if (ENABLE_ASSERTIONS) assert(log_no > 0);
}



static inline void comment_on_why_we_dont_check_if_rmw_committed()
{
  // We don't need to check if the RMW is already registered (committed) in
  // (attempt_local_accept_to_help)-- it's not wrong to do so--
  // but if the RMW has been committed, it will be in the present log_no
  // and we will not be able to accept locally anyway.
}

static inline void check_store_rmw_rep_to_help_loc_entry(loc_entry_t* loc_entry,
                                                         cp_rmw_rep_t* prop_rep,
                                                         compare_t ts_comp)
{
  if (ENABLE_ASSERTIONS) {
    loc_entry_t *help_loc_entry = loc_entry->help_loc_entry;
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED) {
      assert(help_loc_entry->new_ts.version > 0);
      assert(help_loc_entry->state == ACCEPTED);
      assert(ts_comp != EQUAL); // It would have been an SAME_ACC_ACK
    }
    assert(help_loc_entry->state == INVALID_RMW || help_loc_entry->state == ACCEPTED);
  }
}

static inline void check_handle_prop_or_acc_rep_ack(cp_rmw_rep_mes_t *rep_mes,
                                                    rmw_rep_info_t *rep_info,
                                                    bool is_accept,
                                                    uint16_t t_id)
{
  if (ENABLE_ASSERTIONS)
    assert(rep_mes->m_id < MACHINE_NUM && rep_mes->m_id != machine_id);
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u, the received rep is an %s ack, "
                     "total acks %u \n", t_id, is_accept ? "acc" : "prop",
              rep_info->acks);
}

static inline void check_handle_rmw_rep_seen_lower_acc(loc_entry_t* loc_entry,
                                                       cp_rmw_rep_t *rep,
                                                       bool is_accept)
{
  if (ENABLE_ASSERTIONS) {
    assert(compare_netw_ts_with_ts(&rep->ts, &loc_entry->new_ts) == SMALLER);
    assert(!is_accept);
  }
}


static inline void print_handle_rmw_rep_seen_higher(cp_rmw_rep_t *rep,
                                                    rmw_rep_info_t *rep_info,
                                                    bool is_accept,
                                                    uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u: the %s rep is %u, %u sum of all other reps %u \n", t_id,
              is_accept ? "acc" : "prop",rep->opcode,
              rep_info->seen_higher_prop_acc,
              rep_info->rmw_id_commited + rep_info->log_too_small +
              rep_info->already_accepted);

}


static inline void print_handle_rmw_rep_higher_ts(rmw_rep_info_t *rep_info,
                                                  uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u: overwriting the TS version %u \n",
              t_id, rep_info->seen_higher_prop_version);

}


static inline void check_handle_rmw_rep_end(loc_entry_t* loc_entry,
                                            bool is_accept)
{
  if (ENABLE_ASSERTIONS) {
    if (is_accept) assert(loc_entry->state == ACCEPTED);
    if (!is_accept) assert(loc_entry->state == PROPOSED);
    check_sum_of_reps(loc_entry);
  }

}

static inline void check_find_local_and_handle_rmw_rep(loc_entry_t *loc_entry_array,
                                                       cp_rmw_rep_t *rep,
                                                       cp_rmw_rep_mes_t *rep_mes,
                                                       uint16_t byte_ptr,
                                                       bool is_accept,
                                                       uint16_t r_rep_i,
                                                       uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry_array != NULL);
    if (!opcode_is_rmw_rep(rep->opcode)) {
      printf("Rep_i %u, current opcode %u first opcode: %u, byte_ptr %u \n",
             r_rep_i, rep->opcode, rep_mes->rmw_rep[0].opcode, byte_ptr);
    }
    assert(opcode_is_rmw_rep(rep->opcode));
  }
  //my_printf(cyan, "RMW rep opcode %u, l_id %u \n", rep->opcode, rep->l_id);

}


static inline void check_zero_out_the_rmw_reply(loc_entry_t* loc_entry)
{
  if (ENABLE_ASSERTIONS) { // make sure the loc_entry is correctly set-up
    if (loc_entry->help_loc_entry == NULL) {
      my_printf(red, "When Zeroing: The help_loc_ptr is NULL. The reason is typically that "
                     "help_loc_entry was passed to the function "
                     "instead of loc entry to check \n");
      assert(false);
    }
    assert(loc_entry->rmw_reps.ready_to_inspect);
    assert(loc_entry->rmw_reps.inspected);
  }
}

static inline void check_after_zeroing_out_rmw_reply(loc_entry_t* loc_entry)
{
  if (ENABLE_ASSERTIONS) assert(!loc_entry->rmw_reps.ready_to_inspect);
}

static inline void check_reinstate_loc_entry_after_helping(loc_entry_t* loc_entry)
{
  check_loc_entry_is_helping(loc_entry);
}

static inline void check_after_reinstate_loc_entry_after_helping(loc_entry_t* loc_entry,
                                                                 uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, sess %u reinstates its RMW id %u after helping \n",
              t_id, loc_entry->sess_id, loc_entry->rmw_id.id);
  if (ENABLE_ASSERTIONS)
    assert(glob_ses_id_to_m_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == (uint8_t) machine_id);

}



static inline void check_after_gathering_acc_acks(loc_entry_t* loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->state != COMMITTED);
    if (loc_entry->helping_flag == HELPING) assert(!loc_entry->all_aboard);
    assert(!loc_entry->avoid_val_in_com);
    assert(!loc_entry->avoid_val_in_com);
    assert(!loc_entry->help_loc_entry->avoid_val_in_com);
  }
}

static inline void check_that_a_nack_is_received(bool received_nack,
                                                 rmw_rep_info_t * rep_info)
{
  if (ENABLE_ASSERTIONS) {
    if (received_nack)
      assert(rep_info->rmw_id_commited > 0 || rep_info->log_too_small > 0 ||
             rep_info->already_accepted > 0 || rep_info->seen_higher_prop_acc > 0 ||
             rep_info->log_too_high > 0);
    else assert(rep_info->rmw_id_commited == 0 && rep_info->log_too_small == 0 &&
                rep_info->already_accepted == 0 && rep_info->seen_higher_prop_acc == 0 &&
                rep_info->log_too_high == 0);
  }
}


static inline void check_that_if_nack_and_helping_flag_is_helping(bool is_helping,
                                                                  bool received_a_nack,
                                                                  loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    if (is_helping && received_a_nack)
      check_loc_entry_help_flag_is(loc_entry, HELPING);
  }
}
//
static inline void check_handle_all_aboard(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(ENABLE_ALL_ABOARD);
    assert(loc_entry->all_aboard);
    assert(loc_entry->new_ts.version == ALL_ABOARD_TS);
  }
}

static inline void print_all_aboard_time_out(loc_entry_t *loc_entry,
                                             uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
     //my_printf(green, "Wrkr %u, Timing out on key %u \n",
     // t_id, loc_entry->key.bkt);
  }
}


static inline void check_inspect_accepts(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    check_sum_of_reps(loc_entry);
    check_loc_entry_help_flag_is_not(loc_entry, PROPOSE_LOCALLY_ACCEPTED);
    check_loc_entry_help_flag_is_not(loc_entry, PROPOSE_NOT_LOCALLY_ACKED);
    check_state_with_allowed_flags(3, loc_entry->helping_flag, NOT_HELPING, HELPING);
  }
}


static inline void check_if_accepted_cannot_be_helping(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    if (loc_entry->state == ACCEPTED)
      check_loc_entry_is_not_helping(loc_entry);
  }
}


static inline void check_bcasting_after_rmw_already_committed()
{
  if (ENABLE_ASSERTIONS) {
    assert(MACHINE_NUM > 3);
  }
}

static inline void check_when_reps_have_been_zeroes_on_prop(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_reps.tot_replies == 1);
    assert(loc_entry->state == ACCEPTED);
  }
}

static inline void print_log_too_high_timeout(loc_entry_t *loc_entry,
                                              uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    my_printf(red, "Timed out on log_too-high\n",
              t_id, loc_entry->sess_id);
    print_loc_entry(loc_entry, yellow, t_id);
  }
}

static inline void print_needs_kv_ptr_timeout_expires(loc_entry_t *loc_entry,
                                                      uint16_t sess_i,
                                                      uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (DEBUG_RMW) {
      my_printf(yellow, "Wrkr %u  sess %u waiting for an "
                        "rmw on key %u "
                        "on log %u, back_of cntr %u waiting on "
                        "rmw_id %u state %u \n",
                t_id, sess_i, loc_entry->key.bkt,
                loc_entry->help_rmw->log_no,
                loc_entry->back_off_cntr,
                loc_entry->help_rmw->rmw_id.id,
                loc_entry->help_rmw->state);
    }
  }
}

static inline void check_handle_needs_kv_ptr_state(cp_core_ctx_t *cp_core_ctx,
                                                   uint16_t sess_i)
{
  if (ENABLE_ASSERTIONS) {
    assert(cp_core_ctx->stall_info->stalled[sess_i]);
  }
}


static inline void check_end_handle_needs_kv_ptr_state(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    check_state_with_allowed_flags(6, (int) loc_entry->state,
                                   INVALID_RMW,
                                   PROPOSED,
                                   NEEDS_KV_PTR,
                                   ACCEPTED,
                                   MUST_BCAST_COMMITS);
  }
}

static inline void check_when_retrying_with_higher_TS(mica_op_t *kv_ptr,
                                                      loc_entry_t *loc_entry,
                                                      bool from_propose)
{

  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state != INVALID_RMW) {
      assert(kv_ptr->log_no == kv_ptr->last_committed_log_no + 1);
      assert(loc_entry->log_no == kv_ptr->last_committed_log_no + 1);
    }
    if (kv_ptr->state == ACCEPTED) {
      assert(!from_propose);
      assert(compare_ts(&kv_ptr->accepted_ts, &loc_entry->new_ts) == EQUAL);
    }
  }
}

static inline void print_when_retrying_fails(mica_op_t *kv_ptr,
                                             loc_entry_t *loc_entry,
                                             uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (DEBUG_RMW)
      my_printf(yellow, "Wrkr %u, session %u  failed when attempting to get/regain the kv_ptr, "
                        "waiting: waited for %u cycles for "
                        "now waiting on rmw_id %, state %u\n",
                t_id, loc_entry->sess_id,
                kv_ptr->rmw_id.id, kv_ptr->state);
  }
}


static inline void check_kv_ptr_state_is_not_acced(mica_op_t *kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr->state != ACCEPTED);
  }
}

static inline void check_op_version(trace_op_t *op,
                                    bool doing_all_aboard)
{
  if (ENABLE_ASSERTIONS) {
    if (doing_all_aboard)
      assert(op->ts.version == ALL_ABOARD_TS);
    else assert(op->ts.version == PAXOS_TS);
  }
}

static inline void check_session_id(uint16_t session_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(session_id < SESSIONS_PER_THREAD);
  }
}

static inline void debug_assign_help_loc_entry_kv_ptr(mica_op_t *kv_ptr,
                                                                  loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    loc_entry->help_loc_entry->kv_ptr = kv_ptr;
  }
}

static inline void print_rmw_tries_first_time(uint16_t op_i,
                                              uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (DEBUG_RMW)
      my_printf(green, "Worker %u trying a local RMW on op %u\n", t_id, op_i);
  }
}

static inline void print_log_on_rmw_recv(uint64_t rmw_l_id,
                                         uint8_t acc_m_id,
                                         uint32_t log_no,
                                         cp_rmw_rep_t *acc_rep,
                                         netw_ts_tuple_t ts,
                                         mica_op_t *kv_ptr,
                                         uint64_t number_of_reqs,
                                         bool is_accept,
                                         uint16_t t_id)
{
  if (PRINT_LOGS)
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Req %lu, %s: m_id:%u, "
                                 "rmw_id %lu, glob_sess id: %u, "
                                 "version %u, m_id: %u, resp: %u \n",
            kv_ptr->key.bkt, log_no, number_of_reqs,
            is_accept ? "Acc" : "Prop",
            acc_m_id, rmw_l_id,
            (uint32_t) rmw_l_id % GLOBAL_SESSION_NUM,
            ts.version, ts.m_id, acc_rep->opcode);
}


// Check the key of the trace_op and the KVS
static inline void check_trace_op_key_vs_kv_ptr(trace_op_t* op, mica_op_t* kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    struct key *op_key = &op->key;
    struct key *kv_key = &kv_ptr->key;
    if (!keys_are_equal(kv_key, op_key)) {
      print_key(kv_key);
      print_key(op_key);
      assert(false);
    }
  }
}

static inline void verify_paxos(loc_entry_t *loc_entry, uint16_t t_id)
{
  if (VERIFY_PAXOS && is_global_ses_id_local((uint32_t)loc_entry->rmw_id.id % GLOBAL_SESSION_NUM, t_id)) {
    //if (committed_log_no != *(uint32_t *)loc_entry->value_to_write)
    //  red_printf ("vale_to write/log no %u/%u",
    //             *(uint32_t *)loc_entry->value_to_write, committed_log_no );
    uint64_t val = *(uint64_t *)loc_entry->value_to_read;
    //assert(val == loc_entry->accepted_log_no - 1);
    fprintf(rmw_verify_fp[t_id], "%u %lu %u \n", loc_entry->key.bkt, val, loc_entry->accepted_log_no);
  }
}


static inline void check_that_the_rmw_ids_match(mica_op_t *kv_ptr, uint64_t rmw_id,
                                                uint32_t log_no, uint32_t version,
                                                uint8_t m_id, const char *message, uint16_t t_id)
{
  uint64_t glob_sess_id = rmw_id % GLOBAL_SESSION_NUM;
  if (kv_ptr->last_committed_rmw_id.id != rmw_id) {
    my_printf(red, "~~~~~~~~COMMIT MISSMATCH Worker %u key: %u, Log %u %s ~~~~~~~~ \n", t_id, kv_ptr->key.bkt, log_no, message);
    my_printf(green, "GLOBAL ENTRY COMMITTED log %u: rmw_id %lu glob_sess-id- %u\n",
              kv_ptr->last_committed_log_no, kv_ptr->last_committed_rmw_id.id,
              kv_ptr->last_committed_rmw_id.id % GLOBAL_SESSION_NUM);
    my_printf(yellow, "COMMIT log %u: rmw_id %lu glob_sess-id-%u version %u m_id %u \n",
              log_no, rmw_id, glob_sess_id, version, m_id);
    /*if (ENABLE_DEBUG_RMW_KV_PTR) {
      my_printf(green, "GLOBAL ENTRY COMMITTED log %u: rmw_id %lu glob_sess-id- %u, FLAG %u\n",
                   kv_ptr->last_committed_log_no, kv_ptr->last_committed_rmw_id.id,
                   kv_ptr->last_committed_rmw_id.glob_sess_id, kv_ptr->dbg->last_committed_flag);
      my_printf(yellow, "COMMIT log %u: rmw_id %lu glob_sess-id-%u version %u m_id %u \n",
                    log_no, rmw_id, glob_sess_id, version, m_id);
      if (kv_ptr->dbg->last_committed_flag <= 1) {
        my_printf(cyan, "PROPOSED log %u: rmw_id %lu glob_sess-id-%u version %u m_id %u \n",
                    kv_ptr->dbg->proposed_log_no, kv_ptr->dbg->proposed_rmw_id.id,
                    kv_ptr->dbg->proposed_rmw_id.glob_sess_id,
                    kv_ptr->dbg->proposed_ts.version, kv_ptr->dbg->proposed_ts.m_id);


        my_printf(cyan, "LAST COMMIT log %u: rmw_id %lu glob_sess-id-%u version %u m_id %u \n",
                    kv_ptr->dbg->last_committed_log_no, kv_ptr->dbg->last_committed_rmw_id.id,
                    kv_ptr->dbg->last_committed_rmw_id.glob_sess_id,
                    kv_ptr->dbg->last_committed_ts.version, kv_ptr->dbg->last_committed_ts.m_id);

      }
    }*/
    assert(false);
  }
}


static inline void check_on_updating_rmw_meta_commit_algorithm(mica_op_t *kv_ptr,
                                                               commit_info_t *com_info,
                                                               uint16_t t_id)
{
  if (kv_ptr->last_committed_log_no < com_info->log_no) {
    if (DEBUG_RMW)
      my_printf(green, "Wrkr %u commits locally rmw id %u: %s \n",
                t_id, com_info->rmw_id, com_info->message);
    update_commit_logs(t_id, kv_ptr->key.bkt, com_info->log_no, kv_ptr->value,
                       com_info->value, com_info->message, LOG_COMS);
  }
  else if (kv_ptr->last_committed_log_no == com_info->log_no) {
    check_that_the_rmw_ids_match(kv_ptr,  com_info->rmw_id.id, com_info->log_no,
                                 com_info->base_ts.version, com_info->base_ts.m_id,
                                 com_info->message, t_id);
  }
}

static inline void print_on_remote_com(cp_com_t *com,
                                       uint16_t op_i,
                                       uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(green, "Worker %u is handling a remote "
                     "RMW commit on com %u, "
                     "rmw_l_id %u, glob_ses_id %u, "
                     "log_no %u, version %u  \n",
              t_id, op_i, com->t_rmw_id,
              com->t_rmw_id % GLOBAL_SESSION_NUM,
              com->log_no, com->base_ts.version);
}


static inline void print_log_remote_com(cp_com_t *com,
                                        cp_com_mes_t *com_mes,
                                        mica_op_t *kv_ptr,
                                        uint16_t t_id)
{
  if (PRINT_LOGS) {
    uint8_t acc_m_id = com_mes->m_id;
    uint64_t number_of_rquests = 0;
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Req %lu, Com: m_id:%u, rmw_id %lu, glob_sess id: %u, "
                                 "version %u, m_id: %u \n",
            kv_ptr->key.bkt, com->log_no, number_of_rquests, acc_m_id, com->t_rmw_id,
            (uint32_t) (com->t_rmw_id % GLOBAL_SESSION_NUM), com->base_ts.version, com->base_ts.m_id);
  }
}


// When stealing kv_ptr from a stuck proposal,
// check that the proposal was referring to a valid log no
static inline void check_the_proposed_log_no(mica_op_t *kv_ptr, loc_entry_t *loc_entry,
                                             uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->log_no > kv_ptr->last_committed_log_no + 1) {
      my_printf(red, "Key %u Last committed//accepted/active %u/%u/%u \n", loc_entry->key.bkt,
                kv_ptr->last_committed_log_no,
                kv_ptr->accepted_log_no,
                kv_ptr->log_no);
      assert(false);
    }
  }
}


static inline void check_when_rmw_has_committed(mica_op_t *kv_ptr,
                                                struct rmw_rep_last_committed *rep,
                                                uint64_t glob_sess_id,
                                                uint32_t log_no,
                                                uint64_t rmw_id,
                                                uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (DEBUG_RMW)
      my_printf(green, "Key %lu: Worker %u: Remote machine  global sess_id %u is trying a propose/accept, \n"
                       "kv_ptr/ prop \n"
                       "log_no: %u/%u \n"
                       "rmw_id %lu/%lu \n It has been already committed, "
                       "because for this global_sess, we have seen rmw_id %lu\n",
                kv_ptr->key.bkt,
                t_id, glob_sess_id,
                kv_ptr->last_committed_log_no, log_no,
                kv_ptr->last_committed_rmw_id.id, rmw_id,
                committed_glob_sess_rmw_id[glob_sess_id]);

    //for (uint64_t i = 0; i < GLOBAL_SESSION_NUM; i++)
    //  printf("Glob sess num %lu: %lu \n", i, committed_glob_sess_rmw_id[i]);
    assert(rep->opcode == RMW_ID_COMMITTED_SAME_LOG || RMW_ID_COMMITTED);
    assert(kv_ptr->last_committed_log_no > 0);
    if (rep->opcode == RMW_ID_COMMITTED_SAME_LOG) {
      assert(kv_ptr->last_committed_log_no == log_no);
      assert(kv_ptr->last_committed_rmw_id.id == rmw_id);
    }
  }

}

static inline void check_loc_entry_when_filling_com(loc_entry_t *loc_entry,
                                                    uint8_t broadcast_state,
                                                    uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (broadcast_state == MUST_BCAST_COMMITS_FROM_HELP) {
      //my_printf(green, "Wrkr %u helping rmw %lu glob_sess_id %u for key %lu \n",
      //                 t_id, loc_entry->rmw_id.id, loc_entry->key.bkt);
      assert(loc_entry->help_loc_entry == NULL);
    }
  }
}

static inline void checks_when_handling_prop_acc_rep(loc_entry_t *loc_entry,
                                                     struct rmw_rep_last_committed *rep,
                                                     bool is_accept, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;
    assert(rep_info->tot_replies > 0);
    if (is_accept) assert(loc_entry->state == ACCEPTED);
    else {
      assert(loc_entry->state == PROPOSED);
      // this checks that the performance optimization of NO-op reps is valid
      assert(rep->opcode != NO_OP_PROP_REP);
      check_state_with_allowed_flags(4, loc_entry->helping_flag, NOT_HELPING,
                                     PROPOSE_NOT_LOCALLY_ACKED, PROPOSE_LOCALLY_ACCEPTED);
      if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED ||
          loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED)
        assert(rep_info->already_accepted > 0);
    }
  }
}
/*--------------------------------------------------------------------------
 * --------------------ACCEPTING-------------------------------------
 * --------------------------------------------------------------------------*/

static inline void checks_preliminary_local_accept(mica_op_t *kv_ptr,
                                                   loc_entry_t *loc_entry,
                                                   uint16_t t_id)
{
  my_assert(keys_are_equal(&loc_entry->key, &kv_ptr->key),
            "Attempt local accept: Local entry does not contain the same key as kv_ptr");

  if (ENABLE_ASSERTIONS) assert(loc_entry->glob_sess_id < GLOBAL_SESSION_NUM);
}

static inline void checks_before_local_accept(mica_op_t *kv_ptr,
                                              loc_entry_t *loc_entry,
                                              uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    //assert(compare_ts(&loc_entry->new_ts, &kv_ptr->prop_ts) == EQUAL);
    assert(kv_ptr->log_no == loc_entry->log_no);
    assert(kv_ptr->last_committed_log_no == loc_entry->log_no - 1);
  }

  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u got rmw id %u, accepted locally \n",
              t_id, loc_entry->rmw_id.id);
}

static inline void check_after_filling_com_with_val(cp_com_t *com)
{
  if (ENABLE_ASSERTIONS) {
    assert(com->log_no > 0);
    assert(com->t_rmw_id > 0);
  }
}


//------------------------------HELP STUCK RMW------------------------------------------
static inline void logging_proposed_but_not_locally_acked(mica_op_t *kv_ptr,
                                                          loc_entry_t * loc_entry,
                                                          loc_entry_t *help_loc_entry,
                                                          uint16_t t_id)
{
  if (PRINT_LOGS && ENABLE_DEBUG_RMW_KV_PTR)
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Prop-not-locally accepted: helping rmw_id %lu, "
                                 "version %u, m_id: %u, From: rmw_id %lu, with version %u, m_id: %u \n",
            loc_entry->key.bkt, loc_entry->log_no, help_loc_entry->rmw_id.id,
            help_loc_entry->new_ts.version, help_loc_entry->new_ts.m_id, loc_entry->rmw_id.id,
            loc_entry->new_ts.version, loc_entry->new_ts.m_id);
}


static inline void checks_init_attempt_to_grab_kv_ptr(loc_entry_t * loc_entry,
                                                      uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_id.id % GLOBAL_SESSION_NUM == loc_entry->glob_sess_id);
    assert(!loc_entry->rmw_reps.ready_to_inspect);
    assert(loc_entry->rmw_reps.tot_replies == 0);
  }
}



static inline void print_when_grabbing_kv_ptr(loc_entry_t * loc_entry,
                                              uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, after waiting for %u cycles, session %u  \n",
              t_id, loc_entry->back_off_cntr, loc_entry->sess_id);
}


static inline void print_when_state_changed_not_grabbing_kv_ptr(mica_op_t *kv_ptr,
                                                                loc_entry_t * loc_entry,
                                                                uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, session %u changed who is waiting: waited for %u cycles on "
                      "state %u rmw_id %u , now waiting on rmw_id %u , state %u\n",
              t_id, loc_entry->sess_id, loc_entry->back_off_cntr,
              loc_entry->help_rmw->state, loc_entry->help_rmw->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);
}


static inline void check_and_print_when_rmw_fails(mica_op_t *kv_ptr,
                                                  loc_entry_t * loc_entry,
                                                  uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->accepted_log_no == 0);
    assert(loc_entry->killable);
    assert(ENABLE_CAS_CANCELLING);
  }
  if (DEBUG_RMW)
    printf("Cancelling on needing kv_ptr Wrkr%u, sess %u, rmw_failing \n",
           t_id, loc_entry->sess_id);

}


static inline void checks_attempt_to_help_locally_accepted(mica_op_t *kv_ptr,
                                                           loc_entry_t * loc_entry,
                                                           uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr->accepted_log_no == kv_ptr->log_no);
    assert(kv_ptr->prop_ts.version > kv_ptr->accepted_ts.version);
    assert(rmw_ids_are_equal(&kv_ptr->rmw_id, &kv_ptr->accepted_rmw_id));
    assert(loc_entry->key.bkt == kv_ptr->key.bkt);
    assert(kv_ptr->state == ACCEPTED);
  }
}

static inline void print_when_state_changed_steal_proposed(mica_op_t *kv_ptr,
                                                           loc_entry_t *loc_entry,
                                                           uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, session %u on attempting to steal the propose, changed who is "
                      "waiting: waited for %u cycles for state %u "
                      "rmw_id %u  state %u,  now waiting on rmw_id % , state %u\n",
              t_id, loc_entry->sess_id, loc_entry->back_off_cntr,
              loc_entry->help_rmw->state, loc_entry->help_rmw->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);
}


static inline void print_after_stealing_proposed(mica_op_t *kv_ptr,
                                                 loc_entry_t * loc_entry,
                                                 uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(cyan, "Wrkr %u: session %u steals kv_ptr to do its propose \n",
              t_id, loc_entry->sess_id);
}
/*--------------------------------------------------------------------------
 * --------------------COMMITS-------------------------------------
 * --------------------------------------------------------------------------*/
static inline void error_mesage_on_commit_check(mica_op_t *kv_ptr,
                                                commit_info_t *com_info,
                                                const char* message,
                                                uint16_t t_id)
{
  my_printf(red, "----Key: %lu ---------Worker %u----- \n"
                 "%s \n"
                 "Flag: %s \n"
                 "kv_ptr / com_info \n"
                 "rmw_id  %lu/%lu\n, "
                 "log_no %u/%u \n"
                 "base ts %u-%u/%u-%u \n",
            kv_ptr->key.bkt, t_id, message,
            com_info->message,
            kv_ptr->last_committed_rmw_id.id, com_info->rmw_id.id,
            kv_ptr->last_committed_log_no, com_info->log_no,
            kv_ptr->ts.version, kv_ptr->ts.m_id,
            com_info->base_ts.version, com_info->base_ts.m_id);
  exit(0);
}

static inline void check_inputs_commit_algorithm(mica_op_t *kv_ptr,
                                                 commit_info_t *com_info,
                                                 uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr != NULL);
    if (com_info->value == NULL) assert(com_info->no_value);
    if (com_info->log_no == 0) {
      if (com_info->rmw_id.id != 0)
        error_mesage_on_commit_check(kv_ptr, com_info, "Rmw-id is zero but not log-no", t_id);
      assert(com_info->rmw_id.id == 0);
    }
    if (com_info->rmw_id.id == 0) assert(com_info->log_no == 0);
    if (!com_info->overwrite_kv)
      assert(com_info->flag == FROM_LOCAL ||
             com_info->flag == FROM_ALREADY_COMM_REP);
  }
}

static inline void check_on_overwriting_commit_algorithm(mica_op_t *kv_ptr,
                                                         commit_info_t *com_info,
                                                         compare_t cart_comp,
                                                         uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (cart_comp == EQUAL) {
      assert(kv_ptr->last_committed_log_no == com_info->log_no);
      if (kv_ptr->last_committed_rmw_id.id != com_info->rmw_id.id)
        error_mesage_on_commit_check(kv_ptr, com_info, "Carts equal, but not rmw-ids", t_id);
      assert(kv_ptr->last_committed_rmw_id.id == com_info->rmw_id.id);
      //assert(memcmp(kv_ptr->value, com_info->value, (size_t) 8) == 0);
    }
    else if (cart_comp == SMALLER) {
      assert(compare_ts(&com_info->base_ts, &kv_ptr->ts) == SMALLER ||
             (compare_ts(&com_info->base_ts, &kv_ptr->ts) == EQUAL &&
              com_info->log_no < kv_ptr->last_committed_log_no));
    }
  }
}

static inline void checks_preliminary_local_accept_help(mica_op_t *kv_ptr,
                                                        loc_entry_t *loc_entry,
                                                        loc_entry_t *help_loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    my_assert(keys_are_equal(&help_loc_entry->key, &kv_ptr->key),
              "Attempt local accpet to help: Local entry does not contain the same key as kv_ptr");
    my_assert(loc_entry->help_loc_entry->log_no == loc_entry->log_no,
              " the help entry and the regular have not the same log nos");
    assert(help_loc_entry->glob_sess_id < GLOBAL_SESSION_NUM);
    assert(loc_entry->log_no == help_loc_entry->log_no);
  }
}

static inline void checks_and_prints_local_accept_help(loc_entry_t *loc_entry,
                                                       loc_entry_t* help_loc_entry,
                                                       mica_op_t *kv_ptr, bool kv_ptr_is_the_same,
                                                       bool kv_ptr_is_invalid_but_not_committed,
                                                       bool helping_stuck_accept,
                                                       bool propose_locally_accepted,
                                                       uint16_t t_id)
{
  if (kv_ptr_is_the_same   || kv_ptr_is_invalid_but_not_committed ||
      helping_stuck_accept || propose_locally_accepted) {
    if (ENABLE_ASSERTIONS) {
      assert(compare_ts(&kv_ptr->prop_ts, &help_loc_entry->new_ts) != SMALLER);
      assert(kv_ptr->last_committed_log_no == help_loc_entry->log_no - 1);
      if (kv_ptr_is_invalid_but_not_committed) {
        printf("last com/log/help-log/loc-log %u/%u/%u/%u \n",
               kv_ptr->last_committed_log_no, kv_ptr->log_no,
               help_loc_entry->log_no, loc_entry->log_no);
        assert(false);
      }
      // if the TS are equal it better be that it is because it remembers the proposed request
      if (kv_ptr->state != INVALID_RMW &&
          compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == EQUAL &&
          !helping_stuck_accept &&
          !propose_locally_accepted) {
        assert(kv_ptr->rmw_id.id == loc_entry->rmw_id.id);
        if (kv_ptr->state != PROPOSED) {
          my_printf(red, "Wrkr: %u, state %u \n", t_id, kv_ptr->state);
          assert(false);
        }
      }
      if (propose_locally_accepted)
        assert(compare_ts(&help_loc_entry->new_ts, &kv_ptr->accepted_ts) == GREATER);
    }
    if (DEBUG_RMW)
      my_printf(green, "Wrkr %u on attempting to locally accept to help "
                       "got rmw id %u, accepted locally \n",
                t_id, help_loc_entry->rmw_id.id);
  }
}

static inline void checks_acting_on_already_accepted_rep(loc_entry_t *loc_entry, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    loc_entry_t* help_loc_entry = loc_entry->help_loc_entry;
    assert(loc_entry->log_no == help_loc_entry->log_no);
    assert(loc_entry->help_loc_entry->state == ACCEPTED);
    assert(compare_ts(&help_loc_entry->new_ts, &loc_entry->new_ts) == SMALLER);
  }
}

// Print the rep info received for a propose or an accept
static inline void print_rmw_rep_info(loc_entry_t *loc_entry, uint16_t t_id) {
  struct rmw_rep_info *rmw_rep = &loc_entry->rmw_reps;
  my_printf(yellow, "Wrkr %u Printing rmw_rep for sess %u state %u helping flag %u \n"
                    "Tot_replies %u \n acks: %u \n rmw_id_committed: %u \n log_too_small %u\n"
                    "already_accepted : %u\n seen_higher_prop : %u\n "
                    "log_too_high: %u \n",
            t_id, loc_entry->sess_id, loc_entry->state, loc_entry->helping_flag,
            rmw_rep->tot_replies,
            rmw_rep->acks, rmw_rep->rmw_id_commited, rmw_rep->log_too_small,
            rmw_rep->already_accepted,
            rmw_rep->seen_higher_prop_acc, rmw_rep->log_too_high);
}


static inline void check_loc_entry_metadata_is_reset(loc_entry_t* loc_entry,
                                                     const char *message,
                                                     uint16_t t_id)
{
  if (loc_entry->helping_flag != PROPOSE_NOT_LOCALLY_ACKED &&
      loc_entry->helping_flag != PROPOSE_LOCALLY_ACCEPTED) {
    if (ENABLE_ASSERTIONS) { // make sure the loc_entry is correctly set-up
      if (loc_entry->help_loc_entry == NULL) {
        //my_printf(red, "The help_loc_ptr is NULL. The reason is typically that help_loc_entry was passed to the function "
        //           "instead of loc entry to check \n");
        assert(loc_entry->state == INVALID_RMW);
      } else {
        if (loc_entry->help_loc_entry->state != INVALID_RMW) {
          my_printf(red, "Wrkr %u: %s \n", t_id, message);
          assert(false);
        }
        assert(loc_entry->rmw_reps.tot_replies == 1);
        assert(loc_entry->back_off_cntr == 0);
      }
    }
  }
}


#endif