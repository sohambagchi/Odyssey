//
// Created by vasilis on 08/09/20.
//

#ifndef ODYSSEY_HR_KVS_UTIL_H
#define ODYSSEY_HR_KVS_UTIL_H

#include <od_network_context.h>
#include <od_netw_func.h>
#include "od_kvs.h"
#include "hr_config.h"
#include "splinterdb.h"
#include "bplus.h"


void hr_KVS_batch_op_trace(context_t *ctx, uint16_t op_num, kvs_t* kvs);

void hr_KVS_batch_op_invs(context_t *ctx, kvs_t* kvs);

void hr_sdb_batch_op_invs(context_t *ctx, splinterdb *spl_handle);
// TODO: fix
void hr_sdb_batch_op_trace(context_t *ctx, uint16_t op_num, splinterdb *spl_handle);
// void hr_bt_batch_op_trace(context_t *ctx, uint16_t op_num, BtDb *bt);
void hr_bt_batch_op_trace(context_t *ctx, uint16_t op_num, bp_db_t* tree);

// void hr_bt_batch_op_invs(context_t *ctx, BtDb *bt);
// TODO: fix
void hr_bt_batch_op_invs(context_t *ctx, bp_db_t* tree);

#endif //ODYSSEY_HR_KVS_UTIL_H
