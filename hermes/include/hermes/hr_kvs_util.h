//
// Created by vasilis on 08/09/20.
//

#ifndef ODYSSEY_HR_KVS_UTIL_H
#define ODYSSEY_HR_KVS_UTIL_H

#include <od_network_context.h>
#include <od_netw_func.h>
#include "od_kvs.h"
#include "hr_config.h"
#include "bplus.h"


void hr_KVS_batch_op_trace(context_t *ctx, uint16_t op_num);

void hr_KVS_batch_op_invs(context_t *ctx);

// void hr_bt_batch_op_trace(context_t *ctx, uint16_t op_num, BtDb *bt);
void hr_bt_batch_op_trace(context_t *ctx, uint16_t op_num, bp_db_t *tree);

// void hr_bt_batch_op_invs(context_t *ctx, BtDb *bt);
void hr_bt_batch_op_invs(context_t *ctx, bp_db_t *tree);

#endif //ODYSSEY_HR_KVS_UTIL_H
