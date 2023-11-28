//
// Created by vasilis on 20/08/20.
//

#include "od_wrkr_prot_sel.h"
#include "od_init_connect.h"
// #include "btree2v.h"
#include "bplus.h"

void *worker(void *arg)
{

  // BtDb *bt = (BtDb*)malloc(sizeof(BtDb));
  bp_db_t tree;
  int ret = bp_open(tree, "bplus.bp");

  if (ret != 0) {
      printf("Unable to create bplustree instance\n");
      return 1;
  }
  printf("Successfully created Btree instance\n");

  struct thread_params params = *(struct thread_params *) arg;
  uint16_t t_id = (uint16_t) params.id;

  if (t_id == 0) {
    my_printf(yellow, "Machine-id %d \n",
              machine_id);
    if (ENABLE_MULTICAST) my_printf(cyan, "MULTICAST IS ENABLED \n");
  }

  context_t *ctx = create_ctx((uint8_t) machine_id,
                              (uint16_t) params.id,
                              (uint16_t) QP_NUM,
                              local_ip);
 appl_init_qp_meta(ctx, tree);

 set_up_ctx(ctx);

  /// Connect with other machines and exchange qp information
  setup_connections_and_spawn_stats_thread(ctx);
  // We can set up the send work requests now that
  // we have address handles for remote machines
  init_ctx_send_wrs(ctx);

  /// Application specific context
  ctx->appl_ctx = (void*) set_up_appl_ctx(ctx);

  if (t_id == 0)
    my_printf(green, "Worker %d  reached the loop "
      "%d sessions \n", t_id, SESSIONS_PER_THREAD);

  ///
  main_loop(ctx, tree);

    bp_close(&tree);

  return NULL;
};