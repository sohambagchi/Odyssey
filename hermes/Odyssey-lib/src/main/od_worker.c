//
// Created by vasilis on 20/08/20.
//

#include "od_wrkr_prot_sel.h"

#include "od_init_connect.h"
#include <stdio.h>
#include <string.h>

#include "default_data_config.h"
#include "splinterdb.h"

#define DB_FILE_NAME    "splinterdb_intro_db"
#define USER_MAX_KEY_SIZE ((int)100)
#define DB_FILE_SIZE_MB 1024 // Size of SplinterDB device; Fixed when created
#define CACHE_SIZE_MB   64

void *worker(void *arg)
{
  struct thread_params params = *(struct thread_params *) arg;
  uint16_t t_id = (uint16_t) params.id;

  if (t_id == 0) {
    my_printf(yellow, "Machine-id %d \n",
              machine_id);
    if (ENABLE_MULTICAST) my_printf(cyan, "MULTICAST IS ENABLED \n");
  }

    data_config splinter_data_cfg;
    default_data_config_init(USER_MAX_KEY_SIZE, &splinter_data_cfg);
    splinterdb_config splinterdb_cfg;
    memset(&splinterdb_cfg, 0, sizeof(splinterdb_cfg));
    splinterdb_cfg.filename   = DB_FILE_NAME;
    splinterdb_cfg.disk_size  = (DB_FILE_SIZE_MB * 1024 * 1024);
    splinterdb_cfg.cache_size = (CACHE_SIZE_MB * 1024 * 1024);
    splinterdb_cfg.data_cfg   = &splinter_data_cfg;
    splinterdb *spl_handle = NULL; // To a running SplinterDB instance
    int rc = splinterdb_create(&splinterdb_cfg, &spl_handle);
    printf("Created SplinterDB instance, dbname '%s'.\n\n", DB_FILE_NAME);
  context_t *ctx = create_ctx((uint8_t) machine_id,
                              (uint16_t) params.id,
                              (uint16_t) QP_NUM,
                              local_ip);
  // todo: create an instance of splinterdb here
 appl_init_qp_meta(ctx, spl_handle);

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
  main_loop(ctx);


  return NULL;
};