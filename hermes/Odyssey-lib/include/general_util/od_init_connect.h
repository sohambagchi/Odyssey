//
// Created by vasilis on 24/06/2020.
//

#ifndef KITE_INIT_CONNECT_H
#define KITE_INIT_CONNECT_H
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "od_top.h"
#include "od_hrd.h"

static int spawn_stats_thread() {
    // todo: here stats are spawned
  pthread_t *thread_arr = (pthread_t *) malloc(sizeof(pthread_t));
  pthread_attr_t attr;
  cpu_set_t cpus_stats;
  int core = -1;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpus_stats);
  if(num_threads > 17) {
    core = 39;
  } else if(2 * (num_threads) + 2 < TOTAL_CORES){
    core = 2 * (num_threads) + 2;
  } else{
    core = num_threads + 1;
  }
  CPU_SET(core, &cpus_stats);
  my_printf(yellow, "Creating stats thread at core %d\n", core);
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_stats);
  return pthread_create(&thread_arr[0], &attr, print_stats, NULL);
}




//--------------------------------------------------
//--------------PUBLISHING QPS---------------------
//--------------------------------------------------


// Worker calls this function to connect with all workers
static void get_qps_from_all_other_machines(context_t *ctx)
{
  int g_i, qp_i, w_i, m_i;
  int ib_port_index = 0;
  // -- CONNECT WITH EVERYONE
  for(g_i = 0; g_i < WORKER_NUM; g_i++) {
    if (g_i / WORKERS_PER_MACHINE == machine_id) continue; // skip the local machine
    w_i = g_i % WORKERS_PER_MACHINE;
    m_i = g_i / WORKERS_PER_MACHINE;
    for (qp_i = 0; qp_i < ctx->qp_num; qp_i++) {
      /* Compute the control block and physical port index for client @i */
      int local_port_i = ib_port_index;
      assert(all_qp_attr->wrkr_qp != NULL);
      assert(all_qp_attr->wrkr_qp[m_i] != NULL);
      assert(all_qp_attr->wrkr_qp[m_i][w_i] != NULL);
      assert(&all_qp_attr->wrkr_qp[m_i][w_i][qp_i] != NULL);
      //printf("Machine %u Wrkr %u, qp %u \n", m_i, w_i, qp_i);
      qp_attr_t *wrkr_qp = &all_qp_attr->wrkr_qp[m_i][w_i][qp_i];


      struct ibv_ah_attr ah_attr = {
        //-----INFINIBAND----------
        .is_global = 0,
        .dlid = (uint16_t) wrkr_qp->lid,
        .sl = (uint8_t) wrkr_qp->sl,
        .src_path_bits = 0,
        /* port_num (> 1): device-local port for responses to this worker */
        .port_num = (uint8_t) (local_port_i + 1),
      };
      // ---ROCE----------
      if (is_roce == 1) {
        ah_attr.is_global = 1;
        ah_attr.dlid = 0;
        ah_attr.grh.dgid.global.interface_id =  wrkr_qp->gid_global_interface_id;
        ah_attr.grh.dgid.global.subnet_prefix = wrkr_qp->gid_global_subnet_prefix;
        ah_attr.grh.sgid_index = 0;
        ah_attr.grh.hop_limit = 1;
      }
      rem_qp[m_i][w_i][qp_i].ah = ibv_create_ah(ctx->rdma_ctx->pd, &ah_attr);
      // EINVAl = wrong pd or ah, ENOMEM = not enough resources
      if (rem_qp[m_i][w_i][qp_i].ah == NULL) {
        printf("Could not create address handle. "
               "Error no: %d/%u/%u\n", errno, EINVAL, ENOMEM);
        assert(true);
      }
      rem_qp[m_i][w_i][qp_i].qpn = wrkr_qp->qpn;
      // printf("%d %d %d success\n", m_i, w_i, qp_i );
    }
  }
}


#define BASE_SOCKET_PORT 8080

// Machines with id higher than 0 connect with machine-id 0.
// First they sent it their qps-attrs and then receive everyone's
static void set_up_qp_attr_client(int qp_num)
{
  int sock = 0, valread;
  struct sockaddr_in serv_addr;
  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
  {
    printf("\n Socket creation error \n");
    assert(false);
  }

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons((uint16_t)(BASE_SOCKET_PORT + machine_id - 1));

  // Convert IPv4 and IPv6 addresses from text to binary form
  if(inet_pton(AF_INET, remote_ips[0], &serv_addr.sin_addr) <= 0)
  {
    printf("\nInvalid address/ Address not supported \n");
    assert(false);
  }
  uint32_t error_counter = 0;
  while (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    error_counter++;
    if (error_counter == MILLION)
      my_printf(red, "Stuck in loop: machine %d is trying to "
                     "connect to socket to exchange qps \n", machine_id);
  }

  struct qp_attr *qp_attr_to_send = &all_qp_attr->wrkr_qp[machine_id][0][0];
  size_t send_size = WORKERS_PER_MACHINE * qp_num * sizeof(struct qp_attr);
  send(sock, qp_attr_to_send, send_size, 0);
  struct qp_attr tmp[WORKERS_PER_MACHINE][qp_num];
  memcpy(tmp, qp_attr_to_send, send_size);
  //printf("Attributes sent\n");
  size_t recv_size = sizeof(qp_attr_t) * MACHINE_NUM * WORKERS_PER_MACHINE * qp_num;
  valread = (int) recv(sock, all_qp_attr->buf, recv_size, MSG_WAITALL);
  assert(valread == recv_size);
  int cmp = memcmp(qp_attr_to_send, qp_attr_to_send, send_size);
  assert(cmp == 0);
  //printf("Received all attributes, capacity %ld \n", sizeof(all_qp_attr_t));
}

// Machine 0 acts as a "server"; it receives all qp attributes,
// and broadcasts them to everyone
static void set_up_qp_attr_server(int qp_num)
{
  int server_fd[REM_MACH_NUM], new_socket[REM_MACH_NUM], valread;
  struct sockaddr_in address;
  int opt = 1;
  int addrlen = sizeof(address);

  for (int rm_i = 0; rm_i < REM_MACH_NUM; rm_i++) {
    // Creating socket file descriptor
    if ((server_fd[rm_i] = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
      printf("socket failed \n");
      assert(false);
    }

    // Forcefully attaching socket to the port 8080
    if (setsockopt(server_fd[rm_i], SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
                   &opt, sizeof(opt))) {
      printf("setsockopt \n");
      assert(false);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons((uint16_t)(BASE_SOCKET_PORT + rm_i));

    // Forcefully attaching socket to the port 8080
    if (bind(server_fd[rm_i], (struct sockaddr *) &address,
             sizeof(address)) < 0) {
      printf("bind failed \n");
      assert(false);
    }
     //printf("Succesful bind \n");
    if (listen(server_fd[rm_i], 3) < 0) {
      printf("listen");
      assert(false);
    }
     //printf("Succesful listen \n");
    if ((new_socket[rm_i] = accept(server_fd[rm_i], (struct sockaddr *) &address,
                                   (socklen_t *) &addrlen)) < 0) {
      //printf("accept");
    }
     //printf("Successful accept \n");
    size_t recv_size = WORKERS_PER_MACHINE * qp_num * sizeof(struct qp_attr);
    valread = (int) recv(new_socket[rm_i], &all_qp_attr->wrkr_qp[rm_i + 1][0][0], recv_size, MSG_WAITALL);
    assert(valread == recv_size);
    //printf("Server received qp_attributes from machine %u capacity %ld \n",
    //       rm_i + 1, recv_size);
  }
  size_t send_size = sizeof(qp_attr_t) * MACHINE_NUM * WORKERS_PER_MACHINE * qp_num;
  for (int rm_i = 0; rm_i < REM_MACH_NUM; rm_i++) {
    send(new_socket[rm_i], all_qp_attr->buf, send_size, 0);
  }
}


// Used by all kinds of threads to publish their QPs
static void fill_qps(context_t *ctx)
{
  rdma_context_t *r_ctx = ctx->rdma_ctx;
  uint32_t qp_i;
  for (qp_i = 0; qp_i <ctx->qp_num; qp_i++) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_i];
    struct qp_attr *qp_attr = &all_qp_attr->wrkr_qp[machine_id][ctx->t_id][qp_i];
    qp_attr->lid = hrd_get_local_lid(qp_meta->send_qp->context, r_ctx->dev_port_id);
    qp_attr->qpn = qp_meta->send_qp->qp_num;
    qp_attr->sl = DEFAULT_SL;
    //   ---ROCE----------
    if (is_roce == 1) {
      union ibv_gid ret_gid;
      int ret = ibv_query_gid(r_ctx->ibv_ctx, IB_PHYS_PORT, 0, &ret_gid);
      assert(ret == 0);
      qp_attr->gid_global_interface_id = ret_gid.global.interface_id;
      qp_attr->gid_global_subnet_prefix = ret_gid.global.subnet_prefix;
    }
  }
  // Signal to other threads that you have filled your qp attributes
  atomic_fetch_add_explicit(&workers_with_filled_qp_attr, 1, memory_order_seq_cst);
}

// All workers both use this to establish connections
static void setup_connections(context_t *ctx)
{
  fill_qps(ctx);
  if (ctx->t_id == 0) {
    while(workers_with_filled_qp_attr != WORKERS_PER_MACHINE);
    if (machine_id == 0) set_up_qp_attr_server(ctx->qp_num);
    else set_up_qp_attr_client(ctx->qp_num);
    get_qps_from_all_other_machines(ctx);
  }
}

static void thread_zero_spawns_stat_thread(uint16_t t_id)
{
  if (t_id == 0) {
    if (CLIENT_MODE != CLIENT_UI) {
      if (spawn_stats_thread() != 0)
        my_printf(red, "Stats thread was not successfully spawned \n");
    }
  }
}



static void setup_connections_and_spawn_stats_thread(context_t *ctx)
{
  setup_connections(ctx);
  thread_zero_spawns_stat_thread(ctx->t_id);
  if (ctx->t_id == 0) atomic_store_explicit(&qps_are_set_up, true, memory_order_release);
  else {
    while (!atomic_load_explicit(&qps_are_set_up, memory_order_acquire));
    usleep(200000);
  }
}

#endif //KITE_INIT_CONNECT_H
