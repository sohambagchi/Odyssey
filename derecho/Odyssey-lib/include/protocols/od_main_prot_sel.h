//
// Created by vasilis on 14/09/20.
//

#ifndef ODYSSEY_OD_MAIN_PROT_SEL_H
#define ODYSSEY_OD_MAIN_PROT_SEL_H


#ifdef KITE
  #include "kt_main.h"
  #include "kt_util.h"
  #define appl_init_func kite_init_functionality
#endif

#ifdef PAXOS
  #include "cp_main.h"
  #include "cp_util.h"
  #define appl_init_func cp_init_functionality
#endif

#ifdef ZOOKEEPER
  #include <zk_util.h>
  #define appl_init_func zk_init_functionality
#endif

#ifdef DERECHO
  #include "dr_util.h"
  #define appl_init_func dr_init_functionality
#endif

#ifdef HERMES
  #include "hr_util.h"
  #define appl_init_func hr_init_functionality
#endif

#ifdef CHT
  #include "cht_util.h"
  #define appl_init_func cht_init_functionality
#endif

#ifdef CRAQ
  #include "cr_util.h"
  #define appl_init_func cr_init_functionality
#endif

#endif //ODYSSEY_OD_MAIN_PROT_SEL_H
