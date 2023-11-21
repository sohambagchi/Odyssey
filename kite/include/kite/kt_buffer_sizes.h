//
// Created by vasilis on 22/05/20.
//

#ifndef KITE_BUFFER_SIZES_H
#define KITE_BUFFER_SIZES_H


// BUFFER SIZES
/*For reads and writes we want more slots than RECV_WRS because of multicasts:
 * if the machine sleeps, it will keep "receiving" messages. If the buffer space is smaller
 * than the receives_wrs, then the buffer will get corrupted!*/
#define R_BUF_SLOTS MAX((REM_MACH_NUM * R_CREDITS), (MAX_RECV_R_WRS + 1))
#define R_BUF_SIZE (R_RECV_SIZE * R_BUF_SLOTS)

#define R_REP_BUF_SLOTS ((REM_MACH_NUM * R_CREDITS) + R_REP_SLOTS_FOR_ACCEPTS)
#define R_REP_BUF_SIZE (R_REP_RECV_SIZE * R_REP_BUF_SLOTS)

#define W_BUF_SLOTS MAX((REM_MACH_NUM * W_CREDITS), (MAX_RECV_W_WRS + 1))
#define W_BUF_SIZE (W_RECV_SIZE * W_BUF_SLOTS)

#define ACK_BUF_SLOTS (REM_MACH_NUM * W_CREDITS)
//#define ACK_BUF_SIZE (CTX_ACK_RECV_SIZE * ACK_BUF_SLOTS)


#define W_BCAST_SS_BATCH MAX((MIN_SS_BATCH / (REM_MACH_NUM)), (MESSAGES_IN_BCAST_BATCH + 1))
#define R_BCAST_SS_BATCH MAX((MIN_SS_BATCH / (REM_MACH_NUM)), (MESSAGES_IN_BCAST_BATCH + 2))
#define R_REP_SS_BATCH MAX(MIN_SS_BATCH, (MAX_R_REP_WRS + 1))
#define ACK_SS_BATCH MAX(MIN_SS_BATCH, (MAX_ACK_WRS + 2))


//  Receive
#define RECV_ACK_Q_DEPTH (MAX_RECV_ACK_WRS + 3)
#define RECV_W_Q_DEPTH  (MAX_RECV_W_WRS + 3) //
#define RECV_R_Q_DEPTH (MAX_RECV_R_WRS + 3) //
#define RECV_R_REP_Q_DEPTH (MAX_RECV_R_REP_WRS + 3)

// Send
#define SEND_ACK_Q_DEPTH ((2 * ACK_SS_BATCH) + 3)
#define SEND_W_Q_DEPTH ((2 * W_BCAST_SS_BATCH * REM_MACH_NUM) + 3) // Do not question or doubt the +3!!
#define SEND_R_Q_DEPTH ((2 * R_BCAST_SS_BATCH * REM_MACH_NUM) + 3) //
#define SEND_R_REP_Q_DEPTH ((2 * R_REP_SS_BATCH) + 3)



#endif //KITE_BUFFER_SIZES_H
