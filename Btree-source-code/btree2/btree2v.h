#ifndef BTREE_2_V
#define BTREE_2_V

#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE

#ifdef linux
#define _GNU_SOURCE
#include <linux/futex.h>
#include <limits.h>
#define SYS_futex 202
#endif

#ifdef unix
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <io.h>
#endif

#include <memory.h>
#include <string.h>

typedef unsigned long long	uid;

#ifndef unix
typedef unsigned long long	off64_t;
typedef unsigned short		ushort;
typedef unsigned int		uint;
#endif

#define BT_ro 0x6f72	// ro
#define BT_rw 0x7772	// rw
#define BT_fl 0x6c66	// fl

#define BT_maxbits		15					// maximum page size in bits
#define BT_minbits		12					// minimum page size in bits
#define BT_minpage		(1 << BT_minbits)	// minimum page size
#define BT_maxpage		(1 << BT_maxbits)	// maximum page size

//  BTree page number constants
#define ALLOC_page		0
#define ROOT_page		1
#define LEAF_page		2
#define LATCH_page		3

//	Number of levels to create in a new BTree

#define MIN_lvl			2
#define MAX_lvl			15

/*
There are five lock types for each node in three independent sets: 
1. (set 1) AccessIntent: Sharable. Going to Read the node. Incompatible with NodeDelete. 
2. (set 1) NodeDelete: Exclusive. About to release the node. Incompatible with AccessIntent. 
3. (set 2) ReadLock: Sharable. Read the node. Incompatible with WriteLock. 
4. (set 2) WriteLock: Exclusive. Modify the node. Incompatible with ReadLock and other WriteLocks. 
5. (set 3) ParentModification: Exclusive. Change the node's parent keys. Incompatible with another ParentModification. 
*/

typedef enum{
	BtLockAccess,
	BtLockDelete,
	BtLockRead,
	BtLockWrite,
	BtLockParent
}BtLock;

enum {
	QueRd = 1,		// reader queue
	QueWr = 2		// writer queue
} RWQueue;

volatile typedef struct {
	ushort rin[1];		// readers in count
	ushort rout[1];		// readers out count
	ushort ticket[1];	// writers in count
	ushort serving[1];	// writers out count
} RWLock;

//	define bits at bottom of rin

#define PHID 0x1	// writer phase (0/1)
#define PRES 0x2	// writer present
#define MASK 0x3	// both write bits
#define RINC 0x4	// reader increment

typedef struct {
  union {
	struct {
	  ushort serving;
	  ushort next;
	} bits[1];
	uint value[1];
  };
} BtMutex;

//	Define the length of the page and key pointers

#define BtId 6

//	Page key slot definition.

//	If BT_maxbits is 15 or less, you can save 2 bytes
//	for each key stored by making the first two uints
//	into ushorts.  You can also save 4 bytes by removing
//	the tod field from the key.

//	Keys are marked dead, but remain on the page until
//	cleanup is called. The fence key (highest key) for
//	the page is always present, even if dead.

typedef struct {
#ifdef USETOD
	uint tod;					// time-stamp for key
#endif
	ushort off:BT_maxbits;		// page offset for key start
	ushort dead:1;				// set for deleted key
	unsigned char id[BtId];		// id associated with key
} BtSlot;

//	The key structure occupies space at the upper end of
//	each page.  It's a length byte followed by the value
//	bytes.

typedef struct {
	unsigned char len;
	unsigned char key[0];
} *BtKey;

//	The first part of an index page.
//	It is immediately followed
//	by the BtSlot array of keys.

typedef struct BtPage_ {
	uint cnt;					// count of keys in page
	uint act;					// count of active keys
	uint min;					// next key offset
	unsigned char bits:6;		// page size in bits
	unsigned char free:1;		// page is on free list
	unsigned char dirty:1;		// page is dirty in cache
	unsigned char lvl:6;		// level of page
	unsigned char kill:1;		// page is being deleted
	unsigned char clean:1;		// page needs cleaning
	unsigned char right[BtId];	// page number to right
} *BtPage;

typedef struct {
	struct BtPage_ alloc[2];	// next & free page_nos in right ptr
	BtMutex lock[1];			// allocation area lite latch
	volatile uint latchdeployed;// highest number of latch entries deployed
	volatile uint nlatchpage;	// number of latch pages at BT_latch
	volatile uint latchtotal;	// number of page latch entries
	volatile uint latchhash;	// number of latch hash table slots
	volatile uint latchvictim;	// next latch hash entry to examine
	volatile uint safelevel;	// safe page level in cache
	volatile uint cache[MAX_lvl];// cache census counts by btree level
} BtLatchMgr;

//  latch hash table entries

typedef struct {
	unsigned char busy[1];	// Latch table entry is busy being reallocated
	uint slot:24;			// Latch table entry at head of collision chain
} BtHashEntry;

//	latch manager table structure

typedef struct {
	volatile uid page_no;	// latch set page number on disk
	RWLock readwr[1];		// read/write page lock
	RWLock access[1];		// Access Intent/Page delete
	RWLock parent[1];		// Posting of fence key in parent
	volatile ushort pin;	// number of pins/level/clock bits
	volatile uint next;		// next entry in hash table chain
	volatile uint prev;		// prev entry in hash table chain
} BtLatchSet;

#define CLOCK_mask 0xe000
#define CLOCK_unit 0x2000
#define PIN_mask 0x07ff
#define LVL_mask 0x1800
#define LVL_shift 11

//	The object structure for Btree access

typedef struct _BtDb {
	uint page_size;		// each page size	
	uint page_bits;		// each page size in bits	
	uid page_no;		// current page number	
	uid cursor_page;	// current cursor page number	
	int  err;
	uint mode;			// read-write mode
	BtPage cursor;		// cached frame for start/next (never mapped)
	BtPage frame;		// spare frame for the page split (never mapped)
	BtPage page;		// current mapped page in buffer pool
	BtLatchSet *latch;			// current page latch
	BtLatchMgr *latchmgr;		// mapped latch page from allocation page
	BtLatchSet *latchsets;		// mapped latch set from latch pages
	unsigned char *pagepool;	// cached page pool set
	BtHashEntry *table;	// the hash table
#ifdef unix
	int idx;
#else
	HANDLE idx;
	HANDLE halloc;		// allocation and latch table handle
#endif
	unsigned char *mem;	// frame, cursor, memory buffers
	uint found;			// last deletekey found key
} BtDb;

typedef enum {
BTERR_ok = 0,
BTERR_notfound,
BTERR_struct,
BTERR_ovflw,
BTERR_read,
BTERR_lock,
BTERR_hash,
BTERR_kill,
BTERR_map,
BTERR_wrt,
BTERR_eof
} BTERR;

// B-Tree functions
extern void bt_close (BtDb *bt);
extern BtDb *bt_open (char *name, uint mode, uint bits, uint cacheblk);
extern BTERR  bt_insertkey (BtDb *bt, unsigned char *key, uint len, uint lvl, uid id, uint tod);
extern BTERR  bt_deletekey (BtDb *bt, unsigned char *key, uint len, uint lvl);
extern uid bt_findkey    (BtDb *bt, unsigned char *key, uint len);
extern uint bt_startkey  (BtDb *bt, unsigned char *key, uint len);
extern uint bt_nextkey   (BtDb *bt, uint slot);

//	internal functions
void bt_update (BtDb *bt, BtPage page);
BtPage bt_mappage (BtDb *bt, BtLatchSet *latch);
//  Helper functions to return slot values

extern BtKey bt_key (BtDb *bt, uint slot);
extern uid bt_uid (BtDb *bt, uint slot);
#ifdef USETOD
extern uint bt_tod (BtDb *bt, uint slot);
#endif

//  The page is allocated from low and hi ends.
//  The key offsets and row-id's are allocated
//  from the bottom, while the text of the key
//  is allocated from the top.  When the two
//  areas meet, the page is split into two.

//  A key consists of a length byte, two bytes of
//  index number (0 - 65534), and up to 253 bytes
//  of key value.  Duplicate keys are discarded.
//  Associated with each key is a 48 bit row-id.

//  The b-tree root is always located at page 1.
//	The first leaf page of level zero is always
//	located on page 2.

//	The b-tree pages are linked with right
//	pointers to facilitate enumerators,
//	and provide for concurrency.

//	When to root page fills, it is split in two and
//	the tree height is raised by a new root at page
//	one with two keys.

//	Deleted keys are marked with a dead bit until
//	page cleanup The fence key for a node is always
//	present, even after deletion and cleanup.

//  Deleted leaf pages are reclaimed  on a free list.
//	The upper levels of the btree are fixed on creation.

//  To achieve maximum concurrency one page is locked at a time
//  as the tree is traversed to find leaf key in question. The right
//  page numbers are used in cases where the page is being split,
//	or consolidated.

//  Page 0 (ALLOC page) is dedicated to lock for new page extensions,
//	and chains empty leaf pages together for reuse.

//	Parent locks are obtained to prevent resplitting or deleting a node
//	before its fence is posted into its upper level.

//	A special open mode of BT_fl is provided to safely access files on
//	WIN32 networks. WIN32 network operations should not use memory mapping.
//	This WIN32 mode sets FILE_FLAG_NOBUFFERING and FILE_FLAG_WRITETHROUGH
//	to prevent local caching of network file contents.

//	Access macros to address slot and key values from the page.
//	Page slots use 1 based indexing.

#define slotptr(page, slot) (((BtSlot *)(page+1)) + (slot-1))
#define keyptr(page, slot) ((BtKey)((unsigned char*)(page) + slotptr(page, slot)->off))

void bt_putid(unsigned char *dest, uid id);

uid bt_getid(unsigned char *src);

BTERR bt_abort(BtDb *bt, BtPage page, uid page_number, BTERR error);

int sys_futex(void *x, int op, int val1, struct timespec *t, void* y, int val3);

// B-Tree functions
void bt_close (BtDb *bt);
BtDb *bt_open (char *name, uint mode, uint bits, uint cacheblk);
BTERR  bt_insertkey (BtDb *bt, unsigned char *key, uint len, uint lvl, uid id, uint tod);
BTERR  bt_deletekey (BtDb *bt, unsigned char *key, uint len, uint lvl);
uid bt_findkey    (BtDb *bt, unsigned char *key, uint len);
uint bt_startkey  (BtDb *bt, unsigned char *key, uint len);
uint bt_nextkey   (BtDb *bt, uint slot);

#endif