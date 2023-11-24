// btree version threadskv7 sched_yield version
//	with reworked bt_deletekey code,
//	phase-fair reader writer lock,
//	librarian page split code,
//	duplicate key management
//	bi-directional cursors
//	traditional buffer pool manager
//	and atomic non-consistent key insert

// 17 SEP 2014

// author: karl malbrain, malbrain@cal.berkeley.edu

/*
This work, including the source code, documentation
and related data, is placed into the public domain.

The orginal author is Karl Malbrain.

THIS SOFTWARE IS PROVIDED AS-IS WITHOUT WARRANTY
OF ANY KIND, NOT EVEN THE IMPLIED WARRANTY OF
MERCHANTABILITY. THE AUTHOR OF THIS SOFTWARE,
ASSUMES _NO_ RESPONSIBILITY FOR ANY CONSEQUENCE
RESULTING FROM THE USE, MODIFICATION, OR
REDISTRIBUTION OF THIS SOFTWARE.
*/

// Please see the project home page for documentation
// code.google.com/p/high-concurrency-btree

#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE

#ifdef linux
#define _GNU_SOURCE
#endif

#ifdef unix
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <errno.h>
#include <pthread.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <process.h>
#include <intrin.h>
#endif

#include <memory.h>
#include <string.h>
#include <stddef.h>

typedef unsigned long long	uid;

#ifndef unix
typedef unsigned long long	off64_t;
typedef unsigned short		ushort;
typedef unsigned int		uint;
#endif

#define BT_ro 0x6f72	// ro
#define BT_rw 0x7772	// rw

#define BT_maxbits		24					// maximum page size in bits
#define BT_minbits		9					// minimum page size in bits
#define BT_minpage		(1 << BT_minbits)	// minimum page size
#define BT_maxpage		(1 << BT_maxbits)	// maximum page size

//  BTree page number constants
#define ALLOC_page		0	// allocation page
#define ROOT_page		1	// root of the btree
#define LEAF_page		2	// first page of leaves

//	Number of levels to create in a new BTree

#define MIN_lvl			2

//	maximum number of keys to insert atomically in one call

#define MAX_atomic		256

#define BT_maxkey	255		// maximum number of bytes in a key
#define BT_keyarray (BT_maxkey + sizeof(BtKey))

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
} BtLock;

//	definition for phase-fair reader/writer lock implementation

typedef struct {
	ushort rin[1];
	ushort rout[1];
	ushort ticket[1];
	ushort serving[1];
} RWLock;

#define PHID 0x1
#define PRES 0x2
#define MASK 0x3
#define RINC 0x4

//	definition for spin latch implementation

// exclusive is set for write access
// share is count of read accessors
// grant write lock when share == 0

volatile typedef struct {
	uint exclusive:1;
	uint pending:1;
	uint share:30;
} BtSpinLatch;

#define XCL 1
#define PEND 2
#define BOTH 3
#define SHARE 4

//	The key structure occupies space at the upper end of
//	each page.  It's a length byte followed by the key
//	bytes.

typedef struct {
	unsigned char len;		// this can be changed to a ushort or uint
	unsigned char key[0];
} BtKey;

//	the value structure also occupies space at the upper
//	end of the page. Each key is immediately followed by a value.

typedef struct {
	unsigned char len;		// this can be changed to a ushort or uint
	unsigned char value[0];
} BtVal;

//  hash table entries

typedef struct {
	uint slot;				// Latch table entry at head of chain
	BtSpinLatch latch[1];
} BtHashEntry;

//	latch manager table structure

typedef struct {
	uid page_no;			// latch set page number
	RWLock readwr[1];		// read/write page lock
	RWLock access[1];		// Access Intent/Page delete
	RWLock parent[1];		// Posting of fence key in parent
	uint slot;				// entry slot in latch table
	uint next;				// next entry in hash table chain
	uint prev;				// prev entry in hash table chain
	volatile ushort pin;	// number of outstanding threads
	ushort dirty:1;			// page in cache is dirty
} BtLatchSet;

//	lock manager table structure

typedef struct {
	RWLock readwr[1];		// read/write key lock
	uint next;
	uint prev;
	uint pin;				// count of readers waiting
	uint hashidx;			// hash idx for entry
	unsigned char key[BT_keyarray];
} BtLockSet;

//	Define the length of the page record numbers

#define BtId 6

//	Page key slot definition.

//	Keys are marked dead, but remain on the page until
//	it cleanup is called. The fence key (highest key) for
//	a leaf page is always present, even after cleanup.

//	Slot types

//	In addition to the Unique keys that occupy slots
//	there are Librarian and Duplicate key
//	slots occupying the key slot array.

//	The Librarian slots are dead keys that
//	serve as filler, available to add new Unique
//	or Dup slots that are inserted into the B-tree.

//	The Duplicate slots have had their key bytes extended
//	by 6 bytes to contain a binary duplicate key uniqueifier.

typedef enum {
	Unique,
	Librarian,
	Duplicate
} BtSlotType;

typedef struct {
	uint off:BT_maxbits;	// page offset for key start
	uint type:3;			// type of slot
	uint dead:1;			// set for deleted slot
} BtSlot;

//	The first part of an index page.
//	It is immediately followed
//	by the BtSlot array of keys.

//	note that this structure size
//	must be a multiple of 8 bytes
//	in order to place dups correctly.

typedef struct BtPage_ {
	uint cnt;					// count of keys in page
	uint act;					// count of active keys
	uint min;					// next key offset
	uint garbage;				// page garbage in bytes
	unsigned char bits:7;		// page size in bits
	unsigned char free:1;		// page is on free chain
	unsigned char lvl:7;		// level of page
	unsigned char kill:1;		// page is being deleted
	unsigned char left[BtId];	// page number to left
	unsigned char filler[2];	// padding to multiple of 8
	unsigned char right[BtId];	// page number to right
} *BtPage;

//  The loadpage interface object

typedef struct {
	uid page_no;		// current page number
	BtPage page;		// current page pointer
	BtLatchSet *latch;	// current page latch set
} BtPageSet;

//	structure for latch manager on ALLOC_page

typedef struct {
	struct BtPage_ alloc[1];	// next page_no in right ptr
	unsigned long long dups[1];	// global duplicate key uniqueifier
	unsigned char chain[BtId];	// head of free page_nos chain
} BtPageZero;

//	The object structure for Btree access

typedef struct {
	uint page_size;				// page size	
	uint page_bits;				// page size in bits	
#ifdef unix
	int idx;
#else
	HANDLE idx;
#endif
	BtPageZero *pagezero;		// mapped allocation page
	BtSpinLatch alloclatch[1];	// allocation area lite latch
	uint latchdeployed;			// highest number of pool entries deployed
	uint nlatchpage;			// number of latch & lock & pool pages
	uint latchtotal;			// number of page latch entries
	uint latchhash;				// number of latch hash table slots
	uint latchvictim;			// next latch entry to examine
	BtHashEntry *hashtable;		// the anonymous mapping buffer pool
	BtLatchSet *latchsets;		// mapped latch set from latch pages
	unsigned char *pagepool;	// mapped to the buffer pool pages
	uint lockhash;				// number of lock hash table slots
	uint lockfree;				// next available lock table entry
	BtSpinLatch locklatch[1];	// lock manager free chain latch
	BtHashEntry *hashlock;		// the lock manager hash table
	BtLockSet *locktable;		// the lock manager key table
#ifndef unix
	HANDLE halloc;				// allocation handle
	HANDLE hpool;				// buffer pool handle
#endif
} BtMgr;

typedef struct {
	BtMgr *mgr;				// buffer manager for thread
	BtPage cursor;			// cached frame for start/next (never mapped)
	BtPage frame;			// spare frame for the page split (never mapped)
	uid cursor_page;				// current cursor page number	
	unsigned char *mem;				// frame, cursor, page memory buffer
	unsigned char key[BT_keyarray];	// last found complete key
	int found;				// last delete or insert was found
	int err;				// last error
	int reads, writes;		// number of reads and writes from the btree
} BtDb;

typedef enum {
	BTERR_ok = 0,
	BTERR_struct,
	BTERR_ovflw,
	BTERR_lock,
	BTERR_map,
	BTERR_read,
	BTERR_wrt,
	BTERR_hash
} BTERR;

#define CLOCK_bit 0x8000

// B-Tree functions
extern void bt_close (BtDb *bt);
extern BtDb *bt_open (BtMgr *mgr);
extern BTERR bt_insertkey (BtDb *bt, unsigned char *key, uint len, uint lvl, void *value, uint vallen, int unique);
extern BTERR  bt_deletekey (BtDb *bt, unsigned char *key, uint len, uint lvl);
extern int bt_findkey    (BtDb *bt, unsigned char *key, uint keylen, unsigned char *value, uint valmax);
extern BtKey *bt_foundkey (BtDb *bt);
extern uint bt_startkey  (BtDb *bt, unsigned char *key, uint len);
extern uint bt_nextkey   (BtDb *bt, uint slot);

//	manager functions
extern BtMgr *bt_mgr (char *name, uint bits, uint poolsize, uint locksize);
void bt_mgrclose (BtMgr *mgr);

//  Helper functions to return slot values
//	from the cursor page.

extern BtKey *bt_key (BtDb *bt, uint slot);
extern BtVal *bt_val (BtDb *bt, uint slot);

//  The page is allocated from low and hi ends.
//  The key slots are allocated from the bottom,
//	while the text and value of the key
//  are allocated from the top.  When the two
//  areas meet, the page is split into two.

//  A key consists of a length byte, two bytes of
//  index number (0 - 65535), and up to 253 bytes
//  of key value.

//  Associated with each key is a value byte string
//	containing any value desired.

//  The b-tree root is always located at page 1.
//	The first leaf page of level zero is always
//	located on page 2.

//	The b-tree pages are linked with next
//	pointers to facilitate enumerators,
//	and provide for concurrency.

//	When to root page fills, it is split in two and
//	the tree height is raised by a new root at page
//	one with two keys.

//	Deleted keys are marked with a dead bit until
//	page cleanup. The fence key for a leaf node is
//	always present

//  To achieve maximum concurrency one page is locked at a time
//  as the tree is traversed to find leaf key in question. The right
//  page numbers are used in cases where the page is being split,
//	or consolidated.

//  Page 0 is dedicated to lock for new page extensions,
//	and chains empty pages together for reuse. It also
//	contains the latch manager hash table.

//	The ParentModification lock on a node is obtained to serialize posting
//	or changing the fence key for a node.

//	Empty pages are chained together through the ALLOC page and reused.

//	Access macros to address slot and key values from the page
//	Page slots use 1 based indexing.

#define slotptr(page, slot) (((BtSlot *)(page+1)) + (slot-1))
#define keyptr(page, slot) ((BtKey*)((unsigned char*)(page) + slotptr(page, slot)->off))
#define valptr(page, slot) ((BtVal*)(keyptr(page,slot)->key + keyptr(page,slot)->len))

void bt_putid(unsigned char *dest, uid id)
{
int i = BtId;

	while( i-- )
		dest[i] = (unsigned char)id, id >>= 8;
}

uid bt_getid(unsigned char *src)
{
uid id = 0;
int i;

	for( i = 0; i < BtId; i++ )
		id <<= 8, id |= *src++; 

	return id;
}

uid bt_newdup (BtDb *bt)
{
#ifdef unix
	return __sync_fetch_and_add (bt->mgr->pagezero->dups, 1) + 1;
#else
	return _InterlockedIncrement64(bt->mgr->pagezero->dups, 1);
#endif
}

//	Phase-Fair reader/writer lock implementation

void WriteLock (RWLock *lock)
{
ushort w, r, tix;

#ifdef unix
	tix = __sync_fetch_and_add (lock->ticket, 1);
#else
	tix = _InterlockedExchangeAdd16 (lock->ticket, 1);
#endif
	// wait for our ticket to come up

	while( tix != lock->serving[0] )
#ifdef unix
		sched_yield();
#else
		SwitchToThread ();
#endif

	w = PRES | (tix & PHID);
#ifdef  unix
	r = __sync_fetch_and_add (lock->rin, w);
#else
	r = _InterlockedExchangeAdd16 (lock->rin, w);
#endif
	while( r != *lock->rout )
#ifdef unix
		sched_yield();
#else
		SwitchToThread();
#endif
}

void WriteRelease (RWLock *lock)
{
#ifdef unix
	__sync_fetch_and_and (lock->rin, ~MASK);
#else
	_InterlockedAnd16 (lock->rin, ~MASK);
#endif
	lock->serving[0]++;
}

void ReadLock (RWLock *lock)
{
ushort w;
#ifdef unix
	w = __sync_fetch_and_add (lock->rin, RINC) & MASK;
#else
	w = _InterlockedExchangeAdd16 (lock->rin, RINC) & MASK;
#endif
	if( w )
	  while( w == (*lock->rin & MASK) )
#ifdef unix
		sched_yield ();
#else
		SwitchToThread ();
#endif
}

void ReadRelease (RWLock *lock)
{
#ifdef unix
	__sync_fetch_and_add (lock->rout, RINC);
#else
	_InterlockedExchangeAdd16 (lock->rout, RINC);
#endif
}

//	Spin Latch Manager

//	wait until write lock mode is clear
//	and add 1 to the share count

void bt_spinreadlock(BtSpinLatch *latch)
{
uint prev;

  do {
#ifdef unix
	prev = __sync_fetch_and_add ((uint *)latch, SHARE);
#else
	prev = _InterlockedExchangeAdd((uint *)latch, SHARE);
#endif
	//  see if exclusive request is granted or pending

	if( !(prev & BOTH) )
		return;
#ifdef unix
	prev = __sync_fetch_and_add ((uint *)latch, -SHARE);
#else
	prev = _InterlockedExchangeAdd((uint *)latch, -SHARE);
#endif
#ifdef  unix
  } while( sched_yield(), 1 );
#else
  } while( SwitchToThread(), 1 );
#endif
}

//	wait for other read and write latches to relinquish

void bt_spinwritelock(BtSpinLatch *latch)
{
uint prev;

  do {
#ifdef  unix
	prev = __sync_fetch_and_or((uint *)latch, PEND | XCL);
#else
	prev = _InterlockedOr((uint *)latch, PEND | XCL);
#endif
	if( !(prev & XCL) )
	  if( !(prev & ~BOTH) )
		return;
	  else
#ifdef unix
		__sync_fetch_and_and ((uint *)latch, ~XCL);
#else
		_InterlockedAnd((uint *)latch, ~XCL);
#endif
#ifdef  unix
  } while( sched_yield(), 1 );
#else
  } while( SwitchToThread(), 1 );
#endif
}

//	try to obtain write lock

//	return 1 if obtained,
//		0 otherwise

int bt_spinwritetry(BtSpinLatch *latch)
{
uint prev;

#ifdef  unix
	prev = __sync_fetch_and_or((uint *)latch, XCL);
#else
	prev = _InterlockedOr((uint *)latch, XCL);
#endif
	//	take write access if all bits are clear

	if( !(prev & XCL) )
	  if( !(prev & ~BOTH) )
		return 1;
	  else
#ifdef unix
		__sync_fetch_and_and ((uint *)latch, ~XCL);
#else
		_InterlockedAnd((uint *)latch, ~XCL);
#endif
	return 0;
}

//	clear write mode

void bt_spinreleasewrite(BtSpinLatch *latch)
{
#ifdef unix
	__sync_fetch_and_and((uint *)latch, ~BOTH);
#else
	_InterlockedAnd((uint *)latch, ~BOTH);
#endif
}

//	decrement reader count

void bt_spinreleaseread(BtSpinLatch *latch)
{
#ifdef unix
	__sync_fetch_and_add((uint *)latch, -SHARE);
#else
	_InterlockedExchangeAdd((uint *)latch, -SHARE);
#endif
}

//	read page from permanent location in Btree file

BTERR bt_readpage (BtMgr *mgr, BtPage page, uid page_no)
{
off64_t off = page_no << mgr->page_bits;

#ifdef unix
	if( pread (mgr->idx, page, mgr->page_size, page_no << mgr->page_bits) < mgr->page_size ) {
		fprintf (stderr, "Unable to read page %.8x errno = %d\n", page_no, errno);
		return BTERR_read;
	}
#else
OVERLAPPED ovl[1];
uint amt[1];

	memset (ovl, 0, sizeof(OVERLAPPED));
	ovl->Offset = off;
	ovl->OffsetHigh = off >> 32;

	if( !ReadFile(mgr->idx, page, mgr->page_size, amt, ovl)) {
		fprintf (stderr, "Unable to read page %.8x GetLastError = %d\n", page_no, GetLastError());
		return BTERR_read;
	}
	if( *amt <  mgr->page_size ) {
		fprintf (stderr, "Unable to read page %.8x GetLastError = %d\n", page_no, GetLastError());
		return BTERR_read;
	}
#endif
	return 0;
}

//	write page to permanent location in Btree file
//	clear the dirty bit

BTERR bt_writepage (BtMgr *mgr, BtPage page, uid page_no)
{
off64_t off = page_no << mgr->page_bits;

#ifdef unix
	if( pwrite(mgr->idx, page, mgr->page_size, off) < mgr->page_size )
		return BTERR_wrt;
#else
OVERLAPPED ovl[1];
uint amt[1];

	memset (ovl, 0, sizeof(OVERLAPPED));
	ovl->Offset = off;
	ovl->OffsetHigh = off >> 32;

	if( !WriteFile(mgr->idx, page, mgr->page_size, amt, ovl) )
		return BTERR_wrt;

	if( *amt <  mgr->page_size )
		return BTERR_wrt;
#endif
	return 0;
}

//	link latch table entry into head of latch hash table

BTERR bt_latchlink (BtDb *bt, uint hashidx, uint slot, uid page_no, uint loadit)
{
BtPage page = (BtPage)(((uid)slot << bt->mgr->page_bits) + bt->mgr->pagepool);
BtLatchSet *latch = bt->mgr->latchsets + slot;

	if( latch->next = bt->mgr->hashtable[hashidx].slot )
		bt->mgr->latchsets[latch->next].prev = slot;

	bt->mgr->hashtable[hashidx].slot = slot;
	latch->page_no = page_no;
	latch->slot = slot;
	latch->prev = 0;
	latch->pin = 1;

	if( loadit )
	  if( bt->err = bt_readpage (bt->mgr, page, page_no) )
		return bt->err;
	  else
		bt->reads++;

	return bt->err = 0;
}

//	set CLOCK bit in latch
//	decrement pin count

void bt_unpinlatch (BtLatchSet *latch)
{
#ifdef unix
	if( ~latch->pin & CLOCK_bit )
		__sync_fetch_and_or(&latch->pin, CLOCK_bit);
	__sync_fetch_and_add(&latch->pin, -1);
#else
	if( ~latch->pin & CLOCK_bit )
		_InterlockedOr16 (&latch->pin, CLOCK_bit);
	_InterlockedDecrement16 (&latch->pin);
#endif
}

//  return the btree cached page address

BtPage bt_mappage (BtDb *bt, BtLatchSet *latch)
{
BtPage page = (BtPage)(((uid)latch->slot << bt->mgr->page_bits) + bt->mgr->pagepool);

	return page;
}

//	find existing latchset or inspire new one
//	return with latchset pinned

BtLatchSet *bt_pinlatch (BtDb *bt, uid page_no, uint loadit)
{
uint hashidx = page_no % bt->mgr->latchhash;
BtLatchSet *latch;
uint slot, idx;
uint lvl, cnt;
off64_t off;
uint amt[1];
BtPage page;

  //  try to find our entry

  bt_spinwritelock(bt->mgr->hashtable[hashidx].latch);

  if( slot = bt->mgr->hashtable[hashidx].slot ) do
  {
	latch = bt->mgr->latchsets + slot;
	if( page_no == latch->page_no )
		break;
  } while( slot = latch->next );

  //  found our entry
  //  increment clock

  if( slot ) {
	latch = bt->mgr->latchsets + slot;
#ifdef unix
	__sync_fetch_and_add(&latch->pin, 1);
#else
	_InterlockedIncrement16 (&latch->pin);
#endif
	bt_spinreleasewrite(bt->mgr->hashtable[hashidx].latch);
	return latch;
  }

	//  see if there are any unused pool entries
#ifdef unix
	slot = __sync_fetch_and_add (&bt->mgr->latchdeployed, 1) + 1;
#else
	slot = _InterlockedIncrement (&bt->mgr->latchdeployed);
#endif

	if( slot < bt->mgr->latchtotal ) {
		latch = bt->mgr->latchsets + slot;
		if( bt_latchlink (bt, hashidx, slot, page_no, loadit) )
			return NULL;
		bt_spinreleasewrite (bt->mgr->hashtable[hashidx].latch);
		return latch;
	}

#ifdef unix
	__sync_fetch_and_add (&bt->mgr->latchdeployed, -1);
#else
	_InterlockedDecrement (&bt->mgr->latchdeployed);
#endif
  //  find and reuse previous entry on victim

  while( 1 ) {
#ifdef unix
	slot = __sync_fetch_and_add(&bt->mgr->latchvictim, 1);
#else
	slot = _InterlockedIncrement (&bt->mgr->latchvictim) - 1;
#endif
	// try to get write lock on hash chain
	//	skip entry if not obtained
	//	or has outstanding pins

	slot %= bt->mgr->latchtotal;

	if( !slot )
		continue;

	latch = bt->mgr->latchsets + slot;
	idx = latch->page_no % bt->mgr->latchhash;

	//	see we are on same chain as hashidx

	if( idx == hashidx )
		continue;

	if( !bt_spinwritetry (bt->mgr->hashtable[idx].latch) )
		continue;

	//  skip this slot if it is pinned
	//	or the CLOCK bit is set

	if( latch->pin ) {
	  if( latch->pin & CLOCK_bit ) {
#ifdef unix
		__sync_fetch_and_and(&latch->pin, ~CLOCK_bit);
#else
		_InterlockedAnd16 (&latch->pin, ~CLOCK_bit);
#endif
	  }
	  bt_spinreleasewrite (bt->mgr->hashtable[idx].latch);
	  continue;
	}

	//  update permanent page area in btree from buffer pool

	page = (BtPage)(((uid)slot << bt->mgr->page_bits) + bt->mgr->pagepool);

	if( latch->dirty )
	  if( bt->err = bt_writepage (bt->mgr, page, latch->page_no) )
		return NULL;
	  else
		latch->dirty = 0, bt->writes++;

	//  unlink our available slot from its hash chain

	if( latch->prev )
		bt->mgr->latchsets[latch->prev].next = latch->next;
	else
		bt->mgr->hashtable[idx].slot = latch->next;

	if( latch->next )
		bt->mgr->latchsets[latch->next].prev = latch->prev;

	bt_spinreleasewrite (bt->mgr->hashtable[idx].latch);

	if( bt_latchlink (bt, hashidx, slot, page_no, loadit) )
		return NULL;

	bt_spinreleasewrite (bt->mgr->hashtable[hashidx].latch);
	return latch;
  }
}

void bt_mgrclose (BtMgr *mgr)
{
BtLatchSet *latch;
uint num = 0;
BtPage page;
uint slot;

	//	flush dirty pool pages to the btree

	for( slot = 1; slot <= mgr->latchdeployed; slot++ ) {
		page = (BtPage)(((uid)slot << mgr->page_bits) + mgr->pagepool);
		latch = mgr->latchsets + slot;

		if( latch->dirty ) {
			bt_writepage(mgr, page, latch->page_no);
			latch->dirty = 0, num++;
		}
//		madvise (page, mgr->page_size, MADV_DONTNEED);
	}

	fprintf(stderr, "%d buffer pool pages flushed\n", num);

#ifdef unix
	munmap (mgr->hashtable, (uid)mgr->nlatchpage << mgr->page_bits);
	munmap (mgr->pagezero, mgr->page_size);
#else
	FlushViewOfFile(mgr->pagezero, 0);
	UnmapViewOfFile(mgr->pagezero);
	UnmapViewOfFile(mgr->hashtable);
	CloseHandle(mgr->halloc);
	CloseHandle(mgr->hpool);
#endif
#ifdef unix
	close (mgr->idx);
	free (mgr);
#else
	FlushFileBuffers(mgr->idx);
	CloseHandle(mgr->idx);
	GlobalFree (mgr);
#endif
}

//	close and release memory

void bt_close (BtDb *bt)
{
#ifdef unix
	if( bt->mem )
		free (bt->mem);
#else
	if( bt->mem)
		VirtualFree (bt->mem, 0, MEM_RELEASE);
#endif
	free (bt);
}

//  open/create new btree buffer manager

//	call with file_name, BT_openmode, bits in page size (e.g. 16),
//		size of page pool (e.g. 262144) and number of lock table entries.

BtMgr *bt_mgr (char *name, uint bits, uint nodemax, uint lockmax)
{
uint lvl, attr, last, slot, idx;
unsigned char value[BtId];
int flag, initit = 0;
BtPageZero *pagezero;
off64_t size;
uint amt[1];
BtMgr* mgr;
BtKey* key;
BtVal *val;

	// determine sanity of page size and buffer pool

	if( bits > BT_maxbits )
		bits = BT_maxbits;
	else if( bits < BT_minbits )
		bits = BT_minbits;

	if( nodemax < 16 ) {
		fprintf(stderr, "Buffer pool too small: %d\n", nodemax);
		return NULL;
	}

#ifdef unix
	mgr = calloc (1, sizeof(BtMgr));

	mgr->idx = open ((char*)name, O_RDWR | O_CREAT, 0666);

	if( mgr->idx == -1 ) {
		fprintf (stderr, "Unable to open btree file\n");
		return free(mgr), NULL;
	}
#else
	mgr = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, sizeof(BtMgr));
	attr = FILE_ATTRIBUTE_NORMAL;
	mgr->idx = CreateFile(name, GENERIC_READ| GENERIC_WRITE, FILE_SHARE_READ|FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, attr, NULL);

	if( mgr->idx == INVALID_HANDLE_VALUE )
		return GlobalFree(mgr), NULL;
#endif

#ifdef unix
	pagezero = valloc (BT_maxpage);
	*amt = 0;

	// read minimum page size to get root info
	//	to support raw disk partition files
	//	check if bits == 0 on the disk.

	if( size = lseek (mgr->idx, 0L, 2) )
		if( pread(mgr->idx, pagezero, BT_minpage, 0) == BT_minpage )
			if( pagezero->alloc->bits )
				bits = pagezero->alloc->bits;
			else
				initit = 1;
		else
			return free(mgr), free(pagezero), NULL;
	else
		initit = 1;
#else
	pagezero = VirtualAlloc(NULL, BT_maxpage, MEM_COMMIT, PAGE_READWRITE);
	size = GetFileSize(mgr->idx, amt);

	if( size || *amt ) {
		if( !ReadFile(mgr->idx, (char *)pagezero, BT_minpage, amt, NULL) )
			return bt_mgrclose (mgr), NULL;
		bits = pagezero->alloc->bits;
	} else
		initit = 1;
#endif

	mgr->page_size = 1 << bits;
	mgr->page_bits = bits;

	//  calculate number of latch hash table entries

	mgr->nlatchpage = (nodemax/16 * sizeof(BtHashEntry) + mgr->page_size - 1) / mgr->page_size;
	mgr->latchhash = ((uid)mgr->nlatchpage << mgr->page_bits) / sizeof(BtHashEntry);

	//  add on the number of pages in buffer pool
	//	along with the corresponding latch table

	mgr->nlatchpage += nodemax;		// size of the buffer pool in pages
	mgr->nlatchpage += (sizeof(BtLatchSet) * nodemax + mgr->page_size - 1)/mgr->page_size;
	mgr->latchtotal = nodemax;

	//	add on the sizeof the lock manager hash table and the lock table

	mgr->nlatchpage += (lockmax / 16 * sizeof(BtHashEntry) + mgr->page_size - 1) / mgr->page_size;

	mgr->nlatchpage += (lockmax * sizeof(BtLockSet) + mgr->page_size - 1) / mgr->page_size;

	if( !initit )
		goto mgrlatch;

	// initialize an empty b-tree with latch page, root page, page of leaves
	// and page(s) of latches and page pool cache

	memset (pagezero, 0, 1 << bits);
	pagezero->alloc->bits = mgr->page_bits;
	bt_putid(pagezero->alloc->right, MIN_lvl+1);

	//  initialize left-most LEAF page in
	//	alloc->left.

	bt_putid (pagezero->alloc->left, LEAF_page);

	if( bt_writepage (mgr, pagezero->alloc, 0) ) {
		fprintf (stderr, "Unable to create btree page zero\n");
		return bt_mgrclose (mgr), NULL;
	}

	memset (pagezero, 0, 1 << bits);
	pagezero->alloc->bits = mgr->page_bits;

	for( lvl=MIN_lvl; lvl--; ) {
		slotptr(pagezero->alloc, 1)->off = mgr->page_size - 3 - (lvl ? BtId + sizeof(BtVal): sizeof(BtVal));
		key = keyptr(pagezero->alloc, 1);
		key->len = 2;		// create stopper key
		key->key[0] = 0xff;
		key->key[1] = 0xff;

		bt_putid(value, MIN_lvl - lvl + 1);
		val = valptr(pagezero->alloc, 1);
		val->len = lvl ? BtId : 0;
		memcpy (val->value, value, val->len);

		pagezero->alloc->min = slotptr(pagezero->alloc, 1)->off;
		pagezero->alloc->lvl = lvl;
		pagezero->alloc->cnt = 1;
		pagezero->alloc->act = 1;

		if( bt_writepage (mgr, pagezero->alloc, MIN_lvl - lvl) ) {
			fprintf (stderr, "Unable to create btree page zero\n");
			return bt_mgrclose (mgr), NULL;
		}
	}

mgrlatch:
#ifdef unix
	free (pagezero);
#else
	VirtualFree (pagezero, 0, MEM_RELEASE);
#endif
#ifdef unix
	// mlock the pagezero page

	flag = PROT_READ | PROT_WRITE;
	mgr->pagezero = mmap (0, mgr->page_size, flag, MAP_SHARED, mgr->idx, ALLOC_page << mgr->page_bits);
	if( mgr->pagezero == MAP_FAILED ) {
		fprintf (stderr, "Unable to mmap btree page zero, error = %d\n", errno);
		return bt_mgrclose (mgr), NULL;
	}
	mlock (mgr->pagezero, mgr->page_size);

	mgr->hashtable = (void *)mmap (0, (uid)mgr->nlatchpage << mgr->page_bits, flag, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
	if( mgr->hashtable == MAP_FAILED ) {
		fprintf (stderr, "Unable to mmap anonymous buffer pool pages, error = %d\n", errno);
		return bt_mgrclose (mgr), NULL;
	}
#else
	flag = PAGE_READWRITE;
	mgr->halloc = CreateFileMapping(mgr->idx, NULL, flag, 0, mgr->page_size, NULL);
	if( !mgr->halloc ) {
		fprintf (stderr, "Unable to create page zero memory mapping, error = %d\n", GetLastError());
		return bt_mgrclose (mgr), NULL;
	}

	flag = FILE_MAP_WRITE;
	mgr->pagezero = MapViewOfFile(mgr->halloc, flag, 0, 0, mgr->page_size);
	if( !mgr->pagezero ) {
		fprintf (stderr, "Unable to map page zero, error = %d\n", GetLastError());
		return bt_mgrclose (mgr), NULL;
	}

	flag = PAGE_READWRITE;
	size = (uid)mgr->nlatchpage << mgr->page_bits;
	mgr->hpool = CreateFileMapping(INVALID_HANDLE_VALUE, NULL, flag, size >> 32, size, NULL);
	if( !mgr->hpool ) {
		fprintf (stderr, "Unable to create buffer pool memory mapping, error = %d\n", GetLastError());
		return bt_mgrclose (mgr), NULL;
	}

	flag = FILE_MAP_WRITE;
	mgr->hashtable = MapViewOfFile(mgr->pool, flag, 0, 0, size);
	if( !mgr->hashtable ) {
		fprintf (stderr, "Unable to map buffer pool, error = %d\n", GetLastError());
		return bt_mgrclose (mgr), NULL;
	}
#endif

	size = (mgr->latchhash * sizeof(BtHashEntry) + mgr->page_size - 1) / mgr->page_size;
	mgr->latchsets = (BtLatchSet *)((unsigned char *)mgr->hashtable + size * mgr->page_size);
	size = (sizeof(BtLatchSet) * nodemax + mgr->page_size - 1)/mgr->page_size;

	mgr->pagepool = (unsigned char *)mgr->hashtable + (size << mgr->page_bits);
	mgr->hashlock = (BtHashEntry *)(mgr->pagepool + ((uid)nodemax << mgr->page_bits));
	mgr->locktable = (BtLockSet *)((unsigned char *)mgr->hashtable + ((uid)mgr->nlatchpage << mgr->page_bits) - lockmax * sizeof(BtLockSet));

	mgr->lockfree = lockmax - 1;
	mgr->lockhash = ((unsigned char *)mgr->locktable - (unsigned char *)mgr->hashlock) / sizeof(BtHashEntry);

	for( idx = 1; idx < lockmax; idx++ )
		mgr->locktable[idx].next = idx - 1;

	return mgr;
}

//	open BTree access method
//	based on buffer manager

BtDb *bt_open (BtMgr *mgr)
{
BtDb *bt = malloc (sizeof(*bt));

	memset (bt, 0, sizeof(*bt));
	bt->mgr = mgr;
#ifdef unix
	bt->mem = valloc (2 *mgr->page_size);
#else
	bt->mem = VirtualAlloc(NULL, 2 * mgr->page_size, MEM_COMMIT, PAGE_READWRITE);
#endif
	bt->frame = (BtPage)bt->mem;
	bt->cursor = (BtPage)(bt->mem + 1 * mgr->page_size);
	return bt;
}

//  compare two keys, returning > 0, = 0, or < 0
//  as the comparison value

int keycmp (BtKey* key1, unsigned char *key2, uint len2)
{
uint len1 = key1->len;
int ans;

	if( ans = memcmp (key1->key, key2, len1 > len2 ? len2 : len1) )
		return ans;

	if( len1 > len2 )
		return 1;
	if( len1 < len2 )
		return -1;

	return 0;
}

// place write, read, or parent lock on requested page_no.

void bt_lockpage(BtLock mode, BtLatchSet *set)
{
	switch( mode ) {
	case BtLockRead:
		ReadLock (set->readwr);
		break;
	case BtLockWrite:
		WriteLock (set->readwr);
		break;
	case BtLockAccess:
		ReadLock (set->access);
		break;
	case BtLockDelete:
		WriteLock (set->access);
		break;
	case BtLockParent:
		WriteLock (set->parent);
		break;
	}
}

// remove write, read, or parent lock on requested page

void bt_unlockpage(BtLock mode, BtLatchSet *set)
{
	switch( mode ) {
	case BtLockRead:
		ReadRelease (set->readwr);
		break;
	case BtLockWrite:
		WriteRelease (set->readwr);
		break;
	case BtLockAccess:
		ReadRelease (set->access);
		break;
	case BtLockDelete:
		WriteRelease (set->access);
		break;
	case BtLockParent:
		WriteRelease (set->parent);
		break;
	}
}

//	allocate a new page
//	return with page latched.

int bt_newpage(BtDb *bt, BtPageSet *set, BtPage contents)
{
int blk;

	//	lock allocation page

	bt_spinwritelock(bt->mgr->alloclatch);

	// use empty chain first
	// else allocate empty page

	if( set->page_no = bt_getid(bt->mgr->pagezero->chain) ) {
		if( set->latch = bt_pinlatch (bt, set->page_no, 1) )
			set->page = bt_mappage (bt, set->latch);
		else
			return bt->err = BTERR_struct, -1;

		bt_putid(bt->mgr->pagezero->chain, bt_getid(set->page->right));
		bt_spinreleasewrite(bt->mgr->alloclatch);

		memcpy (set->page, contents, bt->mgr->page_size);
		set->latch->dirty = 1;
		return 0;
	}

	set->page_no = bt_getid(bt->mgr->pagezero->alloc->right);
	bt_putid(bt->mgr->pagezero->alloc->right, set->page_no+1);

	// unlock allocation latch

	bt_spinreleasewrite(bt->mgr->alloclatch);

	//	don't load cache from btree page

	if( set->latch = bt_pinlatch (bt, set->page_no, 0) )
		set->page = bt_mappage (bt, set->latch);
	else
		return bt->err = BTERR_struct;

	memcpy (set->page, contents, bt->mgr->page_size);
	set->latch->dirty = 1;
	return 0;
}

//  find slot in page for given key at a given level

int bt_findslot (BtPageSet *set, unsigned char *key, uint len)
{
uint diff, higher = set->page->cnt, low = 1, slot;
uint good = 0;

	//	  make stopper key an infinite fence value

	if( bt_getid (set->page->right) )
		higher++;
	else
		good++;

	//	low is the lowest candidate.
	//  loop ends when they meet

	//  higher is already
	//	tested as .ge. the passed key.

	while( diff = higher - low ) {
		slot = low + ( diff >> 1 );
		if( keycmp (keyptr(set->page, slot), key, len) < 0 )
			low = slot + 1;
		else
			higher = slot, good++;
	}

	//	return zero if key is on right link page

	return good ? higher : 0;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

int bt_loadpage (BtDb *bt, BtPageSet *set, unsigned char *key, uint len, uint lvl, BtLock lock)
{
uid page_no = ROOT_page, prevpage = 0;
uint drill = 0xff, slot;
BtLatchSet *prevlatch;
uint mode, prevmode;

  //  start at root of btree and drill down

  do {
	// determine lock mode of drill level
	mode = (drill == lvl) ? lock : BtLockRead; 

	if( set->latch = bt_pinlatch (bt, page_no, 1) )
		set->page_no = page_no;
	else
		return 0;

 	// obtain access lock using lock chaining with Access mode

	if( page_no > ROOT_page )
	  bt_lockpage(BtLockAccess, set->latch);

	set->page = bt_mappage (bt, set->latch);

	//	release & unpin parent page

	if( prevpage ) {
	  bt_unlockpage(prevmode, prevlatch);
	  bt_unpinlatch (prevlatch);
	  prevpage = 0;
	}

 	// obtain read lock using lock chaining

	bt_lockpage(mode, set->latch);

	if( set->page->free )
		return bt->err = BTERR_struct, 0;

	if( page_no > ROOT_page )
	  bt_unlockpage(BtLockAccess, set->latch);

	// re-read and re-lock root after determining actual level of root

	if( set->page->lvl != drill) {
		if( set->page_no != ROOT_page )
			return bt->err = BTERR_struct, 0;
			
		drill = set->page->lvl;

		if( lock != BtLockRead && drill == lvl ) {
		  bt_unlockpage(mode, set->latch);
		  bt_unpinlatch (set->latch);
		  continue;
		}
	}

	prevpage = set->page_no;
	prevlatch = set->latch;
	prevmode = mode;

	//  find key on page at this level
	//  and descend to requested level

	if( set->page->kill )
	  goto slideright;

	if( slot = bt_findslot (set, key, len) ) {
	  if( drill == lvl )
		return slot;

	  while( slotptr(set->page, slot)->dead )
		if( slot++ < set->page->cnt )
			continue;
		else
			goto slideright;

	  page_no = bt_getid(valptr(set->page, slot)->value);
	  drill--;
	  continue;
	}

	//  or slide right into next page

slideright:
	page_no = bt_getid(set->page->right);

  } while( page_no );

  // return error on end of right chain

  bt->err = BTERR_struct;
  return 0;	// return error
}

//	return page to free list
//	page must be delete & write locked

void bt_freepage (BtDb *bt, BtPageSet *set)
{
	//	lock allocation page

	bt_spinwritelock (bt->mgr->alloclatch);

	//	store chain
	memcpy(set->page->right, bt->mgr->pagezero->chain, BtId);
	bt_putid(bt->mgr->pagezero->chain, set->page_no);
	set->latch->dirty = 1;
	set->page->free = 1;

	// unlock released page

	bt_unlockpage (BtLockDelete, set->latch);
	bt_unlockpage (BtLockWrite, set->latch);
	bt_unpinlatch (set->latch);

	// unlock allocation page

	bt_spinreleasewrite (bt->mgr->alloclatch);
}

//	a fence key was deleted from a page
//	push new fence value upwards

BTERR bt_fixfence (BtDb *bt, BtPageSet *set, uint lvl)
{
unsigned char leftkey[BT_keyarray], rightkey[BT_keyarray];
unsigned char value[BtId];
BtKey* ptr;
uint idx;

	//	remove the old fence value

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (rightkey, ptr, ptr->len + sizeof(BtKey));
	memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
	set->latch->dirty = 1;

	//  cache new fence value

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (leftkey, ptr, ptr->len + sizeof(BtKey));

	bt_lockpage (BtLockParent, set->latch);
	bt_unlockpage (BtLockWrite, set->latch);

	//	insert new (now smaller) fence key

	bt_putid (value, set->page_no);
	ptr = (BtKey*)leftkey;

	if( bt_insertkey (bt, ptr->key, ptr->len, lvl+1, value, BtId, 1) )
	  return bt->err;

	//	now delete old fence key

	ptr = (BtKey*)rightkey;

	if( bt_deletekey (bt, ptr->key, ptr->len, lvl+1) )
		return bt->err;

	bt_unlockpage (BtLockParent, set->latch);
	bt_unpinlatch(set->latch);
	return 0;
}

//	root has a single child
//	collapse a level from the tree

BTERR bt_collapseroot (BtDb *bt, BtPageSet *root)
{
BtPageSet child[1];
uint idx;

  // find the child entry and promote as new root contents

  do {
	for( idx = 0; idx++ < root->page->cnt; )
	  if( !slotptr(root->page, idx)->dead )
		break;

	child->page_no = bt_getid (valptr(root->page, idx)->value);

	if( child->latch = bt_pinlatch (bt, child->page_no, 1) )
		child->page = bt_mappage (bt, child->latch);
	else
		return bt->err;

	bt_lockpage (BtLockDelete, child->latch);
	bt_lockpage (BtLockWrite, child->latch);

	memcpy (root->page, child->page, bt->mgr->page_size);
	root->latch->dirty = 1;

	bt_freepage (bt, child);

  } while( root->page->lvl > 1 && root->page->act == 1 );

  bt_unlockpage (BtLockWrite, root->latch);
  bt_unpinlatch (root->latch);
  return 0;
}

//	maintain reverse scan pointers by
//	linking left pointer of far right node

BTERR bt_linkleft (BtDb *bt, uid left_page_no, uid right_page_no)
{
BtPageSet right2[1];

	//	keep track of rightmost leaf page

	if( !right_page_no ) {
	  bt_putid (bt->mgr->pagezero->alloc->left, left_page_no);
	  return 0;
	}

	//	link right page to left page

	if( right2->latch = bt_pinlatch (bt, right_page_no, 1) )
		right2->page = bt_mappage (bt, right2->latch);
	else
		return bt->err;

	bt_lockpage (BtLockWrite, right2->latch);

	bt_putid(right2->page->left, left_page_no);
	bt_unlockpage (BtLockWrite, right2->latch);
	bt_unpinlatch (right2->latch);
	return 0;
}

//  find and delete key on page by marking delete flag bit
//  if page becomes empty, delete it from the btree

BTERR bt_deletekey (BtDb *bt, unsigned char *key, uint len, uint lvl)
{
unsigned char lowerfence[BT_keyarray], higherfence[BT_keyarray];
uint slot, idx, found, fence;
BtPageSet set[1], right[1];
unsigned char value[BtId];
BtKey *ptr, *tst;
BtVal *val;

	if( slot = bt_loadpage (bt, set, key, len, lvl, BtLockWrite) )
		ptr = keyptr(set->page, slot);
	else
		return bt->err;

	// if librarian slot, advance to real slot

	if( slotptr(set->page, slot)->type == Librarian )
		ptr = keyptr(set->page, ++slot);

	fence = slot == set->page->cnt;

	// if key is found delete it, otherwise ignore request

	if( found = !keycmp (ptr, key, len) )
	  if( found = slotptr(set->page, slot)->dead == 0 ) {
		val = valptr(set->page,slot);
		slotptr(set->page, slot)->dead = 1;
 		set->page->garbage += ptr->len + val->len + sizeof(BtKey) + sizeof(BtVal);
 		set->page->act--;

		// collapse empty slots beneath the fence

		while( idx = set->page->cnt - 1 )
		  if( slotptr(set->page, idx)->dead ) {
			*slotptr(set->page, idx) = *slotptr(set->page, idx + 1);
			memset (slotptr(set->page, set->page->cnt--), 0, sizeof(BtSlot));
		  } else
			break;
	  }

	//	did we delete a fence key in an upper level?

	if( found && lvl && set->page->act && fence )
	  if( bt_fixfence (bt, set, lvl) )
		return bt->err;
	  else
		return bt->found = found, 0;

	//	do we need to collapse root?

	if( lvl > 1 && set->page_no == ROOT_page && set->page->act == 1 )
	  if( bt_collapseroot (bt, set) )
		return bt->err;
	  else
		return bt->found = found, 0;

	//	return if page is not empty

 	if( set->page->act ) {
		set->latch->dirty = 1;
		bt_unlockpage(BtLockWrite, set->latch);
		bt_unpinlatch (set->latch);
		return bt->found = found, 0;
	}

	//	cache copy of fence key
	//	to post in parent

	ptr = keyptr(set->page, set->page->cnt);
	memcpy (lowerfence, ptr, ptr->len + sizeof(BtKey));

	//	obtain lock on right page

	right->page_no = bt_getid(set->page->right);

	if( right->latch = bt_pinlatch (bt, right->page_no, 1) )
		right->page = bt_mappage (bt, right->latch);
	else
		return 0;

	bt_lockpage (BtLockWrite, right->latch);

	if( right->page->kill )
		return bt->err = BTERR_struct;

	// transfer left link

	memcpy (right->page->left, set->page->left, BtId);

	// pull contents of right peer into our empty page

	memcpy (set->page, right->page, bt->mgr->page_size);
	set->latch->dirty = 1;

	// update left link

	if( !lvl )
	  if( bt_linkleft (bt, set->page_no, bt_getid (set->page->right)) )
		return bt->err;

	// cache copy of key to update

	ptr = keyptr(right->page, right->page->cnt);
	memcpy (higherfence, ptr, ptr->len + sizeof(BtKey));

	// mark right page deleted and point it to left page
	//	until we can post parent updates

	bt_putid (right->page->right, set->page_no);
	right->latch->dirty = 1;
	right->page->kill = 1;

	bt_lockpage (BtLockParent, right->latch);
	bt_unlockpage (BtLockWrite, right->latch);

	bt_lockpage (BtLockParent, set->latch);
	bt_unlockpage (BtLockWrite, set->latch);

	// redirect higher key directly to our new node contents

	bt_putid (value, set->page_no);
	ptr = (BtKey*)higherfence;

	if( bt_insertkey (bt, ptr->key, ptr->len, lvl+1, value, BtId, 1) )
	  return bt->err;

	//	delete old lower key to our node

	ptr = (BtKey*)lowerfence;

	if( bt_deletekey (bt, ptr->key, ptr->len, lvl+1) )
	  return bt->err;

	//	obtain delete and write locks to right node

	bt_unlockpage (BtLockParent, right->latch);
	bt_lockpage (BtLockDelete, right->latch);
	bt_lockpage (BtLockWrite, right->latch);
	bt_freepage (bt, right);

	bt_unlockpage (BtLockParent, set->latch);
	bt_unpinlatch (set->latch);
	bt->found = found;
	return 0;
}

BtKey *bt_foundkey (BtDb *bt)
{
	return (BtKey*)(bt->key);
}

//	advance to next slot

uint bt_findnext (BtDb *bt, BtPageSet *set, uint slot)
{
BtLatchSet *prevlatch;
uid page_no;

	if( slot < set->page->cnt )
		return slot + 1;

	prevlatch = set->latch;

	if( page_no = bt_getid(set->page->right) )
	  if( set->latch = bt_pinlatch (bt, page_no, 1) )
		set->page = bt_mappage (bt, set->latch);
	  else
		return 0;
	else
	  return bt->err = BTERR_struct, 0;

 	// obtain access lock using lock chaining with Access mode

	bt_lockpage(BtLockAccess, set->latch);

	bt_unlockpage(BtLockRead, prevlatch);
	bt_unpinlatch (prevlatch);

	bt_lockpage(BtLockRead, set->latch);
	bt_unlockpage(BtLockAccess, set->latch);

	set->page_no = page_no;
	return 1;
}

//	find unique key or first duplicate key in
//	leaf level and return number of value bytes
//	or (-1) if not found.  Setup key for bt_foundkey

int bt_findkey (BtDb *bt, unsigned char *key, uint keylen, unsigned char *value, uint valmax)
{
BtPageSet set[1];
uint len, slot;
int ret = -1;
BtKey *ptr;
BtVal *val;

  if( slot = bt_loadpage (bt, set, key, keylen, 0, BtLockRead) )
   do {
	ptr = keyptr(set->page, slot);

	//	skip librarian slot place holder

	if( slotptr(set->page, slot)->type == Librarian )
		ptr = keyptr(set->page, ++slot);

	//	return actual key found

	memcpy (bt->key, ptr, ptr->len + sizeof(BtKey));
	len = ptr->len;

	if( slotptr(set->page, slot)->type == Duplicate )
		len -= BtId;

	//	not there if we reach the stopper key

	if( slot == set->page->cnt )
	  if( !bt_getid (set->page->right) )
		break;

	// if key exists, return >= 0 value bytes copied
	//	otherwise return (-1)

	if( slotptr(set->page, slot)->dead )
		continue;

	if( keylen == len )
	  if( !memcmp (ptr->key, key, len) ) {
		val = valptr (set->page,slot);
		if( valmax > val->len )
			valmax = val->len;
		memcpy (value, val->value, valmax);
		ret = valmax;
	  }

	break;

   } while( slot = bt_findnext (bt, set, slot) );

  bt_unlockpage (BtLockRead, set->latch);
  bt_unpinlatch (set->latch);
  return ret;
}

//	check page for space available,
//	clean if necessary and return
//	0 - page needs splitting
//	>0  new slot value

uint bt_cleanpage(BtDb *bt, BtPageSet *set, uint keylen, uint slot, uint vallen)
{
uint nxt = bt->mgr->page_size;
BtPage page = set->page;
uint cnt = 0, idx = 0;
uint max = page->cnt;
uint newslot = max;
BtKey *key;
BtVal *val;

	if( page->min >= (max+2) * sizeof(BtSlot) + sizeof(*page) + keylen + sizeof(BtKey) + vallen + sizeof(BtVal))
		return slot;

	//	skip cleanup and proceed to split
	//	if there's not enough garbage
	//	to bother with.

	if( page->garbage < nxt / 5 )
		return 0;

	memcpy (bt->frame, page, bt->mgr->page_size);

	// skip page info and set rest of page to zero

	memset (page+1, 0, bt->mgr->page_size - sizeof(*page));
	set->latch->dirty = 1;
	page->garbage = 0;
	page->act = 0;

	// clean up page first by
	// removing deleted keys

	while( cnt++ < max ) {
		if( cnt == slot )
			newslot = idx + 2;
		if( cnt < max || page->lvl )
		  if( slotptr(bt->frame,cnt)->dead )
			continue;

		// copy the value across

		val = valptr(bt->frame, cnt);
		nxt -= val->len + sizeof(BtVal);
		memcpy ((unsigned char *)page + nxt, val, val->len + sizeof(BtVal));

		// copy the key across

		key = keyptr(bt->frame, cnt);
		nxt -= key->len + sizeof(BtKey);
		memcpy ((unsigned char *)page + nxt, key, key->len + sizeof(BtKey));

		// make a librarian slot

		slotptr(page, ++idx)->off = nxt;
		slotptr(page, idx)->type = Librarian;
		slotptr(page, idx)->dead = 1;

		// set up the slot

		slotptr(page, ++idx)->off = nxt;
		slotptr(page, idx)->type = slotptr(bt->frame, cnt)->type;

		if( !(slotptr(page, idx)->dead = slotptr(bt->frame, cnt)->dead) )
			page->act++;
	}

	page->min = nxt;
	page->cnt = idx;

	//	see if page has enough space now, or does it need splitting?

	if( page->min >= (idx+2) * sizeof(BtSlot) + sizeof(*page) + keylen + sizeof(BtKey) + vallen + sizeof(BtVal) )
		return newslot;

	return 0;
}

// split the root and raise the height of the btree

BTERR bt_splitroot(BtDb *bt, BtPageSet *root, BtKey *leftkey, BtPageSet *right)
{
uint nxt = bt->mgr->page_size;
unsigned char value[BtId];
BtPageSet left[1];
BtKey *ptr;
BtVal *val;

	//  Obtain an empty page to use, and copy the current
	//  root contents into it, e.g. lower keys

	if( bt_newpage(bt, left, root->page) )
		return bt->err;

	bt_unpinlatch (left->latch);

	// set left from higher to lower page

	bt_putid (right->page->left, left->page_no);
	bt_unpinlatch (right->latch);

	// preserve the page info at the bottom
	// of higher keys and set rest to zero

	memset(root->page+1, 0, bt->mgr->page_size - sizeof(*root->page));

	// insert stopper key at top of newroot page
	// and increase the root height

	nxt -= BtId + sizeof(BtVal);
	bt_putid (value, right->page_no);
	val = (BtVal *)((unsigned char *)root->page + nxt);
	memcpy (val->value, value, BtId);
	val->len = BtId;

	nxt -= 2 + sizeof(BtKey);
	slotptr(root->page, 2)->off = nxt;
	ptr = (BtKey *)((unsigned char *)root->page + nxt);
	ptr->len = 2;
	ptr->key[0] = 0xff;
	ptr->key[1] = 0xff;

	// insert lower keys page fence key on newroot page as first key

	nxt -= BtId + sizeof(BtVal);
	bt_putid (value, left->page_no);
	val = (BtVal *)((unsigned char *)root->page + nxt);
	memcpy (val->value, value, BtId);
	val->len = BtId;

	nxt -= leftkey->len + sizeof(BtKey);
	slotptr(root->page, 1)->off = nxt;
	memcpy ((unsigned char *)root->page + nxt, leftkey, leftkey->len + sizeof(BtKey));
	
	bt_putid(root->page->right, 0);
	root->page->min = nxt;		// reset lowest used offset and key count
	root->page->cnt = 2;
	root->page->act = 2;
	root->page->lvl++;

	// release and unpin root pages

	bt_unlockpage(BtLockWrite, root->latch);
	bt_unpinlatch (root->latch);
	return 0;
}

//  split already locked full node
//	return unlocked.

BTERR bt_splitpage (BtDb *bt, BtPageSet *set)
{
unsigned char fencekey[BT_keyarray], rightkey[BT_keyarray];
uint cnt = 0, idx = 0, max, nxt = bt->mgr->page_size;
unsigned char value[BtId];
uint lvl = set->page->lvl;
BtPageSet right[1];
BtKey *key, *ptr;
BtVal *val, *src;
uid right2;
uint prev;

	//  split higher half of keys to bt->frame

	memset (bt->frame, 0, bt->mgr->page_size);
	max = set->page->cnt;
	cnt = max / 2;
	idx = 0;

	while( cnt++ < max ) {
		if( cnt < max || set->page->lvl )
		  if( slotptr(set->page, cnt)->dead && cnt < max )
			continue;

		src = valptr(set->page, cnt);
		nxt -= src->len + sizeof(BtVal);
		memcpy ((unsigned char *)bt->frame + nxt, src, src->len + sizeof(BtVal));

		key = keyptr(set->page, cnt);
		nxt -= key->len + sizeof(BtKey);
		ptr = (BtKey*)((unsigned char *)bt->frame + nxt);
		memcpy (ptr, key, key->len + sizeof(BtKey));

		//	add librarian slot

		slotptr(bt->frame, ++idx)->off = nxt;
		slotptr(bt->frame, idx)->type = Librarian;
		slotptr(bt->frame, idx)->dead = 1;

		//  add actual slot

		slotptr(bt->frame, ++idx)->off = nxt;
		slotptr(bt->frame, idx)->type = slotptr(set->page, cnt)->type;

		if( !(slotptr(bt->frame, idx)->dead = slotptr(set->page, cnt)->dead) )
			bt->frame->act++;
	}

	// remember existing fence key for new page to the right

	memcpy (rightkey, key, key->len + sizeof(BtKey));

	bt->frame->bits = bt->mgr->page_bits;
	bt->frame->min = nxt;
	bt->frame->cnt = idx;
	bt->frame->lvl = lvl;

	// link right node

	if( set->page_no > ROOT_page ) {
		bt_putid (bt->frame->right, bt_getid (set->page->right));
		bt_putid(bt->frame->left, set->page_no);
	}

	//	get new free page and write higher keys to it.

	if( bt_newpage(bt, right, bt->frame) )
		return bt->err;

	// link left node

	if( set->page_no > ROOT_page && !lvl )
	  if( bt_linkleft (bt, right->page_no, bt_getid (set->page->right)) )
		return bt->err;

	//	update lower keys to continue in old page

	memcpy (bt->frame, set->page, bt->mgr->page_size);
	memset (set->page+1, 0, bt->mgr->page_size - sizeof(*set->page));
	set->latch->dirty = 1;

	nxt = bt->mgr->page_size;
	set->page->garbage = 0;
	set->page->act = 0;
	max /= 2;
	cnt = 0;
	idx = 0;

	if( slotptr(bt->frame, max)->type == Librarian )
		max--;

	//  assemble page of smaller keys

	while( cnt++ < max ) {
		if( slotptr(bt->frame, cnt)->dead )
			continue;
		val = valptr(bt->frame, cnt);
		nxt -= val->len + sizeof(BtVal);
		memcpy ((unsigned char *)set->page + nxt, val, val->len + sizeof(BtVal));

		key = keyptr(bt->frame, cnt);
		nxt -= key->len + sizeof(BtKey);
		memcpy ((unsigned char *)set->page + nxt, key, key->len + sizeof(BtKey));

		//	add librarian slot

		slotptr(set->page, ++idx)->off = nxt;
		slotptr(set->page, idx)->type = Librarian;
		slotptr(set->page, idx)->dead = 1;

		//	add actual slot

		slotptr(set->page, ++idx)->off = nxt;
		slotptr(set->page, idx)->type = slotptr(bt->frame, cnt)->type;
		set->page->act++;
	}

	// remember fence key for smaller page

	memcpy(fencekey, key, key->len + sizeof(BtKey));

	bt_putid(set->page->right, right->page_no);
	set->page->min = nxt;
	set->page->cnt = idx;

	// if current page is the root page, split it

	if( set->page_no == ROOT_page )
		return bt_splitroot (bt, set, (BtKey *)fencekey, right);

	// insert new fences in their parent pages

	bt_lockpage (BtLockParent, right->latch);

	bt_lockpage (BtLockParent, set->latch);
	bt_unlockpage (BtLockWrite, set->latch);

	// insert new fence for reformulated left block of smaller keys

	ptr = (BtKey*)fencekey;
	bt_putid (value, set->page_no);

	if( bt_insertkey (bt, ptr->key, ptr->len, lvl+1, value, BtId, 1) )
		return bt->err;

	// switch fence for right block of larger keys to new right page

	ptr = (BtKey*)rightkey;
	bt_putid (value, right->page_no);

	if( bt_insertkey (bt, ptr->key, ptr->len, lvl+1, value, BtId, 1) )
		return bt->err;

	bt_unlockpage (BtLockParent, set->latch);
	bt_unpinlatch (set->latch);

	bt_unlockpage (BtLockParent, right->latch);
	bt_unpinlatch (right->latch);
	return 0;
}

//	install new key and value onto page
//	page must already be checked for
//	adequate space

BTERR bt_insertslot (BtDb *bt, BtPageSet *set, uint slot, unsigned char *key,uint keylen, unsigned char *value, uint vallen, uint type)
{
uint idx, librarian;
BtSlot *node;
BtKey *ptr;
BtVal *val;

	//	if found slot > desired slot and previous slot
	//	is a librarian slot, use it

	if( slot > 1 )
	  if( slotptr(set->page, slot-1)->type == Librarian )
		slot--;

	// copy value onto page

	set->page->min -= vallen + sizeof(BtVal);
	val = (BtVal*)((unsigned char *)set->page + set->page->min);
	memcpy (val->value, value, vallen);
	val->len = vallen;

	// copy key onto page

	set->page->min -= keylen + sizeof(BtKey);
	ptr = (BtKey*)((unsigned char *)set->page + set->page->min);
	memcpy (ptr->key, key, keylen);
	ptr->len = keylen;
	
	//	find first empty slot

	for( idx = slot; idx < set->page->cnt; idx++ )
	  if( slotptr(set->page, idx)->dead )
		break;

	// now insert key into array before slot

	if( idx == set->page->cnt )
		idx += 2, set->page->cnt += 2, librarian = 2;
	else
		librarian = 1;

	set->latch->dirty = 1;
	set->page->act++;

	while( idx > slot + librarian - 1 )
		*slotptr(set->page, idx) = *slotptr(set->page, idx - librarian), idx--;

	//	add librarian slot

	if( librarian > 1 ) {
		node = slotptr(set->page, slot++);
		node->off = set->page->min;
		node->type = Librarian;
		node->dead = 1;
	}

	//	fill in new slot

	node = slotptr(set->page, slot);
	node->off = set->page->min;
	node->type = type;
	node->dead = 0;

	bt_unlockpage (BtLockWrite, set->latch);
	bt_unpinlatch (set->latch);
	return 0;
}

//  Insert new key into the btree at given level.
//	either add a new key or update/add an existing one

BTERR bt_insertkey (BtDb *bt, unsigned char *key, uint keylen, uint lvl, void *value, uint vallen, int unique)
{
unsigned char newkey[BT_keyarray];
uint slot, idx, len;
BtPageSet set[1];
BtKey *ptr, *ins;
uid sequence;
BtVal *val;
uint type;

  // set up the key we're working on

  ins = (BtKey*)newkey;
  memcpy (ins->key, key, keylen);
  ins->len = keylen;

  // is this a non-unique index value?

  if( unique )
	type = Unique;
  else {
	type = Duplicate;
	sequence = bt_newdup (bt);
	bt_putid (ins->key + ins->len + sizeof(BtKey), sequence);
	ins->len += BtId;
  }

  while( 1 ) { // find the page and slot for the current key
	if( slot = bt_loadpage (bt, set, ins->key, ins->len, lvl, BtLockWrite) )
		ptr = keyptr(set->page, slot);
	else {
		if( !bt->err )
			bt->err = BTERR_ovflw;
		return bt->err;
	}

	// if librarian slot == found slot, advance to real slot

	if( slotptr(set->page, slot)->type == Librarian )
	  if( !keycmp (ptr, key, keylen) )
		ptr = keyptr(set->page, ++slot);

	len = ptr->len;

	if( slotptr(set->page, slot)->type == Duplicate )
		len -= BtId;

	//  if inserting a duplicate key or unique key
	//	check for adequate space on the page
	//	and insert the new key before slot.

	if( unique && (len != ins->len || memcmp (ptr->key, ins->key, ins->len)) || !unique ) {
	  if( !(slot = bt_cleanpage (bt, set, ins->len, slot, vallen)) )
		if( bt_splitpage (bt, set) )
		  return bt->err;
		else
		  continue;

	  return bt_insertslot (bt, set, slot, ins->key, ins->len, value, vallen, type);
	}

	// if key already exists, update value and return

	val = valptr(set->page, slot);

	if( val->len >= vallen ) {
		if( slotptr(set->page, slot)->dead )
			set->page->act++;
		set->page->garbage += val->len - vallen;
		set->latch->dirty = 1;
		slotptr(set->page, slot)->dead = 0;
		val->len = vallen;
		memcpy (val->value, value, vallen);
		bt_unlockpage(BtLockWrite, set->latch);
		bt_unpinlatch (set->latch);
		return 0;
	}

	//	new update value doesn't fit in existing value area

	if( !slotptr(set->page, slot)->dead )
		set->page->garbage += val->len + ptr->len + sizeof(BtKey) + sizeof(BtVal);
	else {
		slotptr(set->page, slot)->dead = 0;
		set->page->act++;
	}

	if( !(slot = bt_cleanpage (bt, set, keylen, slot, vallen)) )
	  if( bt_splitpage (bt, set) )
		return bt->err;
	  else
		continue;

	set->page->min -= vallen + sizeof(BtVal);
	val = (BtVal*)((unsigned char *)set->page + set->page->min);
	memcpy (val->value, value, vallen);
	val->len = vallen;

	set->latch->dirty = 1;
	set->page->min -= keylen + sizeof(BtKey);
	ptr = (BtKey*)((unsigned char *)set->page + set->page->min);
	memcpy (ptr->key, key, keylen);
	ptr->len = keylen;
	
	slotptr(set->page, slot)->off = set->page->min;
	bt_unlockpage(BtLockWrite, set->latch);
	bt_unpinlatch (set->latch);
	return 0;
  }
  return 0;
}

//	compute hash of string

uint bt_hashkey (unsigned char *key, unsigned int len)
{
uint hash = 0;

	while( len >= sizeof(uint) )
		hash *= 11, hash += *(uint *)key, len -= sizeof(uint), key += sizeof(uint);

	while( len )
		hash *= 11, hash += *key++ * len--;

	return hash;
}

//	add a new lock table entry

uint bt_newlock (BtDb *bt, BtKey *key, uint hashidx)
{
BtLockSet *lock = bt->mgr->locktable;
uint slot, prev;

	//	obtain lock manager global lock

	bt_spinwritelock (bt->mgr->locklatch);

	//	return NULL if table is full

	if( !(slot = bt->mgr->lockfree) ) {
		bt_spinreleasewrite (bt->mgr->locklatch);
		return 0;
	}

	//  maintain free chain

	bt->mgr->lockfree = lock[slot].next;
	bt_spinreleasewrite (bt->mgr->locklatch);
	
	if( prev = bt->mgr->hashlock[hashidx].slot )
		lock[prev].prev = slot;

	bt->mgr->hashlock[hashidx].slot = slot;
	lock[slot].hashidx = hashidx;
	lock[slot].next = prev;
	lock[slot].prev = 0;

	//	save the key being locked

	memcpy (lock[slot].key, key, key->len + sizeof(BtKey));
	return slot;
}

//	add key to the lock table
//	block until available.

uint bt_setlock(BtDb *bt, BtKey *key)
{
uint hashidx = bt_hashkey(key->key, key->len) % bt->mgr->lockhash;
BtLockSet *lock = NULL;
BtKey *key2;
uint slot;

  //  find existing lock entry
  //  or recover from full table

  while( lock == NULL ) {
	//	obtain lock on hash slot

	bt_spinwritelock (bt->mgr->hashlock[hashidx].latch);

	if( slot = bt->mgr->hashlock[hashidx].slot )
	  do {
		lock = bt->mgr->locktable + slot;
		key2 = (BtKey *)lock->key;

		if( !keycmp (key, key2->key, key2->len) )
			break;
	  } while( slot = lock->next );

	if( slot )
		break;

	if( slot = bt_newlock (bt, key, hashidx) )
		break;

	bt_spinreleasewrite (bt->mgr->hashlock[hashidx].latch);
#ifdef unix
	sched_yield();
#else
	SwitchToThread ();
#endif
  }

  lock = bt->mgr->locktable + slot;
  lock->pin++;

  bt_spinreleasewrite (bt->mgr->hashlock[hashidx].latch);
  WriteLock (lock->readwr);
  return slot;
}

void bt_lockclr (BtDb *bt, uint slot)
{
BtLockSet *lock = bt->mgr->locktable + slot;
uint hashidx = lock->hashidx;
uint next, prev;
	
	bt_spinwritelock (bt->mgr->hashlock[hashidx].latch);
	WriteRelease (lock->readwr);

	// if entry is no longer in use,
	//	return it to the free chain.

	if( !--lock->pin ) {
		if( next = lock->next )
			bt->mgr->locktable[next].prev = lock->prev;

		if( prev = lock->prev )
			bt->mgr->locktable[prev].next = lock->next;
		else
			bt->mgr->hashlock[lock->hashidx].slot = next;

		bt_spinwritelock (bt->mgr->locklatch);
		lock->next = bt->mgr->lockfree;
		bt->mgr->lockfree = slot;
  		bt_spinreleasewrite (bt->mgr->locklatch);
	}

	bt_spinreleasewrite (bt->mgr->hashlock[hashidx].latch);
}

//	atomic insert of a batch of keys.
//	return -1 if BTERR is set
//	otherwise return slot number
//	causing the key constraint violation
//	or zero on successful completion.

int bt_atomicinsert (BtDb *bt, BtPage source)
{
uint locks[MAX_atomic];
BtKey *key, *key2;
int result = 0;
BtSlot temp[1];
uint slot, idx;
BtVal *val;
int type;

	// first sort the list of keys into order to
	//	prevent deadlocks between threads.

	for( slot = 1; slot++ < source->cnt; ) {
	  *temp = *slotptr(source,slot);
	  key = keyptr (source,slot);
	  for( idx = slot; --idx; ) {
	    key2 = keyptr (source,idx);
		if( keycmp (key, key2->key, key2->len) < 0 ) {
		  *slotptr(source,idx+1) = *slotptr(source,idx);
		  *slotptr(source,idx) = *temp;
		} else
		  break;
	  }
	}

	// take each unique-type key and add it to the lock table

	for( slot = 0; slot++ < source->cnt; )
	  if( slotptr(source, slot)->type == Unique )
		locks[slot] = bt_setlock (bt, keyptr(source,slot));

	// Lookup each unique key and determine constraint violations

	for( slot = 0; slot++ < source->cnt; )
	  if( slotptr(source, slot)->type == Unique ) {
		key = keyptr(source, slot);
		if( bt_findkey (bt, key->key, key->len, NULL, 0) < 0 )
		  continue;
		result = slot;
		break;
	  }

	//	add each key to the btree

	if( !result )
	 for( slot = 0; slot++ < source->cnt; ) {
	  key = keyptr(source,slot);
	  val = valptr(source,slot);
	  type = slotptr(source,slot)->type;
	  if( bt_insertkey (bt, key->key, key->len, 0, val->value, val->len, type == Unique) )
		return -1;
	 }

	// remove each unique-type key from the lock table

	for( slot = 0; slot++ < source->cnt; )
	  if( slotptr(source, slot)->type == Unique )
		bt_lockclr (bt, locks[slot]);

	return result;
}

//	set cursor to highest slot on highest page

uint bt_lastkey (BtDb *bt)
{
uid page_no = bt_getid (bt->mgr->pagezero->alloc->left);
BtPageSet set[1];
uint slot;

	if( set->latch = bt_pinlatch (bt, page_no, 1) )
		set->page = bt_mappage (bt, set->latch);
	else
		return 0;

    bt_lockpage(BtLockRead, set->latch);

	memcpy (bt->cursor, set->page, bt->mgr->page_size);
	slot = set->page->cnt;

    bt_unlockpage(BtLockRead, set->latch);
	bt_unpinlatch (set->latch);
	return slot;
}

//	return previous slot on cursor page

uint bt_prevkey (BtDb *bt, uint slot)
{
BtPageSet set[1];
uid left;

	if( --slot )
		return slot;

	if( left = bt_getid(bt->cursor->left) )
		bt->cursor_page = left;
	else
		return 0;

	if( set->latch = bt_pinlatch (bt, left, 1) )
		set->page = bt_mappage (bt, set->latch);
	else
		return 0;

    bt_lockpage(BtLockRead, set->latch);
	memcpy (bt->cursor, set->page, bt->mgr->page_size);
	bt_unlockpage(BtLockRead, set->latch);
	bt_unpinlatch (set->latch);
	return bt->cursor->cnt;
}

//  return next slot on cursor page
//  or slide cursor right into next page

uint bt_nextkey (BtDb *bt, uint slot)
{
BtPageSet set[1];
uid right;

  do {
	right = bt_getid(bt->cursor->right);

	while( slot++ < bt->cursor->cnt )
	  if( slotptr(bt->cursor,slot)->dead )
		continue;
	  else if( right || (slot < bt->cursor->cnt) ) // skip infinite stopper
		return slot;
	  else
		break;

	if( !right )
		break;

	bt->cursor_page = right;

	if( set->latch = bt_pinlatch (bt, right, 1) )
		set->page = bt_mappage (bt, set->latch);
	else
		return 0;

    bt_lockpage(BtLockRead, set->latch);

	memcpy (bt->cursor, set->page, bt->mgr->page_size);

	bt_unlockpage(BtLockRead, set->latch);
	bt_unpinlatch (set->latch);
	slot = 0;

  } while( 1 );

  return bt->err = 0;
}

//  cache page of keys into cursor and return starting slot for given key

uint bt_startkey (BtDb *bt, unsigned char *key, uint len)
{
BtPageSet set[1];
uint slot;

	// cache page for retrieval

	if( slot = bt_loadpage (bt, set, key, len, 0, BtLockRead) )
	  memcpy (bt->cursor, set->page, bt->mgr->page_size);
	else
	  return 0;

	bt->cursor_page = set->page_no;

	bt_unlockpage(BtLockRead, set->latch);
	bt_unpinlatch (set->latch);
	return slot;
}

BtKey *bt_key(BtDb *bt, uint slot)
{
	return keyptr(bt->cursor, slot);
}

BtVal *bt_val(BtDb *bt, uint slot)
{
	return valptr(bt->cursor,slot);
}

#ifdef STANDALONE

#ifndef unix
double getCpuTime(int type)
{
FILETIME crtime[1];
FILETIME xittime[1];
FILETIME systime[1];
FILETIME usrtime[1];
SYSTEMTIME timeconv[1];
double ans = 0;

	memset (timeconv, 0, sizeof(SYSTEMTIME));

	switch( type ) {
	case 0:
		GetSystemTimeAsFileTime (xittime);
		FileTimeToSystemTime (xittime, timeconv);
		ans = (double)timeconv->wDayOfWeek * 3600 * 24;
		break;
	case 1:
		GetProcessTimes (GetCurrentProcess(), crtime, xittime, systime, usrtime);
		FileTimeToSystemTime (usrtime, timeconv);
		break;
	case 2:
		GetProcessTimes (GetCurrentProcess(), crtime, xittime, systime, usrtime);
		FileTimeToSystemTime (systime, timeconv);
		break;
	}

	ans += (double)timeconv->wHour * 3600;
	ans += (double)timeconv->wMinute * 60;
	ans += (double)timeconv->wSecond;
	ans += (double)timeconv->wMilliseconds / 1000;
	return ans;
}
#else
#include <time.h>
#include <sys/resource.h>

double getCpuTime(int type)
{
struct rusage used[1];
struct timeval tv[1];

	switch( type ) {
	case 0:
		gettimeofday(tv, NULL);
		return (double)tv->tv_sec + (double)tv->tv_usec / 1000000;

	case 1:
		getrusage(RUSAGE_SELF, used);
		return (double)used->ru_utime.tv_sec + (double)used->ru_utime.tv_usec / 1000000;

	case 2:
		getrusage(RUSAGE_SELF, used);
		return (double)used->ru_stime.tv_sec + (double)used->ru_stime.tv_usec / 1000000;
	}

	return 0;
}
#endif

void bt_poolaudit (BtMgr *mgr)
{
BtLatchSet *latch;
uint slot = 0;

	while( slot++ < mgr->latchdeployed ) {
		latch = mgr->latchsets + slot;

		if( *latch->readwr->rin & MASK )
			fprintf(stderr, "latchset %d rwlocked for page %.8x\n", slot, latch->page_no);
		memset ((ushort *)latch->readwr, 0, sizeof(RWLock));

		if( *latch->access->rin & MASK )
			fprintf(stderr, "latchset %d accesslocked for page %.8x\n", slot, latch->page_no);
		memset ((ushort *)latch->access, 0, sizeof(RWLock));

		if( *latch->parent->rin & MASK )
			fprintf(stderr, "latchset %d parentlocked for page %.8x\n", slot, latch->page_no);
		memset ((ushort *)latch->parent, 0, sizeof(RWLock));

		if( latch->pin & ~CLOCK_bit ) {
			fprintf(stderr, "latchset %d pinned for page %.8x\n", slot, latch->page_no);
			latch->pin = 0;
		}
	}
}

uint bt_latchaudit (BtDb *bt)
{
ushort idx, hashidx;
uid next, page_no;
BtLatchSet *latch;
uint cnt = 0;
BtKey *ptr;

	if( *(ushort *)(bt->mgr->alloclatch) )
		fprintf(stderr, "Alloc page locked\n");
	*(uint *)(bt->mgr->alloclatch) = 0;

	for( idx = 1; idx <= bt->mgr->latchdeployed; idx++ ) {
		latch = bt->mgr->latchsets + idx;
		if( *latch->readwr->rin & MASK )
			fprintf(stderr, "latchset %d rwlocked for page %.8x\n", idx, latch->page_no);
		memset ((ushort *)latch->readwr, 0, sizeof(RWLock));

		if( *latch->access->rin & MASK )
			fprintf(stderr, "latchset %d accesslocked for page %.8x\n", idx, latch->page_no);
		memset ((ushort *)latch->access, 0, sizeof(RWLock));

		if( *latch->parent->rin & MASK )
			fprintf(stderr, "latchset %d parentlocked for page %.8x\n", idx, latch->page_no);
		memset ((ushort *)latch->parent, 0, sizeof(RWLock));

		if( latch->pin ) {
			fprintf(stderr, "latchset %d pinned for page %.8x\n", idx, latch->page_no);
			latch->pin = 0;
		}
	}

	for( hashidx = 0; hashidx < bt->mgr->latchhash; hashidx++ ) {
	  if( *(uint *)(bt->mgr->hashtable[hashidx].latch) )
			fprintf(stderr, "hash entry %d locked\n", hashidx);

	  *(uint *)(bt->mgr->hashtable[hashidx].latch) = 0;

	  if( idx = bt->mgr->hashtable[hashidx].slot ) do {
		latch = bt->mgr->latchsets + idx;
		if( latch->pin )
			fprintf(stderr, "latchset %d pinned for page %.8x\n", idx, latch->page_no);
	  } while( idx = latch->next );
	}

	page_no = LEAF_page;

	while( page_no < bt_getid(bt->mgr->pagezero->alloc->right) ) {
	uid off = page_no << bt->mgr->page_bits;
#ifdef unix
	  pread (bt->mgr->idx, bt->frame, bt->mgr->page_size, off);
#else
	DWORD amt[1];

	  SetFilePointer (bt->mgr->idx, (long)off, (long*)(&off)+1, FILE_BEGIN);

	  if( !ReadFile(bt->mgr->idx, bt->frame, bt->mgr->page_size, amt, NULL))
		return bt->err = BTERR_map;

	  if( *amt <  bt->mgr->page_size )
		return bt->err = BTERR_map;
#endif
		if( !bt->frame->free && !bt->frame->lvl )
			cnt += bt->frame->act;
		page_no++;
	}
		
	cnt--;	// remove stopper key
	fprintf(stderr, " Total keys read %d\n", cnt);

	bt_close (bt);
	return 0;
}

typedef struct {
	char idx;
	char *type;
	char *infile;
	BtMgr *mgr;
} ThreadArg;

//  standalone program to index file of keys
//  then list them onto std-out

#ifdef unix
void *index_file (void *arg)
#else
uint __stdcall index_file (void *arg)
#endif
{
int line = 0, found = 0, cnt = 0, unique;
uid next, page_no = LEAF_page;	// start on first page of leaves
BtPage page = calloc (4096, 1);
unsigned char key[BT_maxkey];
ThreadArg *args = arg;
int ch, len = 0, slot;
BtPageSet set[1];
int atomic;
BtKey *ptr;
BtVal *val;
BtDb *bt;
FILE *in;

	bt = bt_open (args->mgr);

	unique = (args->type[1] | 0x20) != 'd';
	atomic = (args->type[1] | 0x20) == 'a';

	switch(args->type[0] | 0x20)
	{
	case 'a':
		fprintf(stderr, "started latch mgr audit\n");
		cnt = bt_latchaudit (bt);
		fprintf(stderr, "finished latch mgr audit, found %d keys\n", cnt);
		break;

	case 'p':
		fprintf(stderr, "started pennysort for %s\n", args->infile);
		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;

			  if( atomic ) {
				memset (page, 0, 4096);
				slotptr(page, 1)->off = 2048;
				slotptr(page, 1)->type = Unique;
				ptr = keyptr(page,1);
				ptr->len = 10;
				memcpy(ptr->key, key, 10);
				val = valptr(page,1);
				val->len = len - 10;
				memcpy (val->value, key + 10, len - 10);
				page->cnt = 1;
				if( slot = bt_atomicinsert (bt, page) )
				  fprintf(stderr, "Error %d Line: %d\n", slot, line), exit(0);
			  }
			  else if( bt_insertkey (bt, key, 10, 0, key + 10, len - 10, unique) )
				fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
			  len = 0;
			}
			else if( len < BT_maxkey )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys: %d reads %d writes\n", args->infile, line, bt->reads, bt->writes);
		break;

	case 'w':
		fprintf(stderr, "started indexing for %s\n", args->infile);
		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;
			  if( bt_insertkey (bt, key, len, 0, NULL, 0, unique) )
				fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
			  len = 0;
			}
			else if( len < BT_maxkey )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys: %d reads %d writes\n", args->infile, line, bt->reads, bt->writes);
		break;

	case 'd':
		fprintf(stderr, "started deleting keys for %s\n", args->infile);
		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;
			  if( bt_findkey (bt, key, len, NULL, 0) < 0 )
				fprintf(stderr, "Cannot find key for Line: %d\n", line), exit(0);
			  ptr = (BtKey*)(bt->key);
			  found++;

			  if( bt_deletekey (bt, ptr->key, ptr->len, 0) )
				fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
			  len = 0;
			}
			else if( len < BT_maxkey )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys, %d found: %d reads %d writes\n", args->infile, line, found, bt->reads, bt->writes);
		break;

	case 'f':
		fprintf(stderr, "started finding keys for %s\n", args->infile);
		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;
			  if( bt_findkey (bt, key, len, NULL, 0) == 0 )
				found++;
			  else if( bt->err )
				fprintf(stderr, "Error %d Syserr %d Line: %d\n", bt->err, errno, line), exit(0);
			  len = 0;
			}
			else if( len < BT_maxkey )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys, found %d: %d reads %d writes\n", args->infile, line, found, bt->reads, bt->writes);
		break;

	case 's':
		fprintf(stderr, "started scanning\n");
	  	do {
			if( set->latch = bt_pinlatch (bt, page_no, 1) )
				set->page = bt_mappage (bt, set->latch);
			else
				fprintf(stderr, "unable to obtain latch"), exit(1);
			bt_lockpage (BtLockRead, set->latch);
			next = bt_getid (set->page->right);

			for( slot = 0; slot++ < set->page->cnt; )
			 if( next || slot < set->page->cnt )
			  if( !slotptr(set->page, slot)->dead ) {
				ptr = keyptr(set->page, slot);
				len = ptr->len;

			    if( slotptr(set->page, slot)->type == Duplicate )
					len -= BtId;

				fwrite (ptr->key, len, 1, stdout);
				val = valptr(set->page, slot);
				fwrite (val->value, val->len, 1, stdout);
				fputc ('\n', stdout);
				cnt++;
			   }

			bt_unlockpage (BtLockRead, set->latch);
			bt_unpinlatch (set->latch);
	  	} while( page_no = next );

		fprintf(stderr, " Total keys read %d: %d reads, %d writes\n", cnt, bt->reads, bt->writes);
		break;

	case 'r':
		fprintf(stderr, "started reverse scan\n");
		if( slot = bt_lastkey (bt) )
	  	   while( slot = bt_prevkey (bt, slot) ) {
			if( slotptr(bt->cursor, slot)->dead )
			  continue;

			ptr = keyptr(bt->cursor, slot);
			len = ptr->len;

			if( slotptr(bt->cursor, slot)->type == Duplicate )
				len -= BtId;

			fwrite (ptr->key, len, 1, stdout);
			val = valptr(bt->cursor, slot);
			fwrite (val->value, val->len, 1, stdout);
			fputc ('\n', stdout);
			cnt++;
		  }

		fprintf(stderr, " Total keys read %d: %d reads, %d writes\n", cnt, bt->reads, bt->writes);
		break;

	case 'c':
#ifdef unix
		posix_fadvise( bt->mgr->idx, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
		fprintf(stderr, "started counting\n");
		page_no = LEAF_page;

		while( page_no < bt_getid(bt->mgr->pagezero->alloc->right) ) {
			if( bt_readpage (bt->mgr, bt->frame, page_no) )
				break;

			if( !bt->frame->free && !bt->frame->lvl )
				cnt += bt->frame->act;

			bt->reads++;
			page_no++;
		}
		
	  	cnt--;	// remove stopper key
		fprintf(stderr, " Total keys read %d: %d reads, %d writes\n", cnt, bt->reads, bt->writes);
		break;
	}

	bt_close (bt);
#ifdef unix
	return NULL;
#else
	return 0;
#endif
}

typedef struct timeval timer;

int main (int argc, char **argv)
{
int idx, cnt, len, slot, err;
int segsize, bits = 16;
double start, stop;
#ifdef unix
pthread_t *threads;
#else
HANDLE *threads;
#endif
ThreadArg *args;
uint poolsize = 0;
uint locksize = 0;
float elapsed;
char key[1];
BtMgr *mgr;
BtKey *ptr;
BtDb *bt;

	if( argc < 3 ) {
		fprintf (stderr, "Usage: %s idx_file Read/Write/Scan/Delete/Find/Atomic [page_bits buffer_pool_size lock_mgr_size src_file1 src_file2 ... ]\n", argv[0]);
		fprintf (stderr, "  where page_bits is the page size in bits\n");
		fprintf (stderr, "  buffer_pool_size is the number of pages in buffer pool\n");
		fprintf (stderr, "  lock_mgr_size is the maximum number of outstanding key locks\n");
		fprintf (stderr, "  src_file1 thru src_filen are files of keys separated by newline\n");
		exit(0);
	}

	start = getCpuTime(0);

	if( argc > 3 )
		bits = atoi(argv[3]);

	if( argc > 4 )
		poolsize = atoi(argv[4]);

	if( !poolsize )
		fprintf (stderr, "Warning: no mapped_pool\n");

	if( argc > 5 )
		locksize = atoi(argv[5]);

	cnt = argc - 6;
#ifdef unix
	threads = malloc (cnt * sizeof(pthread_t));
#else
	threads = GlobalAlloc (GMEM_FIXED|GMEM_ZEROINIT, cnt * sizeof(HANDLE));
#endif
	args = malloc (cnt * sizeof(ThreadArg));

	mgr = bt_mgr ((argv[1]), bits, poolsize, locksize);

	if( !mgr ) {
		fprintf(stderr, "Index Open Error %s\n", argv[1]);
		exit (1);
	}

	//	fire off threads

	for( idx = 0; idx < cnt; idx++ ) {
		args[idx].infile = argv[idx + 6];
		args[idx].type = argv[2];
		args[idx].mgr = mgr;
		args[idx].idx = idx;
#ifdef unix
		if( err = pthread_create (threads + idx, NULL, index_file, args + idx) )
			fprintf(stderr, "Error creating thread %d\n", err);
#else
		threads[idx] = (HANDLE)_beginthreadex(NULL, 65536, index_file, args + idx, 0, NULL);
#endif
	}

	// 	wait for termination

#ifdef unix
	for( idx = 0; idx < cnt; idx++ )
		pthread_join (threads[idx], NULL);
#else
	WaitForMultipleObjects (cnt, threads, TRUE, INFINITE);

	for( idx = 0; idx < cnt; idx++ )
		CloseHandle(threads[idx]);

#endif
	bt_poolaudit(mgr);
	bt_mgrclose (mgr);

	elapsed = getCpuTime(0) - start;
	fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(1);
	fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(2);
	fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
}

#endif	//STANDALONE
