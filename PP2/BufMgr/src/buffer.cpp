/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

/**
 * Constructor class given to us.  Initialized empty frames, empty buffer pool and empty hashtable
 **/
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

// bufDescTable is just the status of what's at it's corresponding buffer frame
  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  // This is where all of our data is stored
  bufPool = new Page[bufs];

  // hashtable which, according to file, pageNo and frameNo, tells which index in bufPool corresponds to it
	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/**
 * I believe already completed? Just wipes our buffer frames
 **/
BufMgr::~BufMgr() {
	delete [] bufPool;
}

/** TODO: 
 * Just use a global var to track where we are in bufpool? This method would just inc that index but make sure to loop back around to start when it hits end
**/
void BufMgr::advanceClock()
{

}

/** TODO: 
 * Keep track of where you started.  Increment through bufDescTable using advance clock until you find first page with pinCnt = 0 or valid = 0.  
 * If valid = 0, put it there. 
 * If valid = 1 and pinCnt = 0 and refbit = 1, dec refbit. 
 * If valid = 1, pinCnt = 0, refBit = 0.  Place it here, making sure to write things back if dirty bit is 1.
 * Once you place it, make sure to advance the clock before leaving
 * If you go around twice that means all the pinCounts are used, return error
 **/
void BufMgr::allocBuf(FrameId & frame) 
{
}

/** TODO: 
 * Described implementation pretty well in spec.  See that.
 **/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}

/** TODO: 
 * Described implementation pretty well in spec.  See that.
 **/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

/** TODO: 
 * Get's rid of all remnants of pages in bufPool, bufHashTable, bufDescTable that have to do with this page but doesn't delete the pages themselves.
 * Throws errors if things are pinned
 **/
void BufMgr::flushFile(const File* file) 
{
}

/** TODO: 
 * Not sure if I understand this one correctly?
 * For when you want a completely new page? Not one that already exists but you read?
 **/
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
}

/** TODO: 
 * Get rid of page from bufDescTable, bufpool, HashTable, and finally the page itself?
 **/
void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
