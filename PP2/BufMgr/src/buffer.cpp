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
#include "file.h"
#include "file_iterator.h"

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
 * Wipes frames and writes to disk if dirty
 **/
BufMgr::~BufMgr() {

	for (FrameId i = 0; i < numBufs; i++) {
		if (bufDescTable[i].valid == 1 && bufDescTable[i].dirty == 1) {
			flushFile(bufDescTable[i].file);
		}
	}
	delete [] bufDescTable;
	delete [] bufPool;
	delete hashTable;
}

/** TODO: 
 * Just use a global var to track where we are in bufpool? This method would just inc that index but make sure to loop back around to start when it hits end
**/

void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs;
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

    int c = 2*numBufs;
    while (c > 0) {
	if (!bufDescTable[clockHand].valid) {
	    bufDescTable[clockHand].valid = true;
	    frame = bufDescTable[clockHand].frameNo;
	    advanceClock();
	    return;
	}
	else if (bufDescTable[clockHand].pinCnt == 0) {
	    if (bufDescTable[clockHand].refbit == 1) {
	        bufDescTable[clockHand].refbit = 0;
	        advanceClock();
	    }
	    else if (bufDescTable[clockHand].refbit == 0) {
	    	if (bufDescTable[clockHand].dirty) {
		    bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
		    bufDescTable[clockHand].dirty = false;
	        }
	    	frame = bufDescTable[clockHand].frameNo;
	    	bufDescTable[clockHand].valid = 1;
            	advanceClock();
            	return;
	    }
	}
	else {
	    advanceClock();
	}
	c--;
    }
    // only reaches this far if gone through 2 cycles with no available frame
    throw BufferExceededException();
}

/** 
 * Described implementation pretty well in spec.  See that.
 **/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{

	// Check to see if the frame is in the buffer pool
	bool frameInPool = true;
	FrameId num;
	try {
		hashTable->lookup(file, pageNo, num);
	} catch (HashNotFoundException e) {
		frameInPool = false;
	}

	// If the frame is not in the pool...
	if (!frameInPool) {

		// Allocate a buffer to hold it
		allocBuf(num);

		// Read
		Page currPage = file->readPage(pageNo);
		bufPool[num] = currPage;

		// Insert into hash table and update desc table
		hashTable->insert(file, pageNo, num);
		bufDescTable[num].Set(file, pageNo);

		page = &bufPool[num];
		return;

	} else {
		// If the frame is in the pool, just read it from there
		BufDesc* frame = &bufDescTable[num];
		frame->refbit = true;
		frame->pinCnt = frame->pinCnt + 1;

		// Return
		page = &bufPool[num];
		return;
	}


}

/** TODO: 
 * Described implementation pretty well in spec.  See that.
 **/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{

	FrameId frameNo = -1;
	try {
		hashTable->lookup(file, pageNo, frameNo);
	}
	// Supposed to do nothing if isn't found in Hash table
	catch (HashNotFoundException e) {
		return;
	}

	// If we make it here we found the frameNo (stored in frameNo)
	// If pin count is already 0, throw exception
	if (bufDescTable[frameNo].pinCnt == 0) {
		throw PageNotPinnedException(file->filename(), pageNo, frameNo);
	} else {
		// Decrement pincount and set dirty to true if need be
		bufDescTable[frameNo].pinCnt--;
		if (dirty) {
			bufDescTable[frameNo].dirty = true;
		}
	}

}

/** TODO: 
 * Get's rid of all remnants of pages in bufPool, bufHashTable, bufDescTable that have to do with this page but doesn't delete the pages themselves.
 * Throws errors if things are pinned
 **/
void BufMgr::flushFile(File* file) 
{
	// Look through all of our bufpool
	for (uint i = 0; i < numBufs; i++) {
		// Look through all pages in our file

		if (file == bufDescTable[i].file) {
			// Throw appropriate exceptions
			if (bufDescTable[i].pinCnt > 0) {
				throw PagePinnedException(file->filename(), bufPool[i].page_number(), i);
			} else if (bufDescTable[i].valid == false) {
				throw BadBufferException(i, bufDescTable[i].dirty, false, bufDescTable[i].refbit);
			} else {
				// Write back
				if (bufDescTable[i].dirty) {
					// Shouldn't need to allocate first since we are just writing back.
					file->writePage(bufPool[i]);
					bufDescTable[i].dirty = false;
				}
				// Remove from hash table
				hashTable->remove(file, bufPool[i].page_number());
				// Remove from desc table
				bufDescTable[i].Clear();
			}
		}
	}
}

/**
 * Not sure if I understand this one correctly?
 * For when you want a completely new page? Not one that already exists but you read?
 **/
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{

	// Allocate an empty page in the file
	Page currPage = file->allocatePage();

	// Then use allocBuf() to obtain a buffer pool frame
	// allocBuff will tell us what frame number we will use
	FrameId num;
	allocBuf(num);
	bufPool[num] = currPage;

	// Return the page num and ptr
	page = &bufPool[num];
	pageNo = bufPool[num].page_number();

	// Insert into hash table
	hashTable->insert(file, pageNo, num);

	// Set up a new frame in the buffer
	bufDescTable[num].Set(file, pageNo);

}

/**
 * Get rid of page from bufDescTable, bufpool, HashTable, and finally the page itself?
 **/
void BufMgr::disposePage(File* file, const PageId PageNo)
{
		FrameId frameNum = numBufs + 1;
		try{
			hashTable->lookup(file, PageNo, frameNum);
		}
		catch(HashNotFoundException){
			// We still need to delete the page from the file here, just not the buffer
			file->deletePage(PageNo);
			return;
		}

		bufDescTable[frameNum].Clear();
		hashTable->remove(file, PageNo);
		file->deletePage(PageNo);
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
