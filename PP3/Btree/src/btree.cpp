/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    //check if scan already in progress and if so, end it
    //scanExecuting = true

    //if root is leaf:
	//if root is (low op) than (low val) and (high op) than (high val): 
	    //bring to buffer pool <--- HELP do i need to do more?? will root have >1 val??

    //else:
	//while NonLeafNodeInt::level != 1: (this loop gets us to level before leaf)
	    //read in page to bufmgr <--- HELP do we need to do this??
	    //pick lowest child that is still (low op) than (low val)
	    //bring said child to buffer pool
	    //move curr to child node

	//(now we're one level above child node)
	//read in page to bufmgr
        //pick lowest leaf that is still (low op) than (low val) 
        //bring said leaf to buffer pool
        //move curr to leaf node

	//do:
	    //scanNext(nextrid)
	    //if val of nextrid record is (high op) than (high val):
		//scan? does this mean also bring to buffer pool? HELP
	//while val of nextrid record is (high op) than (high val);

	//endScan
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
