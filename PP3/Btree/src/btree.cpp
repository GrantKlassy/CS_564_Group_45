/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "file.h"
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
	//Taken from spec: How to get name of index file
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str(); // name of index file

	Page* myMetaPage;
	IndexMetaInfo* myMetaInfo;

	this->bufMgr = befMgrIn;
	this->currentPageNum = 0;
	this->scanExecuting = false;
	// Should always be integer
	this->attributeType = attrType;


	// Find out if the file exists
	if (!(File::exists(indexName))) {
		this->file = new BlobFile(indexName, true);

		// Scanner to look through files
		// Directly taken from main.cpp so this should scan through it correctly
		FileScan fscan(relationName, this->bufMgr);
   	     	try {
			RecordId scanRid;
               		while(1) {
                		fscan.scanNext(scanRid);
                        	//Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record.
                        	std::string recordStr = fscan.getRecord();
                        	const char *record = recordStr.c_str();
				// Edited by mike to be int*
                        	int * key = ((int *)(record + offsetof (RECORD, i)));
                        	//std::cout << "Extracted : " << key << std::endl;

				// Added by mike
				// Call insert on each entry?
				// FIXME: Is this right
				insertEntry(key, scanRid);
       		        }
        	}
        	catch(EndOfFileException e) {
        		//std::cout << "Read all records" << std::endl;
        	}

		//TODO: Still have to update meta data

	} else {
		// open file
		// This is "“raw” file, i.e., it has no page structure on top of it"
		this->file = new BlobFile((indexName, false);
		this->headerPageNum = file->getFirstPageNo();
		this->bufMgr->readPage(this->file, this->headerPageNum, myMetaPage);
		myMetaInfo = (IndexMetaInfo*) myMetaPage;
		this->attributeByteOffset = myMetaInfo->attrByteOffset;
		this->rootPageNum = myMetaInfo->rootPageNo;
		this->attributeType = myMetaInfo->attrType;
		this->bufMgr->unPinPage( this->file, this->headerPageNum, false);
		// TODO: Do we still need to scan through everything to add
	}


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

    // mike: There are multiple times we are going to have to traverse the tree so I'm guessing
    // we'll end up making a helper function to traverse.  In this case we'd end up calling
    // that helper function with the lower value which would return the leaf node closest
    // I think how to traverse in the helper function will be more clear once we write the
    // constructor.
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
	// mike: I think this is right.  "A leaf page that has been read into the buffer
        // pool for the purpose of scanning, should not be unpinned from buffer pool unless
	// all records from it are read or the scan has reached its end".  That makes it sound
	// like in scanNext we bring it into the buffer pool and pin it.
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
