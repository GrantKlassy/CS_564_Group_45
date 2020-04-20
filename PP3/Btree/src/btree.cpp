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
	std::string outIndexName = idxStr.str(); // name of index file

	this->bufMgr = befMgrIn;
	this->scanExecuting = false;
	// Should always be integer
	this->attributeType = attrType;
	this->nodeOccupancy = INTARRAYNONLEAFSIZE;
	this->leafOccupancy = INTARRAYLEAFSIZE;
	this->attrByteOffset = attrByteOffset;
	// At the start our root is a leaf
	this->rootLeaf = true;

	// Things we have yet to set
	// rootPageNum = -1 says we haven't started setting up stuff yet
	this->rootPageNum = -1;
	this->currentPageNum = 0;
	this->headerPageNum = 0;
	this->file = NULL;
	Page* myMetaPage;
	IndexMetaInfo* myMetaInfo;
	

	// Find out if the file exists
	if (!(File::exists(outIndexName))) {
		this->file = new BlobFile(outIndexName, true);

		// Before looking through and adding things, lets alloc metadata at page 0
		bufMgr->allocPage(this->file, 0, myMetaPage);
		myMetaInfo = (IndexMetaInfo*)(myMetaPage);
		// Since we already set headerPageNum to 0 by default, this is redundant
		// this->headerPageNum = 0;
		strcpy(myMetaInfo->relationName, relationName.c_str());
		myMetaInfo->attrType = this->attributeType;
		myMetaInfo->attrByteOffset = this->attrByteOffset;
		// We again assume insert takes care of this
		myMetaInfo->rootPageNo = -1;
		myMetaInfo->rootLeaf = true;

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
				// We assume insert entry will handle root node setting
				// Assume insert allocs additional necessary pages
				insertEntry(key, scanRid);
       		        }
        	}
        	catch(EndOfFileException e) {
			// FIXME: Right place to unpin header file?
			bufMgr->unPinPage(this->file, 0, true);
        		//std::cout << "Read all records" << std::endl;
        	}
	} else {
		// open file
		this->file = new BlobFile((outIndexName, false);
		this->headerPageNum = file->getFirstPageNo();
		this->bufMgr->readPage(this->file, this->headerPageNum, myMetaPage);
		myMetaInfo = (IndexMetaInfo*) myMetaPage;
		this->attributeByteOffset = myMetaInfo->attrByteOffset;
		this->rootPageNum = myMetaInfo->rootPageNo;
		this->attributeType = myMetaInfo->attrType;
		this->rootLeaf = myMetaInfo->rootLeaf;
		this->bufMgr->unPinPage( this->file, this->headerPageNum, false);
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
// Destructor should be done
BTreeIndex::~BTreeIndex()
{
	this->scanExecuting = false;
	this->bugMgr->flushFile(this->file);
	delete this->file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

// !!!!!!!!!!!!!!!!!!!!!!!!!!!! IMPORTANT !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// If this->rootPageNum = -1 it means we have just started setting up 
// nodes.  If this is the case, the entry we are setting needs to become the root
// and you need to update this->rootPageNum and any other relevant root info
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	// If we don't have a root yet, let's make a root, update info
	if (this->rootPageNum == -1) {

		RIDKeyPair<int> firstPairIn;
		int * firstKey;
		firstKey = (int*) (key);
		firstPairIn.set(rid, *(firstKey));
		Page * newNode;
		PageID newPageNum;
		bufMgr->allocPage(this->file, newPageNum, newNode);

		// Insert into root at index 1
		insertLeafHelper(newNode, firstPairIn, 0);

		// Set header info
		this->rootLeaf = true;
		this->rootPageNum = newPageNum;	

		// Set meta info
		Page* myMetaPage;
	        IndexMetaInfo* myMetaInfo;
		this->bufMgr->readPage(this->file, this->headerPageNum, myMetaPage);
		myMetaInfo = (IndexMetaInfo*) myMetaPage;
		myMetaInfo->rootLeaf = true;
		myMetaInfo->rootPageNum = newPageNum;

		myMetaInfo->unPinPage(this->file, newPageNum, true);
		myMetaInfo->unPinPage(this->file, myMetaPage, true);
		return;
	}
	// Else we'll call a recursive helper function on the root
		
}



void BTreeIndex::insertLeafHelper(Page * myNode, RIDKeyPair<int> insertMe, int currNumIndices) {

	LeafNodeInt * myLeaf;
	myLeaf = (LeafNodeInt *) myNode;

	// Just insert it at front if it's new
	if (currNumIndices == 0) {
		myLeaf->keyArray[0] = insertMe.key;
		myLeaf->ridArray[0] = insertMe.rid;
		return;
	}

	// Go through and find place to insert it
	// TODO: Figure out if we have to split and call it appropriately?
	int testKey;
	RecordId testRid;
	for (int i = 0; i < currNumIndices; i++) {
		testKey = myLeaf->keyArray[i];
		// Have we found the right place to insert
		if (insertMe.key < testKey) {
			// We gotta shift everything over
			// TODO: Double check when not late at night
			for (int j = currNumIndices - 1; j > i - 1; i--) {
				leafNode->keyArray[j+1] = leafNode->keyArray[j];
				leafNode->ridArray[j+1] = leafNode->ridArray[j];
			}
			leafNode->keyArray[i] = insertMe.key;
			leafNode->ridArray[i] = insertMe.rid;
			return;
		}
	}
	// If we make it all the way until the end, jsut put it at end
	leafNode->keyArray[currNumIndices-1] = insertMe.key;
	leafNode->ridArray[currNumIndices-1] = insertMe.rid;
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE)) {
	throw new BadOpcodesException;
    }
    if (lowValParm > highValParm) {
	throw new BadScanrangeException;
    }

    // check if scan already in progress and if so, end it
    if (this->scanExecuting) {
	endScan();
    }
    this->scanExecuting = true;

    bufMgr->readPage(this->file, rootPageNum, this->currentPageData);

    if (this->rootLeaf) {
	LeafNodeInt *currLeaf = reinterpret_cast<LeafNodeInt*> this->currentPageData;
	this->nextEntry = lowLeafHelper(currLeaf, lowValParm, lowOpParm);	
	
	this->currentPageData = reinterpret_cast<Page*> currLeaf;
        scanLeafHelper(highValParm, highOpParm);
    }
    else {
	// mike: I think this is right.  "A leaf page that has been read into the buffer
        // pool for the purpose of scanning, should not be unpinned from buffer pool unless
	// all records from it are read or the scan has reached its end".  That makes it sound
	// like in scanNext we bring it into the buffer pool and pin it.

	NonLeafNodeInt *currNode = reinterpret_cast<NonLeafNodeInt*> this->currentPageData;
	
	// if the next level from root is the leaf level, call with bool nextLeaf = true, otherwise false
	if (currNode->level != 1) {
	    findLeavesHelper(currNode, false, lowValParm, lowOpParm);
	}
	else {
	    findLeavesHelper(currNode, true, lowValParm, lowOpParm);
	}
	
	// we should return with currNode --> first leaf node in range, so cast to leaf struct
	LeafNodeInt *currLeaf = reinterpret_cast<LeafNodeInt*> currNode;	
	this->nextEntry = lowLeafHelper(currLeaf, lowValParm, lowOpParm);

	// recast currLeaf to Page* and store in currentPageData to save as global data
	this->currentPageData = reinterpret_cast<Page*> currLeaf;
	scanLeafHelper(highValParm, highOpParm);

	endScan();
    }
}

/**
 * Traverses the tree recursively to find the leaf node that is found to be at the beginning of the range.
 */
void BTreeIndex::findLeavesHelper(NonLeafNodeInt * currNode, bool nextLeaf, const void* lowVal, const Operator lowOp) {
    int curridx = 0;
    bool smFound = false;
    while (!smFound) {
        if (curridx >= INTARRAYNONLEAFSIZE) {
            // reset currNode as right pid
            bufMgr->readPage(this->file, currNode->pageNoArray[INTARRAYNONLEAFSIZE], currNode);
            smFound = true;
        }
        else {
            if ((lowOp == GT && currNode->keyArray[curridx] > lowVal)
                    || (lowOp == GTE && currNode->keyArray[curridx] >= lowVal)) {
                // reset currNode as left pid
                bufMgr->readPage(this->file, currNode->pageNoArray[curridx], currNode);
                smFound = true;
            }
            else {
		// if the current key is still <=/< the low val...
                curridx++;
            }
        }
    }
    if (nextLeaf) {
	return;
    }
    else if (currNode->level != 1) {
	findLeavesHelper(currNode, false, lowVal, lowOp);
    }
    else {
	// we should be at the level above the leaves
	findLeavesHelper(currNode, true, lowVal, lowOp);
    }
}

/**
 * Traverses the leaf node found to be at the beginning of the range, finds the specific key that is the
 * smallest but >/>= lowVal, and sets nextEntry to its index and returns nextEntry.
 * 
 * Throws new NoSuchKeyFoundException in the case where we couldn't find and value >/>= lowVal
 */
int BTreeIndex::lowLeafHelper(LeafNodeInt * currLeaf, const void* lowVal, const Operator lowOp) {
    int startidx = -1;
    for (int i = INTARRAYLEAFSIZE-1; i >= 0; i--) {
	if ((lowOp == GT && currLeaf->keyArray[i] > lowVal) 
		|| (lowOp == GTE && currLeaf->keyArray[i] >= lowVal)) {
	    startidx = i;
	}
    }
    if (startidx == -1) {
	throw new NoSuchKeyFoundException;
    }
    else {
	return startidx;
    } 
}

/**
 * Traverses the leaf node from nextEntry until it finds the specific key that is the
 * largest but </<= highVal, and scans each leaf that satisfies the range using scanNext.
 *
 * Throws new NoSuchKeyFoundException in the case where we couldn't find and value </<= highVal
 */
void BTreeIndex::scanLeafHelper(const void* highVal, const Operator highOp) {
    // check to make sure that at least one key is within range
    LeafNodeInt *currLeaf = reinterpret_cast<LeafNodeInt*> this->currentPageData;
    if ((highOp == LT && currLeaf->keyArray[this->nextEntry] >= highVal)
            || (highOp == LTE && currLeaf->keyArray[this->nextEntry] > highVal)) {
	throw new NoSuchKeyFoundException;
    }

    bool inRange;
    do {
	//cast everytime to ensure we use the right node if scanNext moves onto the next one
	LeafNodeInt *currLeaf = reinterpret_cast<LeafNodeInt*> this->currentPageData;
        if ((highOp == LT && currLeaf->keyArray[this->nextEntry] < highVal) 
                || (highOp == LTE && currLeaf->keyArray[this->nextEntry] <= highVal)) {
            inRange = true;
            scanNext(currLeaf->ridArray[this->nextEntry]);
        }
        else {
            inRange = false;
        }
    } while (inRange);
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
    //note to whoever does this method: make sure you increment nextEntry appropriately
    //because I am not doing that in startScan
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
