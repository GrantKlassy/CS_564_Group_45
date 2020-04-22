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

		this->headerPageNum = 0;
		// Before looking through and adding things, lets alloc metadata at page 0
		bufMgr->allocPage(this->file, this->headerPageNum, myMetaPage);
		myMetaInfo = (IndexMetaInfo*)(myMetaPage);
		// Since we already set headerPageNum to 0 by default, this is redundant
		// this->headerPageNum = 0;
		strcpy(myMetaInfo->relationName, relationName.c_str());
		myMetaInfo->attrType = this->attributeType;
		myMetaInfo->attrByteOffset = this->attrByteOffset;
		// We again assume insert takes care of this
		myMetaInfo->rootPageNo = -1;
		myMetaInfo->rootLeaf = true;

		// unpin header
		this->bufMgr->unPinPage(this->file, this->headerPageNum, true);

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
		int * keyToInsert;
		keyToInsert = (int*) (key);
		RIDKeyPair<int> ridKeyCombo;
		ridKeyCombo.set(rid, *(keyToInsert));


	// If we don't have a root yet, let's make a root, update info
	if (this->rootPageNum == -1) {

		Page * newNode;
		PageID newPageNum;
		bufMgr->allocPage(this->file, newPageNum, newNode);

		// Insert into root at index 1
		insertLeafHelper((LeafNodeInt *) newNode, ridKeyCombo, 0);

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

		this->bufMgr->unPinPage(this->file, newPageNum, true);
		this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
		return;
	}
	// Else we'll call a recursive helper function on the root
	
	// Path to remember how to get back up
	// Holds levels of previous things
	std::stack<int> path;

	// Call to recursive helper
	insertHelper(this->rootPageNum, ridKeyCombo, path);
	
}

// Return value is the page, key pair that needs to be added if splitting occured
PageKeyPair<int> BTreeIndex::insertHelper(PageId myPage, RIDKeyPair ridKey, std::stack<int> &path) {
	
	// initialized values
	int myKey = ridKey.key;
	RecordId myRid = ridKey.rid;
	// Arguments we have
	// PageId myPage
	// std::stack<int> path

	// TBD values
	bool imRoot = false;
	Page * myNode;
	// If we ever need to check if we are a leaf or not we can check which one of these is NULL
	NonLeafNodeInt * myNonLeaf = NULL;
	LeafNodeInt * myLeaf = NULL;

	// myNode now holds the info on this page
	bufMgr->readPage(this->file, myPage, myNode);

	// If both checkLeaf1 and 2 are true, we are a leaf
	// Not sure if I'll get NULL pointer if I call top on an empty one so called this way
	bool checkLeaf1 = (path.size() != 0);
	bool checkLeaf2 = false;
	if (checkLeaf1) {
		checkLeaf2 = (path.top() == 1);
	}

	////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////// LEAF SECTION //////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////
	if (checkLeaf1 && checkLeaf2) {
		// Our parent was right before a leaf, we must be a leaf
		myLeaf = (LeafNodeInt *) myNode;

		int numEntries = getNumEntries((Page *) myLeaf, true);

		///////////////////// SPLIT LEAF ////////////////////////////////////////
		if (numEntries == INTARRAYLEAFSIZE - 1) {
			PageId newPageNum;
			Page * newPage;
			LeafNodeInt* newLeaf;
			this->bufMgr->allocPage(this->file, newPageNum, newPage);
			newLeaf = (LeafNodeInt*) newLeaf;

			splitLeafAndInsert(myLeaf, newLeaf, ridKey, numEntries);

			// Improve readability
			LeafNodeInt* leftLeaf;
			LeafNodeInt* rightLeaf;
			// NOTE: Old leaf has to be on left because things still point to it
			leftLeaf = myLeaf;
			rightLeaf = newLeaf;

			// Handle sibling pages
			PageId tempPn;
			tempPn = leftLeaf->rightSibPageNo;
			rightLeaf->rightSibPageNo = tempPn;
			leftLeaf->rightSibPageNo = newPageNum;

			// Get all of the stuff we need to return
			// The middle key we are going to use in non-leaf node
			key returnKey = rightLeaf->keyArray[0]; 
			// The pageNo of the rightLeaf node we just made
			PageId returnPageNum = newPageNum;
			PageKeyPair<int> returnPair;
			returnPair.key = returnKey;
			returnPair.pageNo = newPageNum;

			// Unpin pages we were working on before returning	
			this->bufMgr->unPinPage(this->file, newPageNum, true);
			this->bufMgr->unPinPage(this->file, myPage, true);

			return returnPair;
		} 
		////////////////// DONT SPLIT LEAF /////////////////////////////////////////
		else {
			insertLeafHelper(myLeaf, myKey, numEntries);

			// Unpin pages we were working on
			this->bufMgr->unPinPage(this->file, myPage, true);	
			return NULL;
		}
	} 
	////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////// NON-LEAF SECTION ///////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////
	else {
		////////////// HANDLE FINDING NEXT PAGE IN TREE ////////////////////////////
		myNonLeaf = (NonLeafNodeInt *) myNode;
		// Record we were just on this level
		path.push(myNonLeaf->level);
		// We want to recurse but first we need to know where to go
		PageId nextPage = -1;

		// how many entries are in current non-leaf node
		int numEntries = getNumEntries((Page *) myNonLeaf, false);

		for (int i = 0; i < numEntries; i++) {
			// When we hit first time it's under key array, we have pageNo and break
			if (key < myNonLeaf->keyArray[i]) {
				nextPage = myNonLeaf->pageNoArray[i];
				break;
			}
		}
		// If we never found a number that was bigger, ours must be biggest
		if (nextPage == -1) {
			nextPage = myNonLeaf->pageNoArray[numEntries];
		}
		
		//////////////////////// RECURSE DOWN //////////////////////////////////////
		PageKeyPair<int> splitInfo;
		splitInfo = insertHelper(nextPage, ridKey, path);

		/////////////////////// ON WAY BACK UP /////////////////////////////////////
		
		// We are one level higher
		path.pop();

		/////////////////// NO SPLIT OCCURED ///////////////////////////////////////
		if (splitInfo == NULL) {
			// We didn't make an edits, not dirty, hence the false
			this->bufMgr->unPinPage(this->file, myPage, false);	
			return NULL;
		}

		///////////////////// SPLIT OCCURED ////////////////////////////////////////
		
		// We already have this number and it shouldn't have changed from above
		// int numEntries = getNumEntries( myNode, false);
		// We can hold one more spot in non-leaves than the size
		//////////////////// PROPAGATE SPLIT UP ////////////////////////////////////
		if (numEntries == INTARRAYNONLEAFSIZE) {

			PageId newPageNum;
			Page * newPage;
			NonLeafNodeInt* newNonLeaf;
			bufMgr->allocPage(this->file, newPageNum, newPage);
			newNonLeaf = (NonLeafNodeInt*) newPage;

			Key rootKey = splitNonLeafAndInsert(myNonLeaf, newNonLeaf, splitInfo, numEntries);
			// If the node we are currently on is root
			if (path.size() == 0) {
				// We need to make new root node

				PageId newRootPageNum;
				Page * newRootPage;
				bufMgr->allocPage( this->file, newRootPageNum, newRootPage);
				NonLeafNodeInt * newRoot = (NonLeafNodeInt*) newRootPage;
				for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
					newRoot->keyArray[i] = 0;
					newRoot->pageNoArray[i] = 0;
				}
				newRoot->level = myNonLeaf->level + 1;
				newRoot->keyArray[0] = rootKey;
				// TODO: double check
				newRoot->pageNoArray[0] = myPage;
				newRoot->pageNoArray[1] = newPageNum;

				this->rootLeaf = false;
				this->rootPageNum = newRootPageNum;
				
				// Set meta info
				Page* myMetaPage;
	       		 	IndexMetaInfo* myMetaInfo;
				this->bufMgr->readPage(this->file, this->headerPageNum, myMetaPage);
				myMetaInfo = (IndexMetaInfo*) myMetaPage;
				myMetaInfo->rootLeaf = false;
				myMetaInfo->rootPageNum = newRootPageNum;

				this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
				this->bufMgr->unPinPage(this->file, newRootPageNum, true);	

			}

			this->bufMgr->unPinPage(this->file, newPageNum, true);	
			this->bufMgr->unPinPage(this->file, myPage, true);	

		}
		//////////////////// NO NEED TO PROPAGATE SPLIT ////////////////////////////
		else {
			insertNonLeafHelper( myNonLeaf, splitInfo, numEntries);
			this->bufMgr->unPinPage(this->file, myPage, true);	
			return NULL;
		}
		


	}
}

// This function splits a leaf.  If uneven, left side ends up bigger
// left side -> myLeaf
// right side -> newLeaf
Key BTreeIndex::splitNonLeafAndInsert(NonLeafNodeInt * myNonLeaf, NonLeafNodeInt * newNonLeaf, PageKeyPair<int> insertMe, int numEntries) {

	// To make things more readable let's essentially rename some things
	NonLeafNodeInt * left;
	NonLeafNodeInt * right;

	// We are going to put all of the left stuff in myLeaf and right stuff in newLeaf
	left = myNonLeaf;
	right = newNonLeaf;

	// We are going to need to NULL out some things
	PageId nullPage = NULL;

	Key middleKey = myNonLeaf->keyArray[nodeOccupancy/2];

	/*
	// grabbed
	// Go through and find place to insert it
	int testKey;
	int indexToInsert = 0;
	bool broke = false;
	for (int i = 0; i < numEntries; i++, indexToInsert++) {
		testKey = myNonLeaf->keyArray[i];
		// Have we found the right place to insert
		if (insertMe.key < testKey) {
			broke = true;
			break;
		}
	}
	// Case that it should be at last entry
	if (!broke) {
		indexToInsert = numEntries - 1;
	}
	// Now indexToInsert should be where we want it to

	// If we have an size = 3, this means halfway will be index 1
	// If we have size 2, this means halfway will be index 2
	int halfway = ;

	// False means insert right side
	// True means insert left side
	bool insertLeftSide = NULL;
	// TODO: Double check when not late at night
	// FIXME: Maybe just always split in same spot?
	// If odd, LEAFSIZE/2 is middle index, 
	if (INTARRAYLEAFSIZE % 2 == 1) {
		// Be mindful so splitting splits evenly depending on where new node will be
		if (indexToInsert <= (INTARRAYLEAFSIZE / 2) ) {
			insertLeftSide = true;
			halfway = INTARRAYLEAFSIZE / 2;
		} else {
			insertLeftSide = false;
			halfway = (INTARRAYLEAFSIZE / 2) + 1;
		}
	} else {
		if (indexToInsert <= (INTARRAYLEAFSIZE / 2) ) {
			insertLeftSide = true;
		} else {
			insertLeftSide = false;
		}
		halfway = (INTARRAYLEAFSIZE / 2);
	}
	*/

	// FIXME: simplify things for non-leaves?
	int halfway = INTARRAYNONLEAFSIZE / 2 + 1;

	// Iterate indextoplace as we copy over right half
	int indexToPlace = 0;
	for (int i = halfway; i < INTARRAYLEAFSIZE; i++, indexToPlace++) {
		// Copy them over
		right->keyArray[indexToPlace] = left->keyArray[i];
		right->ridArray[indexToPlace] = left->pageNoArray[i];

		// Clear left out
		left->keyArray[i] = 0;
		left->pageNoArray[i] = NULL;
	}
	// Clear back half of right
	for (int i = indexToPlace; i < INTARRAYLEAFSIZE; i++) {
		right->keyArray[i] = 0;
		right->pageNoArray[i] = NULL;
	}

	// FIXME: Does this simplerway work?
	bool insertLeftSide = NULL;
	if ( insertMe.key < middleKey) {
		insertLeftSide = true;
	} else {
		insertLeftSide = false;
	}

	// Insert left or right depending on what we determined above
	if (insertLeftSide) {
		int numLeft = ((Page *) left, insertMe.key, false);
		insertNonLeafHelper(left, insertMe, numEntries);
	} else {
		int numRight = ((Page *) right, insertMe.key, false);
		insertNonLeafHelper(right, insertMe, numEntries);
	}
	return middleKey;
}

// This function splits a leaf.  If uneven, left side ends up bigger
// left side -> myLeaf
// right side -> newLeaf
// FIXME: Fix to return key?
void BTreeIndex::splitLeafAndInsert(LeafNodeInt * myLeaf, LeafNodeInt * newLeaf, RIDKeyPair<int> insertMe, int numEntries) {

	// To make things more readable let's essentially rename some things
	LeafNodeInt * leftLeaf;
	LeafNodeInt * rightLeaf;

	// We are going to put all of the left stuff in myLeaf and right stuff in newLeaf
	leftLeaf = myLeaf;
	rightLeaf = newLeaf;

	// We are going to need to NULL out some things
	RecordId nullRecord;
	nullRecord.page_number = 0;


	// grabbed
	// Go through and find place to insert it
	int testKey;
	int indexToInsert = 0;
	bool broke = false;
	for (int i = 0; i < numEntries; i++, indexToInsert++) {
		testKey = myLeaf->keyArray[i];
		// Have we found the right place to insert
		if (insertMe.key < testKey) {
			broke = true;
			break;
		}
	}
	// Case that it should be at last entry
	if (!broke) {
		indexToInsert = numEntries - 1;
	}
	// Now indexToInsert should be where we want it to

	// If we have an size = 3, this means halfway will be index 1
	// If we have size 2, this means halfway will be index 2
	int halfway;
	// False means insert right side
	// True means insert left side
	bool insertLeftSide = NULL;

	// TODO: Double check when not late at night
	// FIXME: Maybe just always split in same spot?
	// If odd, LEAFSIZE/2 is middle index, 
	if (INTARRAYLEAFSIZE % 2 == 1) {
		// Be mindful so splitting splits evenly depending on where new node will be
		if (indexToInsert <= (INTARRAYLEAFSIZE / 2) ) {
			insertLeftSide = true;
			halfway = INTARRAYLEAFSIZE / 2;
		} else {
			insertLeftSide = false;
			halfway = (INTARRAYLEAFSIZE / 2) + 1;
		}
	} else {
		if (indexToInsert <= (INTARRAYLEAFSIZE / 2) ) {
			insertLeftSide = true;
		} else {
			insertLeftSide = false;
		}
		halfway = (INTARRAYLEAFSIZE / 2);
	}


	// Iterate indextoplace as we copy over right half
	int indexToPlace = 0;
	for (int i = halfway; i < INTARRAYLEAFSIZE; i++, indexToPlace++) {
		// Copy them over
		rightLeaf->keyArray[indexToPlace] = leftLeaf->keyArray[i];
		rightLeaf->ridArray[indexToPlace] = leftLeaf->ridArray[i];

		// Clear left out
		leftLeaf->keyArray[i] = 0;
		leftLeaf->ridArray[i] = nullRecord;
	}
	// Clear back half of right
	for (int i = indexToPlace; i < INTARRAYLEAFSIZE; i++) {
		rightLeaf->keyArray[i] = 0;
		rightLeaf->ridArray[i] = nullRecord;
	}

	// Insert left or right depending on what we determined above
	if (insertLeftSide) {
		int numLeft = getNumEntries((Page *) leftLeaf, insertMe.key, true);
		insertLeafHelper(leftLeaf, insertMe, numLeft);
	} else {
		int numRight = getNumEntries((Page *) rightLeaf, insertMe.key, true);
		insertLeafHelper(rightLeaf, insertMe, numLeft);
	}

}

// TODO: @Carley Use this method in findLeavesHelper to determine maxIndex you can go to safely
// The number returned is the number of entries in the non leaf node.  So the last valid index
// would be the number returned - 1.  This can be used for leaves and non-leaves depending on
// isLeaf flag
int BTreeIndex::getNumEntries(Page * myNode, bool isLeaf) {
	
	// This should be changed in next line
	int max = -1;
	// If the node isLeaf = INTARRAYLEAFSIZE, else INTARRAYNONLEAFSIZE
	// We have an extra non leaf node pointer
	max = isLeaf ? INTARRAYLEAFSIZE : INTARRAYNONLEAFSIZE + 1;

	// For Leaves
	RecordId myRid;

	// For Non-Leaves
	PageId myPid;

	for (int i = 0; i < max; i++) {
		if (isLeaf) {
			myRid = ( (leafNodeInt *) myNode )->ridArray[i];
			// FIXME: Will this error out when I hit end
			// FIXME: Assumes when we go too far things are zeroed out
			// We know page_number 0 is metadata so that should be safe
			if (myRid.page_number == 0) {
				return i;
			}
		}
		else {
			myPid = ( (NonLeafNodeInt *) myNode )->pageNoArray[i];
			// FIXME: Assume zeroed out again
			if (myPid == 0) {
				return i;
			}
		}	
	}
	return max;
}

//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! NOTE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//!!!!!!!!!!!!!!!!!!!! THIS FUNCTION DOES NOT HANDLE SPLITS !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//!!!!!!!!!!!!!!!!!!!! WILL SEGFAULT IF CALLED ON FULL LEAF !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
void BTreeIndex::insertLeafHelper(LeafNodeInt * myLeaf, RIDKeyPair<int> insertMe, int numEntries) {

	// Just insert it at front if it's new
	if (numEntries == 0) {
		myLeaf->keyArray[0] = insertMe.key;
		myLeaf->ridArray[0] = insertMe.rid;
		return;
	}

	// Go through and find place to insert it
	int testKey;
	for (int i = 0; i < numEntries; i++) {
		testKey = myLeaf->keyArray[i];
		// Have we found the right place to insert
		if (insertMe.key < testKey) {
			// We gotta shift everything over
			// THIS WILL SEGFAULT IF CALLED ON FULL ARRAY
			// From last entry -> where we are, shift, data up 1
			for (int j = numEntries - 1; j > i - 1; j--) {
				myLeaf->keyArray[j+1] = myLeaf->keyArray[j];
				myLeaf->ridArray[j+1] = myLeaf->ridArray[j];
			}
			myLeaf->keyArray[i] = insertMe.key;
			myLeaf->ridArray[i] = insertMe.rid;
			return;
		}
	}
	// If we make it all the way until the end, just put it at end
	myLeaf->keyArray[numEntries] = insertMe.key;
	myLeaf->ridArray[numEntries] = insertMe.rid;
}

// This function is very similar to the leaf-version except we need to place the page to 
// the right of our key.  Also, pageNo arrays are one larger than key arrays
// TODO: Check indices are correct in this version
void BTreeIndex::insertNonLeafHelper(NonLeafNodeInt * myNonLeaf, PageKeyPair<int> insertMe, int numEntries) {

	// Just insert it at front if it's new
	if (numEntries == 0) {
		myNonLeaf->keyArray[0] = insertMe.key;
		myNonLeaf->pageNoArray[1] = insertMe.pageNo;
		return;
	}

	// Go through and find place to insert it
	int testKey;
	for (int i = 0; i < numEntries; i++) {
		testKey = myNonLeaf->keyArray[i];
		// Have we found the right place to insert
		if (insertMe.key < testKey) {
			// We gotta shift everything over
			// THIS WILL SEGFAULT IF CALLED ON FULL ARRAY
			// From last entry -> where we are, shift, data up 1
			for (int j = numEntries - 1; j > i - 1; j--) {
				myLeaf->keyArray[j+1] = myLeaf->keyArray[j];
				myLeaf->pageNoArray[j+2] = myLeaf->pageNoArray[j+1];
			}
			myLeaf->keyArray[i] = insertMe.key;
			myLeaf->pageNoArray[i+1] = insertMe.pageNo;
			return;
		}
	}
	// If we make it all the way until the end, just put it at end
	myLeaf->keyArray[numEntries] = insertMe.key;
	myLeaf->pageNoArray[numEntries+1] = insertMe.pageNo;
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
    lowOp = lowOpParm;
    highOp = highOpParm;	

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
    }
    endScan();
}

/**
 * Traverses the tree recursively to find the leaf node that is found to be at the beginning of the range.
 */
void BTreeIndex::findLeavesHelper(NonLeafNodeInt * currNode, bool nextLeaf, const void* lowVal, const Operator lowOp) {
    int curridx = 0;
    bool smFound = false;
    int numEntries = getNumEntries(currNode, false);
    while (!smFound) {
        if (curridx >= numEntries) {
            // reset currNode as right pid
            bufMgr->readPage(this->file, currNode->pageNoArray[numEntries], currNode);
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
    int numEntries = getNumEntries(currLeaf, true);
    for (int i = numEntries-1; i >= 0; i--) {
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
    if(!this->scanExecuting)
  {
    throw ScanNotInitializedException();
  }
	// Casting page to node
	LeafNodeInt* currentNode = (LeafNodeInt *) currentPageData;
  if(currentNode->ridArray[nextEntry].page_number == 0 or this->nextEntry == this->leafOccupancy)
  {
    //read page
    bufMgr->unPinPage(file, currentPageNum, false);
    // if there is no next leaves
    if(currentNode->rightSibPageNo == 0)
    {
      throw new IndexScanCompletedException();
    }
    this->currentPageNum = currentNode->rightSibPageNo;
    bufMgr->readPage(file, this->currentPageNum, this->currentPageData);
    currentNode = (LeafNodeInt *) currentPageData;
    // Resetting nextEntry
    this->nextEntry = 0;
  }
 
  int key = currentNode->keyArray[this->nextEntry];
  if(keyCheck(*((int *)lowValParm), *((int *)highValParm), lowOp, highOp, key))
  {
    outRid = currentNode->ridArray[this->nextEntry];
    // Incrment nextEntry
    this->nextEntry++;
    // If current page has been scanned to its entirety
  }
  else
  {
    throw new IndexScanCompletedException();
  }
}
					  
//helper for scanNext() to check the key(I thought it was useful)					  
const bool BTreeIndex::keyCheck(int lowVal, int highVal, const Operator lowOp, const Operator highOp, int key)
{
  if(lowOp == GTE && highOp == LTE)
    return key <= highVal && key >= lowVal;
  else if(lowOp == GTE && highOp == LT)  
    return key < highVal && key >= lowVal;  
  else if(lowOp == GT && highOp == LTE)  
    return key <= highVal && key > lowVal;  
  else  
    return key < highVal && key > lowVal;  
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
if(!this->scanExecuting)
    throw new ScanNotInitializedException();
this->scanExecuting = false;
  // Unpin page
bufMgr->unPinPage(file, currentPageNum, false);
  // Resetting variable
this->currentPageData = nullptr;
this->currentPageNum = static_cast<PageId>(-1);
this->nextEntry = -1;
}

}
