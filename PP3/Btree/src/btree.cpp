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

#include <stack>

// This is the structure for tuples in the base relation

typedef struct tuple {
	int i;
	double d;
	char s[64];
} RECORD;


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
		//printf("IN CONSTRUCTOR\n");

		//Taken from spec: How to get name of index file
		std::ostringstream idxStr;
		idxStr << relationName << '.' << attrByteOffset;

		outIndexName = idxStr.str(); // name of index file

		this->bufMgr = bufMgrIn;
		this->scanExecuting = false;
		// Should always be integer
		this->attributeType = attrType;
		this->nodeOccupancy = INTARRAYNONLEAFSIZE;
		this->leafOccupancy = INTARRAYLEAFSIZE;
		this->attrByteOffset = attrByteOffset;
		// At the start our root is a leaf
		this->rootLeaf = true;

		// Things we have yet to set
		// rootPageNum = 0 says we haven't started setting up stuff yet
		this->rootPageNum = 0;
		this->currentPageNum = 0;
		this->headerPageNum = 0;
		this->file = NULL;
		Page* myMetaPage;
		IndexMetaInfo* myMetaInfo;


		// Find out if the file exists
		if (!(File::exists(outIndexName))) {
			this->file = new BlobFile(outIndexName, true);

			//printf("FILE DOESN'T EXIST YET\n");

			// Before looking through and adding things, lets alloc metadata at page 0
			//printf("ALLOCING HEADER\n");
			bufMgr->allocPage(this->file, this->headerPageNum, myMetaPage);
			//printf("HEADER PAGE NUM: %u\n", this->headerPageNum);
			myMetaInfo = (IndexMetaInfo*)(myMetaPage);
			// Since we already set headerPageNum to 0 by default, this is redundant
			// this->headerPageNum = 0;
			strcpy(myMetaInfo->relationName, relationName.c_str());
			myMetaInfo->attrType = this->attributeType;
			myMetaInfo->attrByteOffset = this->attrByteOffset;
			// We again assume insert takes care of this
			myMetaInfo->rootPageNo = 0;
			myMetaInfo->rootLeaf = true;

			// unpin header
			this->bufMgr->unPinPage(this->file, this->headerPageNum, true);

			// Scanner to look through files
			// Directly taken from main.cpp so this should scan through it correctly
			FileScan fscan(relationName, this->bufMgr);
			try {
				RecordId scanRid;
				int counter = 1;
				while(1) {
					fscan.scanNext(scanRid);
					//Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record.
					std::string recordStr = fscan.getRecord();
					const char *record = recordStr.c_str();

					// Edited by mike to be int*
					int * key = ((int *)(record + offsetof (RECORD, i)));

					//std::cout << "Extracted : " << key << std::endl;
					//printf("INSERTING ENTRY NUM: %d WITH KEY %d\n", counter, *key);
					insertEntry(key, scanRid);
					counter++;
				}
			}
			catch(EndOfFileException e) {
				// Do nothing if EOF
			}
		} else {
			// open file
			this->file = new BlobFile(outIndexName, false);
			this->headerPageNum = file->getFirstPageNo();
			this->bufMgr->readPage(this->file, this->headerPageNum, myMetaPage);
			myMetaInfo = (IndexMetaInfo*) myMetaPage;
			this->attrByteOffset = myMetaInfo->attrByteOffset;
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
		this->bufMgr->flushFile(this->file);
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
		//printf("IN INSERT\n");

		int * keyToInsert;
		keyToInsert = (int*) (key);
		RIDKeyPair<int> ridKeyCombo;
		ridKeyCombo.set(rid, *(keyToInsert));


		// FIXME FIXME DEBUG
		//	Page* myMetaPage;
		//	IndexMetaInfo* myMetaInfo;
		//	this->bufMgr->readPage(this->file, this->headerPageNum, myMetaPage);
		//	myMetaInfo = (IndexMetaInfo*) myMetaPage;
		//	std::cout << myMetaInfo->rootPageNo << std::endl;

		// If we don't have a root yet, let's make a root, update info
		//printf("On Insert\n");
		if (this->rootPageNum == 0) {

			//printf("INSERTING ROOT\n");
			Page * newNode;
			PageId newPageNum;
			bufMgr->allocPage(this->file, newPageNum, newNode);
			//printf("ROOT PAGE NUM: %u\n", newPageNum);

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
			myMetaInfo->rootPageNo = newPageNum;

			this->bufMgr->unPinPage(this->file, newPageNum, true);
			this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
			return;
		}
		// Else we'll call a recursive helper function on the root

		// Path to remember how to get back up
		// Holds levels of previous things
		std::stack<int> path;

		// Call to recursive helper
		// FIXME: If root was a leaf and just got split we might have to handle that here
		insertHelper(this->rootPageNum, ridKeyCombo, path);

	}

	// Return value is the page, key pair that needs to be added if splitting occured
	// Recursive Helper function which handles insertions, balancing of b tree
	PageKeyPair<int> BTreeIndex::insertHelper(PageId myPage, RIDKeyPair<int> ridKey, std::stack<int> &path) {

		// initialized values
		int myKey = ridKey.key;
		// Arguments we have
		// PageId myPage
		// std::stack<int> path

		// TBD values
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
			checkLeaf2 = ((path.top() == 1) || (this->rootLeaf));
		}

		////////////////////////////////////////////////////////////////////////////////////
		//////////////////////////////// LEAF SECTION //////////////////////////////////////
		////////////////////////////////////////////////////////////////////////////////////
		//printf("RIGHT BEFORE CHECKLEAF\n");
		if ( (checkLeaf1 && checkLeaf2) || this->rootLeaf) {
			//printf("IN CHECKLEAF\n");
			// Our parent was right before a leaf, we must be a leaf
			myLeaf = (LeafNodeInt *) myNode;

			int numEntries = getNumEntries((Page *) myLeaf, true);

			///////////////////// SPLIT LEAF ////////////////////////////////////////
			// If we are at max capacity
			if (numEntries == INTARRAYLEAFSIZE - 1 ) {

				//printf("SPLITTING LEAF\n");

				// Alloc new leaf
				PageId newPageNum = 0;
				Page * newPage;
				LeafNodeInt* newLeaf;
				//printf("BEFORE ALLOC: new page num: %u\n", newPageNum);
				//printf("Trying to insert key: %d\n", myKey);
				//	printLeaf(myLeaf);
				this->bufMgr->allocPage(this->file, newPageNum, newPage);
				//printf("AFTER ALLOC: new page num: %u\n", newPageNum);

				newLeaf = (LeafNodeInt*) newPage;

				//printf("numEntries is %d\n", numEntries);

				// Split and insert new leaf
				splitLeafAndInsert(myLeaf, newLeaf, ridKey, numEntries);

				//printf("THROUGH SPLIT AND INSERT\n");

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
				int returnKey = rightLeaf->keyArray[0];
				// The pageNo of the rightLeaf node we just made

				PageId returnPageNum = newPageNum;
				PageKeyPair<int> returnPair;
				returnPair.key = returnKey;
				returnPair.pageNo = returnPageNum;

				//printf("makes to rootleaf\n");

				// If the node we are currently on is root
				if ( this->rootLeaf ) {

					//printf("IN ROOTLEAF SPLIT\n");
					// We need to make new root node cuz we have nothing to return to?
					PageId newRootPageNum;
					Page * newRootPage;
					bufMgr->allocPage( this->file, newRootPageNum, newRootPage);
					//printf("ROOTLEAF SPLIT NUM%u\n", newRootPageNum);
					NonLeafNodeInt * newRoot = (NonLeafNodeInt*) newRootPage;
					for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
						newRoot->keyArray[i] = 0;
						newRoot->pageNoArray[i] = 0;
					}
					newRoot->level = 1;
					newRoot->keyArray[0] = returnKey;

					// Old page on left, new page on right
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
					myMetaInfo->rootPageNo = newRootPageNum;

					this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
					this->bufMgr->unPinPage(this->file, newRootPageNum, true);

				}

				// Unpin pages we were working on before returning
				this->bufMgr->unPinPage(this->file, newPageNum, true);
				this->bufMgr->unPinPage(this->file, myPage, true);

				// The fact we didn't return NULL signals to calling function we just split
				return returnPair;
			}
			////////////////// DONT SPLIT LEAF /////////////////////////////////////////
			else {
				// It's safe to just insert mykey in the leaf

				insertLeafHelper(myLeaf, ridKey, numEntries);

				// Unpin pages we were working on
				this->bufMgr->unPinPage(this->file, myPage, true);

				// Signal that we didn't split to calling function

				//printf("not splitting leaf\n");

				// Return a PageKeyPair with 0
				PageKeyPair<int> zeroRet;
				zeroRet.key = 0;
				zeroRet.pageNo = 0;
				return zeroRet;
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
			PageId nextPage = 0;

			// how many entries are in current non-leaf node
			int numEntries = getNumEntries((Page *) myNonLeaf, false);

			// Determine which page to to recurse down into
			// TODO: Double check correct page
			for (int i = 0; i < numEntries; i++) {
				// When we hit first time it's under key array, we have pageNo and break
				if (myKey < myNonLeaf->keyArray[i]) {
					nextPage = myNonLeaf->pageNoArray[i];
					break;
				}
			}
			// If we never found a number that was bigger, ours must be biggest
			if (nextPage == 0) {
				nextPage = myNonLeaf->pageNoArray[numEntries];
			}

			//////////////////////// RECURSE DOWN //////////////////////////////////////
			PageKeyPair<int> splitInfo;
			splitInfo = insertHelper(nextPage, ridKey, path);

			/////////////////////// ON WAY BACK UP /////////////////////////////////////

			// We are one level higher
			path.pop();

			/////////////////// NO SPLIT OCCURED ///////////////////////////////////////
			if (splitInfo.pageNo == 0) {
				// We didn't make any edits, not dirty, hence the false
				this->bufMgr->unPinPage(this->file, myPage, false);

				// Return a PageKeyPair with 0
				PageKeyPair<int> zeroRet;
				zeroRet.key = 0;
				zeroRet.pageNo = 0;
				return zeroRet;
			}

			///////////////////// SPLIT OCCURED ////////////////////////////////////////

			// We can hold one more spot in non-leaves than the size
			//////////////////// PROPAGATE SPLIT UP ////////////////////////////////////
			// TODO: Check this is right number, it or leaf one might be off by one
			if (numEntries == INTARRAYNONLEAFSIZE) {

				//printf("SPLITTING NONLEAF\n");
				// Alloc new non-Leaf
				PageId newPageNum;
				Page * newPage;
				NonLeafNodeInt* newNonLeaf;
				bufMgr->allocPage(this->file, newPageNum, newPage);
				newNonLeaf = (NonLeafNodeInt*) newPage;

				//printf("NON LEAF SPLIT NUM%u\n", newPageNum);
				int returnKey = splitNonLeafAndInsert(myNonLeaf, newNonLeaf, splitInfo, numEntries);
				// Get all of the stuff we need to return
				// The pageNo of the rightLeaf node we just made

				PageId returnPageNum = newPageNum;
				PageKeyPair<int> returnPair;
				returnPair.key = returnKey;
				returnPair.pageNo = returnPageNum;

				// If the node we are currently on is root
				if (path.size() == 0) {

					// We need to make new root node cuz we have nothing to return to?
					//printf("SPLITTING NON LEAF ROOT\n");
					PageId newRootPageNum;
					Page * newRootPage;
					bufMgr->allocPage( this->file, newRootPageNum, newRootPage);
					//printf("NONLEAF ROOT PAGE NUM: %u\n", newRootPageNum);
					NonLeafNodeInt * newRoot = (NonLeafNodeInt*) newRootPage;
					for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
						newRoot->keyArray[i] = 0;
						newRoot->pageNoArray[i] = 0;
					}
					newRoot->level = myNonLeaf->level + 1;
					newRoot->keyArray[0] = returnKey;

					// Old page on left, new page on right
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
					myMetaInfo->rootPageNo = newRootPageNum;

					this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
					this->bufMgr->unPinPage(this->file, newRootPageNum, true);

				}

				this->bufMgr->unPinPage(this->file, newPageNum, true);
				this->bufMgr->unPinPage(this->file, myPage, true);
				// Doesn't matter if we return garbage on root since it's not being used in original insert function?
				return returnPair;

			}
			//////////////////// NO NEED TO PROPAGATE SPLIT ////////////////////////////
			else {
				// Just insert and return NULL to say split didn't go up
				insertNonLeafHelper( myNonLeaf, splitInfo, numEntries);
				this->bufMgr->unPinPage(this->file, myPage, true);

				// Return a PageKeyPair with 0
				PageKeyPair<int> zeroRet;
				zeroRet.key = 0;
				zeroRet.pageNo = 0;
				return zeroRet;
			}



		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////
	// SPLITNONLEAFANDINSERT
	// This function splits a non-leaf.
	// left side -> myLeaf
	// right side -> newLeaf
	/////////////////////////////////////////////////////////////////////////////////////////
	int BTreeIndex::splitNonLeafAndInsert(NonLeafNodeInt * myNonLeaf, NonLeafNodeInt * newNonLeaf, PageKeyPair<int> insertMe, int numEntries) {

		// To make things more readable let's essentially rename some things
		NonLeafNodeInt * left;
		NonLeafNodeInt * right;

		// We are going to put all of the left stuff in myLeaf and right stuff in newLeaf
		left = myNonLeaf;
		right = newNonLeaf;

		// Middle node key
		int middleKey = myNonLeaf->keyArray[nodeOccupancy/2];

		// Place we are going to split at
		int halfway = INTARRAYNONLEAFSIZE / 2 + 1;

		// Iterate indextoplace as we copy over right half
		int indexToPlace = 0;
		for (int i = halfway; i < INTARRAYNONLEAFSIZE; i++, indexToPlace++) {
			// Copy them over
			right->keyArray[indexToPlace] = left->keyArray[i];
			right->pageNoArray[indexToPlace] = left->pageNoArray[i];

			// Clear left out
			left->keyArray[i] = 0;
			left->pageNoArray[i] = 0;
		}
		// Clear back half of right
		for (int i = indexToPlace; i < INTARRAYNONLEAFSIZE; i++) {
			right->keyArray[i] = 0;
			right->pageNoArray[i] = 0;
		}

		// Figure out which side our new key has to go in
		bool insertLeftSide = NULL;
		if ( insertMe.key < middleKey) {
			insertLeftSide = true;
		} else {
			insertLeftSide = false;
		}

		// Insert left or right depending on what we determined above
		if (insertLeftSide) {
			int numLeft = getNumEntries((Page *) left, false);
			insertNonLeafHelper(left, insertMe, numLeft);
		} else {
			int numRight = getNumEntries((Page *) right, false);
			insertNonLeafHelper(right, insertMe, numRight);
		}
		// MIGHT not want to return this, just find middle key after
		return middleKey;
	}

	/////////////////////////////////////////////////////////////////////////////////////////
	// SPLITLEAFANDINSERT
	// This function splits a leaf. 
	// left side -> myLeaf
	// right side -> newLeaf
	/////////////////////////////////////////////////////////////////////////////////////////
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

		int halfway = INTARRAYLEAFSIZE/2 + 1;

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

		bool insertLeftSide = (insertMe.key < rightLeaf->keyArray[0]);

		// Insert left or right depending on what we determined above
		if (insertLeftSide) {
			int numLeft = getNumEntries((Page *) leftLeaf, true);
			insertLeafHelper(leftLeaf, insertMe, numLeft);
		} else {
			int numRight = getNumEntries((Page *) rightLeaf, true);
			insertLeafHelper(rightLeaf, insertMe, numRight);
		}

	}

	///////////////////////////////////////////////////////////////////////////////////////
	// PRINTLEAF
	// Debug method
	///////////////////////////////////////////////////////////////////////////////////////
	void BTreeIndex::printLeaf(LeafNodeInt * myLeaf) {
		for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
			printf("Key at index: %d = %d\n", i, myLeaf->keyArray[i]);
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////
	// GETNUMENTRIES
	// Use this method in findLeavesHelper to determine maxIndex you can go to safely
	// The number returned is the number of entries in the non leaf node 
	// Works for leaves and non leaves depending on isLeaf flag
	//////////////////////////////////////////////////////////////////////////////////////
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
				myRid = ( (LeafNodeInt *) myNode )->ridArray[i];
				// We know page_number 0 is metadata so that should be safe
				if (myRid.page_number == 0) {
					return i;
				}
			}
			else {
				myPid = ( (NonLeafNodeInt *) myNode )->pageNoArray[i+1];
				if (myPid == 0) {
					return i;
				}
			}
		}
		return max;
	}

	/////////////////////////////////////////////////////////////////////////////////////////
	// INSERTLEAFHELPER
	// Inserts leaves
	// NOTE: Will segfault if called on full leave
	/////////////////////////////////////////////////////////////////////////////////////////
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

	/////////////////////////////////////////////////////////////////////////////////////////
	// INSERTNONLEAFHELPER
	// This function is very similar to the leaf-version except we need to place the page to
	// the right of our key.  Also, pageNo arrays are one larger than key arrays
	/////////////////////////////////////////////////////////////////////////////////////////
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
				// One different from leaf function
				for (int j = numEntries - 1; j > i - 1; j--) {
					myNonLeaf->keyArray[j+1] = myNonLeaf->keyArray[j];
					myNonLeaf->pageNoArray[j+2] = myNonLeaf->pageNoArray[j+1];
				}
				myNonLeaf->keyArray[i] = insertMe.key;
				myNonLeaf->pageNoArray[i+1] = insertMe.pageNo;
				return;
			}
		}
		// If we make it all the way until the end, just put it at end
		myNonLeaf->keyArray[numEntries] = insertMe.key;
		myNonLeaf->pageNoArray[numEntries+1] = insertMe.pageNo;
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
		if (*(int*)lowValParm > *(int*)highValParm) {
			throw new BadScanrangeException;
		}
		lowOp = lowOpParm;
		highOp = highOpParm;
		lowValInt = *(int*)lowValParm;
		highValInt = *(int*)highValParm;

		// check if scan already in progress and if so, end it
		if (this->scanExecuting) {
			endScan();
		}
		this->scanExecuting = true;

		// FIXME GRANT: Moving this uncommented logic above into the recursive methods
		bool found = findLeavesHelper(this->rootPageNum);
		if (!found) {
			this->endScan();
			throw NoSuchKeyFoundException();
		}



	}

	/////////////////////////////////////////////////////////////////////////////////////////
	// FINDLEAVESHELPER
	// Traverses the tree recursively to find the leaf node that is found to be at the beginning of the range.
	/////////////////////////////////////////////////////////////////////////////////////////
	bool BTreeIndex::findLeavesHelper(PageId pn) {

		//printf("FIND LEAVES HELPER: PID = %d\n", pn);

		Page* currPage;
		NonLeafNodeInt* currNode;
		bufMgr->readPage(this->file, pn, currPage);
		currNode = (NonLeafNodeInt*)currPage;

		int curridx = 0;
		int numEntries = getNumEntries((Page*)currNode, false);

		//////////////// ABOVE NON LEAF /////////////////////
		if (currNode->level != 1) {
			// Traverse the tree
			for (curridx = 0; curridx < numEntries-1; curridx++) {
				if (currNode->keyArray[curridx] > lowValInt) {

					PageId newPage = currNode->pageNoArray[curridx];
					bufMgr->unPinPage(this->file, pn, false);
					return findLeavesHelper(newPage);
				}

			}
			PageId newPage = currNode->pageNoArray[curridx+1];
			bufMgr->unPinPage(this->file, pn, false);
			return findLeavesHelper(newPage);
		} 
		//////////////// ABOVE LEAF /////////////////////
		else {
			// look what leaf to got to, go there
			for (curridx = 0; curridx < numEntries-1; curridx++) {
				if (currNode->keyArray[curridx] > lowValInt) {
					PageId newPage = currNode->pageNoArray[curridx];
					bufMgr->unPinPage(this->file, pn, false);
					return lowLeafHelper(newPage);
				}
			}
			PageId newPage = currNode->pageNoArray[curridx+1];
			bufMgr->unPinPage(this->file, pn, false);
			return lowLeafHelper(newPage);

		}

		return false;

	}

	/////////////////////////////////////////////////////////////////////////////////////////
	// LOWLEAFHELPER
	// Throws new NoSuchKeyFoundException in the case where we couldn't find and value >/>= lowVal
	// Helper method for scanning in leaves
	////////////////////////////////////////////////////////////////////////////////////////
	bool BTreeIndex::lowLeafHelper(PageId pn) {

		// empty leaf
		if (pn == 0) {
			return false;
		}

		this->currentPageNum = pn;
		bufMgr->readPage(this->file, pn, this->currentPageData);
		LeafNodeInt* currNode = (LeafNodeInt*)this->currentPageData;
		int numEntries = getNumEntries((Page*)currNode, true);
		int* keys = currNode->keyArray;
		numEntries -= 1;
		int i;

		// This is just the same section of code copied 4 times based on the ops

		if (lowOp == GT && highOp == LT) {
			for (i = 0; i <= numEntries; i++) {
				if (keys[i] >= highValInt) {
					bufMgr->readPage(this->file, pn, this->currentPageData);
					bufMgr->unPinPage(this->file, this->currentPageNum, false);
					return false;
				}
				if (lowValInt < keys[i] && keys[i] < highValInt) {
					this->nextEntry = i;
					return true;
				}
			}
			bufMgr->readPage(this->file, pn, this->currentPageData);
			bufMgr->unPinPage(this->file, currentPageNum, false);
			return lowLeafHelper(currNode->rightSibPageNo);
		} else if (lowOp == GTE && highOp == LT) {
			for (i = 0; i <= numEntries; i++) {
				if (keys[i] >= highValInt) {
					bufMgr->readPage(this->file, pn, this->currentPageData);
					bufMgr->unPinPage(this->file, currentPageNum, false);
					return false;
				}
				if (lowValInt <= keys[i] && keys[i] < highValInt) {
					this->nextEntry = i;
					return true;
				}
			}
			bufMgr->readPage(this->file, pn, this->currentPageData);
			bufMgr->unPinPage(this->file, currentPageNum, false);
			return lowLeafHelper(currNode->rightSibPageNo);
		} else if (lowOp == GTE && highOp == LTE) {
			for (i = 0; i <= numEntries; i++) {
				if (keys[i] > highValInt) {
					bufMgr->readPage(this->file, pn, this->currentPageData);
					bufMgr->unPinPage(this->file, currentPageNum, false);
					return false;
				}
				if (lowValInt <= keys[i] && keys[i] <= highValInt) {
					this->nextEntry = i;
					return true;
				}
			}
			bufMgr->readPage(this->file, pn, this->currentPageData);
			bufMgr->unPinPage(this->file, currentPageNum, false);
			return lowLeafHelper(currNode->rightSibPageNo);
		} else if (lowOp == GT && highOp == LTE) {
			for (i = 0; i <= numEntries; i++) {
				if (keys[i] > highValInt) {
					bufMgr->readPage(this->file, pn, this->currentPageData);
					bufMgr->unPinPage(this->file, currentPageNum, false);
					return false;
				}
				if (lowValInt < keys[i] && keys[i] <= highValInt) {
					this->nextEntry = i;
					return true;
				}
			}
			bufMgr->readPage(this->file, pn, this->currentPageData);
			bufMgr->unPinPage(this->file, currentPageNum, false);
			return lowLeafHelper(currNode->rightSibPageNo);
		}
		return false;



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

		if (this->nextEntry == -1) {
			throw IndexScanCompletedException();
		}

		// The current node that we're scanning through
		LeafNodeInt* currentNode = (LeafNodeInt *) currentPageData;

		// Get the lastIndex that we used and the rid array for this node
		outRid = currentNode->ridArray[nextEntry];
		int lastIndexUsed = getNumEntries(this->currentPageData, true);
		lastIndexUsed -= 1;

		// If we are still on the same node...
		if (nextEntry + 1 <= lastIndexUsed) {

			// Get the next value and check it against our key
			int nextVal = currentNode->keyArray[nextEntry + 1];
			if (  ((highOp == LT) && (nextVal < highValInt)) || ((highOp == LTE) && (nextVal <= highValInt)) ) {
				nextEntry = nextEntry + 1;
			} else {
				nextEntry = -1;
			}

		} else {
			// If we need to go to the next node, go to the right sibling
			PageId lastPage = currentPageNum;
			currentPageNum = currentNode->rightSibPageNo;

			// If the right sibling doesn't exist, end
			if(currentNode->rightSibPageNo ==0)
			{
				bufMgr->unPinPage(this->file, lastPage, false);
				nextEntry = -1;
				return;
			}

			// Read the right sibling
			bufMgr->readPage( file, currentNode->rightSibPageNo, currentPageData);
			currentNode = (LeafNodeInt*) currentPageData;
			bufMgr->unPinPage(this->file, lastPage, false);
			nextEntry = -1;

			// Get the next value and check it against our key
			int nextVal = currentNode->keyArray[nextEntry+1];
			if (  ((highOp == LT) && (nextVal < highValInt)) || ((highOp == LTE) && (nextVal <= highValInt)) ) {
				nextEntry = nextEntry + 1;
			} else {
				nextEntry = -1;
			}

		}

	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::endScan
	// -----------------------------------------------------------------------------
	//
	const void BTreeIndex::endScan()
	{
		if(!this->scanExecuting) {
			throw new ScanNotInitializedException();
		}
		this->scanExecuting = false;
		// Unpin page
		bufMgr->unPinPage(file, currentPageNum, false);
		// Resetting variable
		this->currentPageData = nullptr;
		this->currentPageNum = static_cast<PageId>(-1);
		this->nextEntry = -1;
	}

}
