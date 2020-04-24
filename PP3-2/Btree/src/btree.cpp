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


namespace badgerdb
{

/**
 * BTreeIndex Constructor. 
 * Check to see if the corresponding index file exists. If so, open the file.
 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
 *
 * @param relationName        Name of file.
 * @param outIndexName        Return the name of index file.
 * @param bufMgrIn            Buffer Manager Instance
 * @param attrByteOffset      Offset of attribute, over which index is to be built, in the record
 * @param attrType            Datatype of attribute over which index is built
 * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
 */
BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
  //bufferManager
  this->bufMgr = bufMgrIn;
  this->scanExecuting = false;
  this->nodeOccupancy = INTARRAYNONLEAFSIZE;
  this->leafOccupancy = INTARRAYLEAFSIZE; 
  std::ostringstream idxStr;
  idxStr << relationName << "." << attrByteOffset;
  outIndexName = idxStr.str();

  if (!File::exists(outIndexName)){
    //when file doesn't exist
    file = new BlobFile(outIndexName, true);
    //header page
    Page *hPage;
    //root page
    Page *rPage;
    bufMgr->allocPage(file, headerPageNum, hPage);
    bufMgr->allocPage(file, rootPageNum, rPage);
    // fill meta info
    IndexMetaInfo *myMetaInfo = (IndexMetaInfo *)hPage;
    myMetaInfo->attrType = attrType;
    myMetaInfo->attrByteOffset = attrByteOffset;    
    myMetaInfo->rootPageNo = rootPageNum;
    strncpy((char *)(&(myMetaInfo->relationName)), relationName.c_str(), 20);
    myMetaInfo->relationName[19] = 0;

    rootInitNum = rootPageNum;
    LeafNodeInt *rootLeaf = (LeafNodeInt *)rPage;
    rootLeaf->rightSibPageNo = 0;

    bufMgr->unPinPage(file, headerPageNum, true);
    bufMgr->unPinPage(file, rootPageNum, true);

    FileScan fileScan(relationName, bufMgr); 
    try{
      RecordId rid;
      while(1){
        fileScan.scanNext(rid);
        std::string record = fileScan.getRecord();
        insertEntry(record.c_str() + attrByteOffset, rid);
      }
    }
    catch(EndOfFileException e){
      bufMgr->flushFile(file);
    }   
  }
 
  else{
    //when file exists
    this->file = new BlobFile(outIndexName, false);
    // read meta info
    this->headerPageNum = file->getFirstPageNo();
    Page *hPage;
    bufMgr->readPage(file, headerPageNum, hPage);
    IndexMetaInfo *myMetaInfo = (IndexMetaInfo *)hPage;
    rootPageNum = myMetaInfo->rootPageNo;
    if (attrType != myMetaInfo->attrType || relationName != myMetaInfo->relationName || attrByteOffset != myMetaInfo->attrByteOffset){
      throw BadIndexInfoException(outIndexName);
    }
    bufMgr->unPinPage(file, headerPageNum, false); 
  }
}

/**
 * BTreeIndex Destructor. 
 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
 * and delete file instance thereby closing the index file.
 * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
 */
BTreeIndex::~BTreeIndex()
{
  scanExecuting = false;
  bufMgr->flushFile(BTreeIndex::file);
  delete file;
	file = nullptr;
}

/**
 * Insert a new entry using the pair <value,rid>. 
 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
 * Make sure to unpin pages as soon as you can.
 * @param key     Key to insert, pointer to integer/double/char string
 * @param rid     Record ID of a record whose entry is getting inserted into the index.
**/

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
  RIDKeyPair<int> entry;
  entry.set(rid, *((int *)key));
  Page* rootLeaf;
  bufMgr->readPage(file, rootPageNum, rootLeaf);
  PageKeyPair<int> *newEntry = nullptr;
  bool leafFlag = false; 
  if(rootPageNum == rootInitNum)
    leafFlag = true;
  insertHelper(rootLeaf, rootPageNum, leafFlag, entry, newEntry);
}


/**
 * Recursive function to act as helper to insert (inserts the index entry to the index file)
*/
const void BTreeIndex::insertHelper(Page *currentPage, PageId numPage, bool leafFlag, const RIDKeyPair<int> entry, PageKeyPair<int> *&newEntry)
{
  // nonleaf node
  if (!leafFlag){
    NonLeafNodeInt *curNode = (NonLeafNodeInt *)currentPage;    
    PageId nextPageId;
    Page *nextPage;     
    int i = nodeOccupancy;
    while(i >= 0 && (curNode->pageNoArray[i] == 0)){
      i--;
    }
    while(i > 0 && (curNode->keyArray[i-1] >= entry.key)){
      i--;
    }
    nextPageId = curNode->pageNoArray[i];
    bufMgr->readPage(file, nextPageId, nextPage);    
    leafFlag = curNode->level == 1;
    insertHelper(nextPage, nextPageId, leafFlag, entry, newEntry);

    if (newEntry == nullptr){
	    bufMgr->unPinPage(file, numPage, false);
    }
    else{       
      if (curNode->pageNoArray[nodeOccupancy] != 0){
        insertIndexEntryToFile(curNode, numPage, newEntry);
      }
      else{
        int j = nodeOccupancy;
        while(j >= 0 && (curNode->pageNoArray[j] == 0)){
          j--;
        }
        // shift
        while( j > 0 && (newEntry->key < curNode->keyArray[j-1]) ){
          curNode->keyArray[j] = curNode->keyArray[j-1];
          curNode->pageNoArray[j+1] = curNode->pageNoArray[j];
          j--;
        }
        // insert
        curNode->keyArray[j] = newEntry->key;
        curNode->pageNoArray[j+1] = newEntry->pageNo;
        newEntry = nullptr;
        
        bufMgr->unPinPage(file, numPage, true);
      }
    }
  }
  else{
    LeafNodeInt *leafNode = (LeafNodeInt *)currentPage;
    
    if (leafNode->ridArray[leafOccupancy - 1].page_number == 0){
      //non empty leafNode page
      if (leafNode->ridArray[0].page_number != 0){        
        int i = leafOccupancy - 1;
        while(i >= 0 && (leafNode->ridArray[i].page_number == 0)){
          i--;
        }
        // shift entry
        while(i >= 0 && (leafNode->keyArray[i] > entry.key)){
          leafNode->keyArray[i+1] = leafNode->keyArray[i];
          leafNode->ridArray[i+1] = leafNode->ridArray[i];
          i--;
        }
        // insert entry
        leafNode->keyArray[i+1] = entry.key;
        leafNode->ridArray[i+1] = entry.rid;   
      }
      else{
        leafNode->keyArray[0] = entry.key;
        leafNode->ridArray[0] = entry.rid; 
      }
      bufMgr->unPinPage(file, numPage, true);
      newEntry = nullptr;
    }
    else{
      splitFullLeafNode(leafNode, numPage, newEntry, entry);
    }
  }
}


const void BTreeIndex::splitFullLeafNode(LeafNodeInt *leafNode, PageId numPage, PageKeyPair<int> *&newEntry, const RIDKeyPair<int> entry)
{
  PageId newPageNum;
  Page *newPage;
  bufMgr->allocPage(file, newPageNum, newPage);
  LeafNodeInt *rightNode = (LeafNodeInt *)newPage;
  int m = leafOccupancy/2;
  if (leafOccupancy %2 == 1 && entry.key > leafNode->keyArray[m])
    m = m + 1;
  // copy half of the pages to rightNode
  for(int i = m; i < leafOccupancy; i++){
    rightNode->ridArray[i-m] = leafNode->ridArray[i];
    rightNode->keyArray[i-m] = leafNode->keyArray[i];    
    leafNode->keyArray[i] = 0;
    leafNode->ridArray[i].page_number = 0;
  }
  
  if (leafNode->keyArray[m-1] > entry.key){
    if (leafNode->ridArray[0].page_number == 0){
      leafNode->keyArray[0] = entry.key;
      leafNode->ridArray[0] = entry.rid;    
    }
    else{
      int i = leafOccupancy - 1;
      // find the end
      while(i >= 0 && (leafNode->ridArray[i].page_number == 0)){
        i--;
      }
      // shift entry
      while(i >= 0 && (leafNode->keyArray[i] > entry.key)){
        leafNode->keyArray[i+1] = leafNode->keyArray[i];
        leafNode->ridArray[i+1] = leafNode->ridArray[i];
        i--;
      }
      // insert entry
      leafNode->keyArray[i+1] = entry.key;
      leafNode->ridArray[i+1] = entry.rid;
    }    
  }
  else{
    if (rightNode->ridArray[0].page_number == 0){
      rightNode->ridArray[0] = entry.rid;
      rightNode->keyArray[0] = entry.key;          
    }
    else{
      int i = leafOccupancy - 1;
      // find the end
      while(i >= 0 && (rightNode->ridArray[i].page_number == 0)){
        i--;
      }
      // shift entry
      while(i >= 0 && (rightNode->keyArray[i] > entry.key)){
        rightNode->keyArray[i+1] = rightNode->keyArray[i];
        rightNode->ridArray[i+1] = rightNode->ridArray[i];
        i--;
      }
      // insert entry
      rightNode->keyArray[i+1] = entry.key;
      rightNode->ridArray[i+1] = entry.rid;
    }
  }

  // update sibling pointer
  rightNode->rightSibPageNo = leafNode->rightSibPageNo;
  leafNode->rightSibPageNo = newPageNum;

  // new child entry
  PageKeyPair<int> newKeyPair;
  newEntry = new PageKeyPair<int>();
  
  newKeyPair.set(newPageNum, rightNode->keyArray[0]);
  newEntry = &newKeyPair;
  bufMgr->unPinPage(file, numPage, true);
  bufMgr->unPinPage(file, newPageNum, true);

  // if curr page is root
  if (numPage == rootPageNum){
    rootUpdateHelper(numPage, newEntry);
  }
}

//ADD Comments
//old nodes on the left, new on the right
const void BTreeIndex::insertIndexEntryToFile(NonLeafNodeInt *leftNode, PageId leftPageNum, PageKeyPair<int> *&newEntry)
{
  // allocate a new nonleaf node
  PageId newPageNum;
  Page *newPage;
  bufMgr->allocPage(file, newPageNum, newPage);
  NonLeafNodeInt *rightNode = (NonLeafNodeInt *)newPage;

  int m = nodeOccupancy/2;
  int indexPush = m;
  PageKeyPair<int> pEntry;
  // even number of keys
  if (nodeOccupancy % 2 == 0){
    indexPush = newEntry->key < leftNode->keyArray[m] ? m -1 : m;
  }
  pEntry.set(newPageNum, leftNode->keyArray[indexPush]);

  m = indexPush + 1;
  // move half of the entries to the new node
  for(int i = m; i < nodeOccupancy; i++){
    rightNode->keyArray[i-m] = leftNode->keyArray[i];
    rightNode->pageNoArray[i-m] = leftNode->pageNoArray[i+1];
    leftNode->pageNoArray[i+1] = (PageId) 0;
    leftNode->keyArray[i+1] = 0;
  }

  rightNode->level = leftNode->level;
  // remove the entry that is pushed up from current node
  leftNode->keyArray[indexPush] = 0;
  leftNode->pageNoArray[indexPush] = (PageId) 0;
  // insert the new child entry
  if(newEntry->key < rightNode->keyArray[0]){
    int i = nodeOccupancy;
    while(i >= 0 && (leftNode->pageNoArray[i] == 0)){
      i--;
    }
    // shift
    while( i > 0 && (leftNode->keyArray[i-1] > newEntry->key)){
      leftNode->keyArray[i] = leftNode->keyArray[i-1];
      leftNode->pageNoArray[i+1] = leftNode->pageNoArray[i];
      i--;
    }
    // insert
    leftNode->keyArray[i] = newEntry->key;
    leftNode->pageNoArray[i+1] = newEntry->pageNo; 
  }
  else{
    int i = nodeOccupancy;
    while(i >= 0 && (leftNode->pageNoArray[i] == 0)){
      i--;
    }
    // shift
    while( i > 0 && (leftNode->keyArray[i-1] > newEntry->key)){
      rightNode->keyArray[i] = rightNode->keyArray[i-1];
      rightNode->pageNoArray[i+1] = rightNode->pageNoArray[i];
      i--;
    }
    // insert
    rightNode->keyArray[i] = newEntry->key;
    rightNode->pageNoArray[i+1] = newEntry->pageNo;
  }
  newEntry = &pEntry;
  bufMgr->unPinPage(file, leftPageNum, true);
  bufMgr->unPinPage(file, newPageNum, true);

  
  if (leftPageNum == rootPageNum){
    rootUpdateHelper(leftPageNum, newEntry);
  }
}


//CHANGE AND ADD COMMENTS
const void BTreeIndex::rootUpdateHelper(PageId rootPointer, PageKeyPair<int> *newEntry){
  // create a new root 
  PageId numPageNew;
  Page *newRootPointer;
  bufMgr->allocPage(file, numPageNew, newRootPointer);
  NonLeafNodeInt *newRootPointerPage = (NonLeafNodeInt *)newRootPointer;

  // update metadata
  newRootPointerPage->level = rootInitNum == rootPageNum ? 1 : 0;
  newRootPointerPage->pageNoArray[0] = rootPointer;
  newRootPointerPage->pageNoArray[1] = newEntry->pageNo;
  newRootPointerPage->keyArray[0] = newEntry->key;

  Page *myMetaPage;
  bufMgr->readPage(file, headerPageNum, myMetaPage);
  IndexMetaInfo *myMetaInfo = (IndexMetaInfo *)myMetaPage;
  myMetaInfo->rootPageNo = numPageNew;
  rootPageNum = numPageNew;
  // unpin unused page
  bufMgr->unPinPage(file, headerPageNum, true);
  bufMgr->unPinPage(file, numPageNew, true);
}




/**
 * Begin a filtered scan of the index.  For instance, if the method is called 
 * using ("a",GT,"d",LTE) then we should seek all entries with a value 
 * greater than "a" and less than or equal to "d".
 * If another scan is already executing, that needs to be ended here.
 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
 * @param lowVal  Low value of range, pointer to integer / double / char string
 * @param lowOp   Low operator (GT/GTE)
 * @param highVal High value of range, pointer to integer / double / char string
 * @param highOp  High operator (LT/LTE)
 * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
 * @throws  BadScanrangeException If lowVal > highval
 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
**/

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

		if (this->scanExecuting) {
			endScan();
		}
    this->currentPageNum = rootPageNum;
		bufMgr->readPage(this->file, rootPageNum, this->currentPageData);

		if (rootPageNum!=rootInitNum) {
			NonLeafNodeInt *currLeaf = reinterpret_cast<NonLeafNodeInt*>(currentPageData);
			bool leafFoundFlag = false;
      while(!leafFoundFlag){
        currLeaf = reinterpret_cast<NonLeafNodeInt*>(currentPageData);
        if(currLeaf->level == 1)
          leafFoundFlag = true;

        PageId nextPageNum;
        int i = nodeOccupancy;
         while(i >= 0 && (currLeaf->pageNoArray[i] == 0)){
          i--;
        }
        while(i > 0 && (currLeaf->keyArray[i-1] >= lowValInt)){
            i--;
        }
        nextPageNum = currLeaf->pageNoArray[i];
        
        bufMgr->unPinPage(file, currentPageNum, false);
        currentPageNum = nextPageNum;
        bufMgr->readPage(file, currentPageNum, currentPageData);
      }
		}
    bool flag = false;
    while(!flag){
      LeafNodeInt* currLeaf = (LeafNodeInt *) currentPageData;
      if(currLeaf->ridArray[0].page_number == 0){
        bufMgr->unPinPage(file, currentPageNum, false);
        throw NoSuchKeyFoundException();
      }
      bool checkNull = false;
      for(int i = 0; i < leafOccupancy and !checkNull; i++){
        int key = currLeaf->keyArray[i];
        if(currLeaf->ridArray[i + 1].page_number == 0 && i < leafOccupancy - 1 ){
          checkNull = true;
        }
        if(checkKey(lowValInt, highValInt, lowOp, highOp, key)){
          nextEntry = i;
          flag = true;
          scanExecuting = true;
          break;
        }
        else if((highOp == LT and key >= highValInt) || (highOp == LTE and key > highValInt)){
          bufMgr->unPinPage(file, currentPageNum, false);
          throw NoSuchKeyFoundException();
        }
        if(i == leafOccupancy - 1 || checkNull){
          bufMgr->unPinPage(file, currentPageNum, false);
          if(currLeaf->rightSibPageNo == 0){
            throw NoSuchKeyFoundException();
          }
          currentPageNum = currLeaf->rightSibPageNo;
          bufMgr->readPage(file, currentPageNum, currentPageData);
        }
      }
    }
	}

/**
  * Fetch the record id of the next index entry that matches the scan.
  * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
  * @param outRid RecordId of next record found that satisfies the scan criteria returned in this
  * @throws ScanNotInitializedException If no scan has been initialized.
  * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
**/
const void BTreeIndex::scanNext(RecordId& outRid) 
{
  if(!scanExecuting){
    throw ScanNotInitializedException();
  }
	LeafNodeInt* currLeaf = reinterpret_cast<LeafNodeInt*>(currentPageData);
  if(this->nextEntry == leafOccupancy || currLeaf->ridArray[nextEntry].page_number == 0 ){
    bufMgr->unPinPage(file, currentPageNum, false);
    if(currLeaf->rightSibPageNo == 0){
      throw IndexScanCompletedException();
    }
    this->currentPageNum = currLeaf->rightSibPageNo;
    bufMgr->readPage(file, currentPageNum, currentPageData);
    currLeaf = reinterpret_cast<LeafNodeInt*>(currentPageData);
    this->nextEntry = 0;
  }
  int k = currLeaf->keyArray[nextEntry];
  if(checkKey(lowValInt,highValInt, lowOp, highOp, k)){
    outRid = currLeaf->ridArray[nextEntry];
    this->nextEntry++;
  }
  else{
    throw IndexScanCompletedException();
  }
}

/**
  * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
  * @throws ScanNotInitializedException If no scan has been initialized.
**/
const void BTreeIndex::endScan() 
{
  if(scanExecuting == false){
    throw ScanNotInitializedException();
  }
  scanExecuting = false;
  bufMgr->unPinPage(file, currentPageNum, false);
  currentPageNum = static_cast<PageId>(-1);
  currentPageData = nullptr;  
  nextEntry = -1;
}

const bool BTreeIndex::checkKey(int lowVal, int highVal, const Operator lowOp,  const Operator highOp, int val){
  if(lowOp == GTE && highOp == LTE)
    return val <= highVal && val >= lowVal;
  else if(lowOp == GTE && highOp == LT)
    return val < highVal && val >= lowVal;
  else if(lowOp == GT && highOp == LTE)
    return val <= highVal && val > lowVal;  
  else
    return val < highVal && val > lowVal;
}

}