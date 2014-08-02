
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
	pfm = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	int returnValue = SUCCESS;

	if (pfm->fexist(fileName) ){
		return -1;
	}

	//create the file that will hold the b+ tree
	returnValue = pfm->createFile(fileName.c_str());

	if (returnValue == SUCCESS) {

		FileHandle fileHandle;
		returnValue = pfm->openFile(fileName.c_str(), fileHandle);

		if(returnValue != SUCCESS) {
            pfm->closeFile(fileHandle);
			return returnValue;
		}

		// header page to store root page num
		char * header = (char*) malloc(PAGE_SIZE);
		*((unsigned *) header) = 1;  //write out the root node page number
		
        returnValue = fileHandle.appendPage(header);
        if(returnValue != SUCCESS){
            pfm->closeFile(fileHandle);
            free(header);
            return returnValue;
        }

		free(header);

		// rootHeader: [pageType][numRecords][freeSpace][freeSpaceOffset][firstPtr]
		char * rootPage = (char *) malloc(PAGE_SIZE);
		IndexHeader *rootHeader = (IndexHeader *)rootPage;
		rootHeader->pageType = Index;
		rootHeader->numOfRecords = 0;
		rootHeader->freeSpace = PAGE_SIZE - sizeof(IndexHeader);
		rootHeader->freeSpaceOffset = PAGE_SIZE;
		rootHeader->firstPtr = 2;

		returnValue = fileHandle.appendPage(rootPage);
        if(returnValue != SUCCESS){
            pfm->closeFile(fileHandle);
            free(rootPage);
            return returnValue;
        }
        
		free(rootPage);


		// leafHeader: [pageType][numRecords][freeSpace][freeSpaceOffset][nextOFlow][nextPage][prevPage]
		char * firstLeafPage = (char*) malloc(PAGE_SIZE);
		LeafHeader *leafHeader = (LeafHeader *) firstLeafPage;
		leafHeader->pageType = Leaf;
		leafHeader->numOfRecords = 0;
		leafHeader->freeSpace = PAGE_SIZE - sizeof(LeafHeader);
		leafHeader->freeSpaceOffset = PAGE_SIZE;
		leafHeader->nextOverFlowPage = NO_PAGE;
		leafHeader->nextPage = NO_PAGE;
		leafHeader->prevPage = NO_PAGE;

		returnValue = fileHandle.appendPage(firstLeafPage);
        if(returnValue != SUCCESS) {
            pfm->closeFile(fileHandle);
            free(firstLeafPage);
            return returnValue;
        }

		free(firstLeafPage);

		returnValue = pfm->closeFile(fileHandle);

	}
	return returnValue;
}

RC IndexManager::destroyFile(const string &fileName)
{
	if (rootPageMap.find(fileName) != rootPageMap.end())
		return 2;

	return pfm->destroyFile(fileName.c_str());
}

RC IndexManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if (!pfm->fexist(fileName) ){
		return -1;
	}
    
	int returnValue = pfm->openFile(fileName.c_str(), fileHandle);

	if (returnValue != SUCCESS)
		return returnValue;

	if (rootPageMap.find(fileName) == rootPageMap.end()) {
		void *page = malloc(PAGE_SIZE);
		fileHandle.readPage(0, page);
		unsigned rootPageNum = *(unsigned *)page;
		rootPageMap[fileName] = rootPageNum;
		free(page);
	}

	return returnValue;
}

RC IndexManager::closeFile(FileHandle &fileHandle)
{
	string fileName = fileHandle.getFileName();

	int returnValue = pfm->closeFile(fileHandle);

	if (returnValue == SUCCESS && pfm->numOfFileHandle(fileName) == 0) {
		rootPageMap.erase(fileName);
	}

	return returnValue;
}



RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	int returnValue = SUCCESS;

	SplitInfo splitInfo;
	splitInfo.handleSplit = false;

	returnValue = insert(fileHandle, attribute, key, rid, rootPageMap[fileHandle.getFileName()], splitInfo);

	if (returnValue != SUCCESS) {
		return returnValue;
	}

	// root page has been splitted, create a new root page, change rootPageNum in map and header page
	if (splitInfo.handleSplit) {
		unsigned oldRootNumber = rootPageMap[fileHandle.getFileName()];

		void * newRootPage = malloc(PAGE_SIZE);

		IndexHeader * rootHeader = (IndexHeader *)newRootPage;
		rootHeader->pageType = Index;
		rootHeader->numOfRecords = 0;
		rootHeader->freeSpace = PAGE_SIZE - sizeof(IndexHeader);
		rootHeader->freeSpaceOffset = PAGE_SIZE;
		rootHeader->firstPtr = oldRootNumber;

		insertEntryInIndexPage((char*)newRootPage, splitInfo.key, rootHeader, attribute, splitInfo.pageNo);

		returnValue = fileHandle.appendPage(newRootPage);
		free(newRootPage);

		if(returnValue != SUCCESS) {
			return -1;
		}

		unsigned newRootNumber = fileHandle.getNumberOfPages() - 1;

		// change the root page number in headerPage
		void * headerPage = malloc(PAGE_SIZE);
		returnValue = fileHandle.readPage(0, headerPage);
		if(returnValue != SUCCESS) {
            free(splitInfo.key);
			free(headerPage);
			return -1;
		}

		*(unsigned *)headerPage = newRootNumber;

		returnValue = fileHandle.writePage(0, headerPage);
		if(returnValue != SUCCESS) {
            free(splitInfo.key);
			free(headerPage);
			return -1;
		}

		rootPageMap[fileHandle.getFileName()] = newRootNumber;

		free(headerPage);
		free(splitInfo.key);
	}


	return returnValue;
}


RC IndexManager::insert(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid, unsigned pageNo, SplitInfo &splitInfo) {
	int returnValue = SUCCESS;
	char * pageIn = (char *) malloc(PAGE_SIZE);
	returnValue = fileHandle.readPage(pageNo, pageIn);  //read the page num that was passed in (index or node to be processed)

	if (returnValue != SUCCESS) {
		free(pageIn);
		return -1;
	}

	PageType pageType;
	memcpy(&pageType, pageIn, sizeof(PageType));

	if (pageType == Root || pageType == Index) {
		IndexHeader * indexHeader = ((IndexHeader *)pageIn);
		int numberOfRecs = indexHeader->numOfRecords;
		short slotNum = indexBinarySearch(key, pageIn, numberOfRecs, attribute.type);

		unsigned nextNode;
		if (slotNum == -1)
			nextNode = indexHeader->firstPtr;
		else {
			IndexSlot * iSlot = goToIndexSlot(pageIn, slotNum);
			nextNode = iSlot->ptr;
		}

		returnValue = insert(fileHandle, attribute, key, rid, nextNode, splitInfo);
		if (returnValue != SUCCESS) {
			free(pageIn);
			return returnValue;
		}

		// split occurred
		if (splitInfo.handleSplit) {
			int keyLength = getKeyLength(splitInfo.key, attribute.type);
			short dataEntrySize = keyLength + sizeof(IndexSlot);

			// have enough space in this index page, insert and set splitInfo to "null"
			if (indexHeader->freeSpace >= dataEntrySize) {
				insertEntryInIndexPage(pageIn, splitInfo.key, indexHeader, attribute, splitInfo.pageNo);
				returnValue = fileHandle.writePage(pageNo, pageIn);

				if (returnValue != SUCCESS) {
                    free(splitInfo.key);
                    free(pageIn);
					return returnValue;
				}

				splitInfo.handleSplit = false;
				free(splitInfo.key);
			}
			else { // index page needs to be splitted

				// create the new index page
				char * newIndexPage = (char *) malloc(PAGE_SIZE);
				returnValue = fileHandle.appendPage(newIndexPage);
				if (returnValue != SUCCESS) {
                    free(splitInfo.key);
					free(pageIn);
					free(newIndexPage);
					return returnValue;
				}

				unsigned newPageNo = fileHandle.getNumberOfPages() - 1;

				// create back up SplitInfo, this SplitInfo will be passed one level up
				SplitInfo backUpSplitInfo;
				backUpSplitInfo.pageNo = newPageNo;
				// back up splitInfo will be set during the split of index page
				copyIndexEntriesInOrder(pageIn, newIndexPage, attribute.type, backUpSplitInfo);

				// prepare key entry used to compare with the key in splitInfo, to decide which index page to go
				void *keyEntry;
				int keyLength;
				if (attribute.type == TypeVarChar) {
					keyLength = *(int *)backUpSplitInfo.key;
					keyEntry = malloc(keyLength);
					memcpy(keyEntry, (char *)backUpSplitInfo.key + sizeof(int), keyLength);
				}
				else if (attribute.type == TypeInt){
					keyLength = sizeof(int);
					keyEntry = malloc(keyLength);
					memcpy(keyEntry, backUpSplitInfo.key, keyLength);
				}
				else {
					keyLength = sizeof(float);
					keyEntry = malloc(keyLength);
					memcpy(keyEntry, backUpSplitInfo.key, keyLength);
				}

				//  compare the key that needs to be copied up
				int compareVal = compare(splitInfo.key, keyEntry, attribute.type, keyLength);

				//  determine where the copied key needs to be inserted (existing index leaf or new index leaf)
				if (compareVal <= 0) {
					insertEntryInIndexPage(pageIn, splitInfo.key, indexHeader, attribute, splitInfo.pageNo);
				}
				else {
					IndexHeader * newIndexHeader = (IndexHeader *) newIndexPage;
					insertEntryInIndexPage(newIndexPage, splitInfo.key, newIndexHeader, attribute, splitInfo.pageNo);
				}

				// write both page
				returnValue = fileHandle.writePage(pageNo, pageIn);
				if (returnValue != SUCCESS) {
                    free(backUpSplitInfo.key);
                    free(splitInfo.key);
					free(pageIn);
					free(newIndexPage);
					free(keyEntry);
					return -1;
				}

				returnValue = fileHandle.writePage(newPageNo, newIndexPage);
				if (returnValue != SUCCESS) {
                    free(backUpSplitInfo.key);
                    free(splitInfo.key);
					free(pageIn);
					free(newIndexPage);
					free(keyEntry);
					return -1;
				}

				free(newIndexPage);
				free(keyEntry);

				// use the backup splitInfo to reset split info
				splitInfo.handleSplit = true;

				// delete the old key
				free(splitInfo.key);
				// copy the key from back up splitInfo
				if (attribute.type == TypeVarChar) {
					int keyLength = *(int *)backUpSplitInfo.key + sizeof(int);
					splitInfo.key = malloc(keyLength);
					memcpy(splitInfo.key, backUpSplitInfo.key, keyLength);
				}
				else if (attribute.type == TypeInt) {
					splitInfo.key = malloc(sizeof(int));
					memcpy(splitInfo.key, backUpSplitInfo.key, sizeof(int));
				}
				else {
					splitInfo.key = malloc(sizeof(float));
					memcpy(splitInfo.key, backUpSplitInfo.key, sizeof(float));
				}
				free(backUpSplitInfo.key);

				splitInfo.pageNo = newPageNo;
			}
		}
	}
	else   //PROCESS A LEAF PAGE
	{
		int keyLength = getKeyLength(key, attribute.type);
		short dataEntrySize = keyLength + sizeof(LeafSlot);

		LeafHeader * leafHeader = (LeafHeader *) pageIn;

		if (leafHeader->freeSpace >= dataEntrySize) { //there enough space in this leaf, we can insert the key directly

			// need to reorganize page
			if ((unsigned)(leafHeader->freeSpaceOffset - keyLength) <
					sizeof(LeafHeader) + (leafHeader->numOfRecords + 1) * sizeof(LeafSlot))
				reorgLeafPage(pageIn);

			insertKeyInLeafPage(pageIn, (char*)key, leafHeader, attribute, rid);
			returnValue = fileHandle.writePage(pageNo, pageIn);

			if (returnValue != SUCCESS) {
				free(pageIn);
				return returnValue;
			}

			splitInfo.handleSplit = false;
		}

		else {                                            //Need to split the current leaf and
			//  0. prepare new leaf page
			char * newLeafPage = (char *) malloc(PAGE_SIZE);
			returnValue = fileHandle.appendPage(newLeafPage);

			if(returnValue != SUCCESS) {
				free(newLeafPage);
				free(pageIn);
				return -1;
			}
			unsigned newPageNo = fileHandle.getNumberOfPages() - 1;

			//  1. copy all the keys starting from startslot from current leaf to new leaf
			copyLeafKeysInOrder(pageIn, newLeafPage, pageNo, newPageNo, attribute.type);

			//  2. now insert the original key into either the old leaf page or the new leaf page
			LeafSlot * ls = goToLeafSlot(newLeafPage, 0);

			void * keyOnPage = malloc(ls->length);
			memcpy(keyOnPage, newLeafPage + ls->offset, ls->length);

			//  4. compare the new key to the largest key on the existing leaf
			int compareVal = compare(key, keyOnPage, attribute.type, ls->length);

			//  5. insert the key into the correct leaf page (existing or new leaf page)
			if (compareVal < 0) {  //key entry can be inserted on the existing leaf
				returnValue = insertKeyInLeafPage(pageIn, key, leafHeader, attribute, rid);
				if (returnValue != SUCCESS) { // key already exist
					free(pageIn);
					free(newLeafPage);
					free(keyOnPage);
					return returnValue;
				}
			}
			else {  //insert key entry into the newly created leaf page
				LeafHeader * newLeafHeader = (LeafHeader*) newLeafPage;
				returnValue = insertKeyInLeafPage(newLeafPage, key, newLeafHeader, attribute, rid);
				if (returnValue != SUCCESS) { // key already exist
					free(pageIn);
					free(newLeafPage);
					free(keyOnPage);
					return returnValue;
				}
			}

			//Save Both Pages
			returnValue = fileHandle.writePage(newPageNo, newLeafPage);
			if(returnValue != SUCCESS) {
				free(newLeafPage);
				free(keyOnPage);
				free(pageIn);
				return returnValue;
			}

			returnValue = fileHandle.writePage(pageNo, pageIn);
			if(returnValue != SUCCESS) {
				free(newLeafPage);
				free(keyOnPage);
				free(pageIn);
				return returnValue;
			}

			// prepare splitInfo which will be return to the level above
			if (attribute.type == TypeVarChar) {
				splitInfo.key = malloc(sizeof(int) + ls->length);
				int length = (int)ls->length;
				memcpy(splitInfo.key, &length, sizeof(int));
				memcpy((char *)splitInfo.key + sizeof(int), keyOnPage, ls->length);
			}
			else {
				splitInfo.key = malloc(ls->length);
				memcpy(splitInfo.key, keyOnPage, ls->length);
			}

			splitInfo.pageNo = newPageNo;
			splitInfo.handleSplit = true;

			free(keyOnPage);
			free(newLeafPage);
		}
	}
	free(pageIn);
	return returnValue;
}




// key here contains four bytes to indicate the length of string
RC IndexManager::insertKeyInLeafPage(char *pageIn, const void *key, LeafHeader *leafHeader, const Attribute &attribute, const RID &rid) {

    int keyLength = getKeyLength(key, attribute.type);
    short dataEntrySize = sizeof(LeafSlot) + keyLength;

    void * keyEntry = malloc(keyLength);

    if (attribute.type == TypeVarChar) {
        memcpy(keyEntry, ((char*)key) + sizeof(int), keyLength);
    }
    else{
        memcpy(keyEntry, key, keyLength);
    }

    bool isEqual;
    short newSlotNum = leafBinarySearch(key, pageIn, leafHeader->numOfRecords, attribute.type, isEqual);
    newSlotNum++;  //leafBinarySearch returns the predecessor, we want the next slot

    if (isEqual) {
        //two keys with the same value, handle with OVERFLOW pages, temporarily just return error code
    	free(keyEntry);
    	return 3;
    }
    else {
        //get the old slot information
        short prevSlotsStart = sizeof(LeafHeader) + (sizeof(LeafSlot) * newSlotNum);
        short prevSlotsEnd =  sizeof(LeafHeader) + (sizeof(LeafSlot) * leafHeader->numOfRecords);
        short prevSlotsSize = prevSlotsEnd - prevSlotsStart;

        //1.  Save the existing slot information starting from insertion point
        if (prevSlotsSize > 0) {  //not inserting to the end of slots
            char * prevSlotsInfo = (char*) malloc(prevSlotsSize);
            memcpy(prevSlotsInfo, pageIn + prevSlotsStart, prevSlotsSize);  //copy the existing slot infomation to the left of new slot
            memcpy(pageIn + prevSlotsStart + sizeof(LeafSlot), prevSlotsInfo, prevSlotsSize);  //SHIFT the old slots down by 1 slot
            free(prevSlotsInfo);
        }

        //2.  Write the slot information for the new entry
        LeafSlot * lSlot = goToLeafSlot(pageIn, newSlotNum);  //go to the new slot and write the information
        lSlot->pageNum = rid.pageNum;
        lSlot->slotNum = rid.slotNum;
        short keyOffSetBegin = leafHeader->freeSpaceOffset - keyLength;
        lSlot->offset = keyOffSetBegin;
        lSlot->length = keyLength;

        //3.  Write the entry to the bottom of the page (determined by freespace offset)
        char * keyEntryStart = pageIn + keyOffSetBegin;

        //4.  Copy the key to the page
        memcpy(keyEntryStart, keyEntry, keyLength);

        //5.  Update the header with the new values
        leafHeader->numOfRecords++;
        leafHeader->freeSpace = leafHeader->freeSpace - dataEntrySize;
        leafHeader->freeSpaceOffset = leafHeader->freeSpaceOffset - keyLength;
    }
    free(keyEntry);
    return 0;
}

// key here contains four bytes to indicate the length of string
RC IndexManager::insertEntryInIndexPage(char *pageIn, const void *key, IndexHeader *indexHeader, const Attribute &attribute, unsigned &pagePointer) {

    int keyLength = getKeyLength(key, attribute.type);
    short dataEntrySize = sizeof(IndexSlot) + keyLength;

    void * keyEntry = malloc(keyLength);

    if (attribute.type == TypeVarChar) {
        memcpy(keyEntry, ((char *)key) + sizeof(int), keyLength);
    }
    else{
        memcpy(keyEntry, key, keyLength);
    }

    short newSlotNum = indexBinarySearch(key, pageIn, indexHeader->numOfRecords, attribute.type);
    newSlotNum++;  //indexBinarySearch returns the predecessor, we want the next slot


    //get the old slot information
    short prevSlotsStart = sizeof(IndexHeader) + (sizeof(IndexSlot) * newSlotNum);
    short prevSlotsEnd =  sizeof(IndexHeader) + (sizeof(IndexSlot) * indexHeader->numOfRecords);
    short prevSlotsSize = prevSlotsEnd - prevSlotsStart;

    //1.  Save the existing slot information starting from insertion point
    if (prevSlotsSize > 0) {  //not inserting to the end of slots
    	char * prevSlotsInfo = (char*) malloc(prevSlotsSize);
    	memcpy(prevSlotsInfo, pageIn + prevSlotsStart, prevSlotsSize);  //copy the existing slot infomation to the left of new slot
    	memcpy(pageIn + prevSlotsStart + sizeof(IndexSlot), prevSlotsInfo, prevSlotsSize);  //SHIFT the old slots down by 1 slot
    	free(prevSlotsInfo);
    }

    //2.  Write the slot information for the new entry
    IndexSlot * iSlot = goToIndexSlot(pageIn, newSlotNum);  //go to the new slot and write the information
    short keyOffSetBegin = indexHeader->freeSpaceOffset - keyLength;
    iSlot->offset = keyOffSetBegin;
    iSlot->length = keyLength;
    iSlot->ptr = pagePointer;

    //3.  Write the entry to the bottom of the page (determined by freespace offset)
    char * keyEntryStart = pageIn + keyOffSetBegin;

    //4.  Copy the key to the page
    memcpy(keyEntryStart, keyEntry, keyLength);

    //5.  Update the header with the new values
    indexHeader->numOfRecords++;
    indexHeader->freeSpace = indexHeader->freeSpace - dataEntrySize;
    indexHeader->freeSpaceOffset = indexHeader->freeSpaceOffset - keyLength;

    free(keyEntry);
    return 0;
}


/**
 *  When a split on a leaf occurs, we need to copy all values from a starting point in the current page, to the new page.  This
 *  method handles that.
 **/
void IndexManager::copyLeafKeysInOrder(char * leafPage, char * newLeafPage, unsigned currentPageNo, unsigned newPageNo, AttrType type) {

	LeafHeader * leafHeader = (LeafHeader *)leafPage;

	//create the leaf header for the new child
	LeafHeader * newLeafHeader = (LeafHeader *)newLeafPage;
	newLeafHeader->pageType = Leaf;
	newLeafHeader->numOfRecords = 0;
	newLeafHeader->nextOverFlowPage = NO_PAGE;
	newLeafHeader->nextPage = leafHeader->nextPage;
	newLeafHeader->prevPage = currentPageNo;
	newLeafHeader->freeSpace = PAGE_SIZE - sizeof(LeafHeader);
	newLeafHeader->freeSpaceOffset = PAGE_SIZE;

	leafHeader->nextPage = newPageNo;

	short numberOfRecords = leafHeader->numOfRecords;
	short startSlot = numberOfRecords / 2;

	short i = startSlot;
	short j = 0;

	// copy the bigger half of the page to new page
	for(; i < numberOfRecords; i++, j++){
		LeafSlot * leafSlot = goToLeafSlot(leafPage, i);
		LeafSlot * newLeafSlot = goToLeafSlot(newLeafPage, j);

		// copy information in slot
		newLeafSlot->length = leafSlot->length;
		newLeafSlot->offset = newLeafHeader->freeSpaceOffset - newLeafSlot->length; // should make some changes
		newLeafSlot->pageNum = leafSlot->pageNum;
		newLeafSlot->slotNum = leafSlot->slotNum;

		// copy key value
		memcpy(newLeafPage + newLeafSlot->offset, leafPage + leafSlot->offset, leafSlot->length);

		// change info in header
		newLeafHeader->numOfRecords++;
		newLeafHeader->freeSpace -= sizeof(LeafSlot) + newLeafSlot->length;
		newLeafHeader->freeSpaceOffset -= newLeafSlot->length;
	}

	// delete the bigger half of the old page
	leafHeader->numOfRecords -= numberOfRecords - startSlot;
	// reorganize the page to release free space
	reorgLeafPage(leafPage);

}

/**
 *  When a split on a index page occurs, we need to copy all values from a starting point in the current page, to the new page.  This
 *  method handles that.
 **/
void IndexManager::copyIndexEntriesInOrder(char * indexPage, char * newIndexPage, AttrType attrType, SplitInfo &splitInfo) {

	IndexHeader * indexHeader = (IndexHeader *)indexPage;

    IndexHeader * newIndexHeader = (IndexHeader *)newIndexPage;
    newIndexHeader->pageType = Index;
    newIndexHeader->numOfRecords = 0;
    newIndexHeader->freeSpace = PAGE_SIZE - sizeof(IndexHeader);
    newIndexHeader->freeSpaceOffset = PAGE_SIZE;

    short numOfRecords = indexHeader->numOfRecords;
    short startSlot = numOfRecords / 2;



    // set the first pointer in the new index page to the pointer of middle slot
    IndexSlot *middleSlot = goToIndexSlot(indexPage, startSlot);
    newIndexHeader->firstPtr = middleSlot->ptr;

    splitInfo.handleSplit = true;

    // set the key in splitInfo
    if (attrType == TypeInt) {
    	splitInfo.key = malloc(sizeof(int));
    	memcpy(splitInfo.key, indexPage + middleSlot->offset, sizeof(int));
    }
    else if (attrType == TypeReal) {
    	splitInfo.key = malloc(sizeof(float));
    	memcpy(splitInfo.key, indexPage + middleSlot->offset, sizeof(float));
    }
    else {
    	splitInfo.key = malloc(sizeof(int) + middleSlot->length);
    	int length = (int)middleSlot->length;
    	memcpy(splitInfo.key, &length, sizeof(int));
    	memcpy((char *)splitInfo.key + sizeof(int), indexPage + middleSlot->offset, middleSlot->length);
    }


    int i = startSlot + 1;
    int j = 0;

    for(; i < numOfRecords; i++, j++){
        IndexSlot * indexSlot = goToIndexSlot(indexPage, i);
        IndexSlot * newIndexSlot = goToIndexSlot(newIndexPage, j);

        newIndexSlot->length = indexSlot->length;
        newIndexSlot->offset = newIndexHeader->freeSpaceOffset - indexSlot->length;
        newIndexSlot->ptr = indexSlot->ptr;

        memcpy(newIndexPage + newIndexSlot->offset, indexPage + indexSlot->offset, indexSlot->length);

        newIndexHeader->numOfRecords++;
        newIndexHeader->freeSpaceOffset -= newIndexSlot->length;
        newIndexHeader->freeSpace -= sizeof(IndexSlot) + indexSlot->length;
    }

    indexHeader->numOfRecords -= numOfRecords - startSlot;
    reorgIndexPage(indexPage);

}


int IndexManager::getKeyLength(const void *key, AttrType attrType) {
	if (attrType == TypeInt)
		return sizeof(int);
	else if (attrType == TypeReal)
		return sizeof(float);
	else
		return *(int *)key;
}


RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	int returnValue = SUCCESS;
	RID recordId;
	EID entryId;

	// find the entry by key
	returnValue = searchEntry(fileHandle, attribute, key, recordId, entryId);
	if (returnValue != SUCCESS) {
		return returnValue;
	}

	// search result doesn't match
	if (recordId.pageNum != rid.pageNum || recordId.slotNum != rid.slotNum)
		return 3;

	char *page = (char *)malloc(PAGE_SIZE);
	returnValue = fileHandle.readPage(entryId.pageNum, page);
	if (returnValue != SUCCESS) {
		free(page);
		return returnValue;
	}

	LeafHeader *headerPtr = (LeafHeader *)page;
	short numOfRecords = headerPtr->numOfRecords;
	LeafSlot *deletedSlot = goToLeafSlot(page, entryId.slotNum);
	short keyLength = deletedSlot->length;

	// shift the all data entries behind entryId left by one
	for (short i = entryId.slotNum; i < numOfRecords - 1; i++) {
		char *slotPtr = page + sizeof(LeafHeader) + i * sizeof(LeafSlot);
		char *copiedSlotPtr = page + sizeof(LeafHeader) + (i + 1) * sizeof(LeafSlot);
		memcpy(slotPtr, copiedSlotPtr, sizeof(LeafSlot));
	}

	headerPtr->numOfRecords--;
	headerPtr->freeSpace += keyLength + sizeof(LeafSlot);

	returnValue = fileHandle.writePage(entryId.pageNum, page);
	free(page);

	return returnValue;
}

RC IndexManager::searchEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, RID &rid, EID &entryId) {
	int returnValue = 0;

	bool isSuccess = false;
	bool isNegOne = false;
	LeafSlot *slotPtr = BTreeSearch(fileHandle, rootPageMap[fileHandle.getFileName()], attribute.type, key, entryId, isSuccess, isNegOne);

	if (isSuccess) { // successful search
		rid.pageNum = slotPtr->pageNum;
		rid.slotNum = slotPtr->slotNum;
	}
	else {
		returnValue = 1;
	}

	delete slotPtr;
	return returnValue;
}

LeafSlot * IndexManager::BTreeSearch(FileHandle &fileHandle, unsigned pageNum, AttrType attrType, const void *key, EID &entryId, bool &isSuccess, bool &isNegOne) {

	void *page = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum, page);

	LeafSlot *result;

	if (*(PageType *)page == Leaf || *(PageType *)page == Overflow) { // leaf page
		LeafHeader *header = (LeafHeader *)page;
		short numOfRecords = header->numOfRecords;

		short slotNum = leafBinarySearch(key, page, numOfRecords, attrType, isSuccess);

		result = new LeafSlot();

		// isNegOne is only set to true when there is at least one data entry and the key is smaller than all data entries
		if (slotNum == -1) {
			slotNum++;
			isNegOne = numOfRecords == 0 ? false : true;
		}
		else
			isNegOne = false;

		// has at least one one slot
		if (numOfRecords > 0) {
			LeafSlot *slotPtr = goToLeafSlot((char *)page, slotNum);
			result->length = slotPtr->length;
			result->offset = slotPtr->offset;
			result->pageNum = slotPtr->pageNum;
			result->slotNum = slotPtr->slotNum;
		}
		else {
			result->length = 0;
			result->offset = 0;
			result->pageNum = 0;
			result->slotNum = 0;
		}

		entryId.pageNum = pageNum;
		entryId.slotNum = slotNum;
	}
	if (*(PageType *)page == Root || *(PageType *)page == Index) { // index page
		IndexHeader *header = (IndexHeader *)page;
		short numOfRecords = header->numOfRecords;
		short slotNum = indexBinarySearch(key, page, numOfRecords, attrType);

		if (slotNum == -1) {
			result = BTreeSearch(fileHandle, header->firstPtr, attrType, key, entryId, isSuccess, isNegOne);
		}
		else {
			IndexSlot *slotPtr = goToIndexSlot((char *)page, slotNum);
			result = BTreeSearch(fileHandle, slotPtr->ptr, attrType, key, entryId, isSuccess, isNegOne);
		}
	}

	free(page);
	return result;
}

/**
 * this is a helper method used in scan initialization. if isInclusive is true, rid will be set to the smallest data
 * entry which is greater than or equal to key. if isInclusive is false, rid will be set to the smallest data entry
 * which is greater than key
 */

RC IndexManager::findSuccessor(FileHandle &fileHandle, const void *key, AttrType type, EID &entryId, bool isInclusive) {
	int returnValue = 0;
	LeafSlot *slotPtr;
	bool isSuccess = false;
	bool isNegOne = false;

	slotPtr = BTreeSearch(fileHandle, rootPageMap[fileHandle.getFileName()], type, key, entryId, isSuccess, isNegOne);


	if (isSuccess && !isInclusive) {
		returnValue = findNextValidSlot(fileHandle, entryId);
	}

	if (!isSuccess && !isNegOne) {
		returnValue = findNextValidSlot(fileHandle, entryId);
	}

	delete slotPtr;
	return returnValue;
}

RC IndexManager::findNextValidSlot(FileHandle &fileHandle, EID &entryId) {
	int returnValue = SUCCESS;
	entryId.slotNum++;

	char *page = (char *)malloc(PAGE_SIZE);
	returnValue = fileHandle.readPage(entryId.pageNum, page);

	if (returnValue != SUCCESS) {
		free(page);
		return returnValue;
	}

	LeafHeader *headerPtr = (LeafHeader *)page;

	// all slots in this page has been read, find next page which has non zero records
	// if no such pages exists, set rid.pageNum to 0 and rid.slotNum to 0
	if (entryId.slotNum >= (unsigned)headerPtr->numOfRecords) {
		entryId.slotNum = 0;
		unsigned nextPage = headerPtr->nextPage;

		if (nextPage == NO_PAGE) {
			entryId.pageNum = NO_PAGE;
		}
		else {
			do {
				entryId.pageNum = nextPage;
				returnValue = fileHandle.readPage(entryId.pageNum, page);

				if (returnValue != 0) {
					free(page);
					return returnValue;
				}

				headerPtr = (LeafHeader *)page;
				nextPage = headerPtr->nextPage;
			}
			while(headerPtr->numOfRecords == 0 && headerPtr->nextPage != NO_PAGE);

			if (headerPtr->numOfRecords == 0 && headerPtr->nextPage == NO_PAGE)
				entryId.pageNum = NO_PAGE;
		}
	}

	free(page);
	return returnValue;
}


int IndexManager::compare(const void *key, const void *data, AttrType attrType, int dataLength) {
	// integer
	if (attrType == TypeInt) {
		int k = *(int *)key;
		int v = *(int *)data;
		return k - v;
	}
	// real
	if (attrType == TypeReal) {
		float k = *(float *)key;
		float v = *(float *)data;

		if (k - v > 0.00001)
			return 1;
		else if (k - v < -0.00001)
			return -1;
		else
			return 0;
	}
	// var char
	if (attrType == TypeVarChar) {
		int stringLength;
		memcpy(&stringLength, key, sizeof(int));

		string k((char *)key + sizeof(int), stringLength);
		string v((char *)data, dataLength);

		return k.compare(v);
	}

	return -1;
}


short IndexManager::indexBinarySearch(const void *key, void *page, short numOfRecords, AttrType attrType) {
	short low = 0;
	short high = numOfRecords - 1;
	IndexSlot *midSlot;

	while (low <= high) {
		short mid = (low + high) / 2;

		midSlot = goToIndexSlot((char *)page, mid);
		int length = midSlot->length;

		void *data = malloc(length);
		memcpy(data, (char *)page + midSlot->offset, length);

		int result = compare(key, data, attrType, length);

		if (result == 0) {
			free(data);
			return mid;
		}
		else if (result < 0) {
			high = mid - 1;
		}
		else {
			low = mid + 1;
		}

		free(data);
	}

	return high;
}


short IndexManager::leafBinarySearch(const void *key, void *page, short numOfRecords, AttrType attrType, bool &isEqual) {
	short low = 0;
	short high = numOfRecords - 1;
	LeafSlot *midSlot;

	while (low <= high) {
		short mid = (low + high) / 2;

		midSlot = goToLeafSlot((char *)page, mid);
		int length = midSlot->length;

		void *data = malloc(length);
		memcpy(data, (char *)page + midSlot->offset, length);

		int result = compare(key, data, attrType, length);

		if (result == 0) {
			isEqual = true;
			free(data);
			return mid;
		}
		else if (result < 0) {
			high = mid - 1;
		}
		else {
			low = mid + 1;
		}

		free(data);
	}

	isEqual = false;
	return high;
}


void IndexManager::reorgLeafPage(void *page) {
	char *copyPage = (char *)malloc(PAGE_SIZE);
	memcpy(copyPage, page, PAGE_SIZE);

	LeafHeader *headerPtr = (LeafHeader *)page;
	LeafHeader *copyHeaderPtr = (LeafHeader *)copyPage;

	short numOfRecords = headerPtr->numOfRecords;

	LeafSlot *slotPtr = goToLeafSlot((char *)page, 0);
	LeafSlot *copySlotPtr = goToLeafSlot(copyPage, 0);
	short offset = PAGE_SIZE;
	short freeSpace = PAGE_SIZE - sizeof(LeafHeader);

	for (short i = 0; i < numOfRecords; i++) {
		// decrement the free space offset
		offset -= slotPtr->length;
		// copy key
		memcpy(copyPage + offset, (char *)page + slotPtr->offset, slotPtr->length);
		// change the start offset
		copySlotPtr->offset = offset;
		// update free space
		freeSpace -= slotPtr->length + sizeof(LeafSlot);

		// increment the slot pointer
		slotPtr++;
		copySlotPtr++;
	}

	copyHeaderPtr->freeSpaceOffset = offset;
	copyHeaderPtr->freeSpace = freeSpace;

	memcpy(page, copyPage, PAGE_SIZE);

	free(copyPage);
}

void IndexManager::reorgIndexPage(void *page) {
	char *copyPage = (char *)malloc(PAGE_SIZE);
	memcpy(copyPage, page, PAGE_SIZE);

	IndexHeader *headerPtr = (IndexHeader *)page;
	IndexHeader *copyHeaderPtr = (IndexHeader *)copyPage;

	short numOfRecords = headerPtr->numOfRecords;

	IndexSlot *slotPtr = goToIndexSlot((char *)page, 0);
	IndexSlot *copySlotPtr = goToIndexSlot(copyPage, 0);
	short offset = PAGE_SIZE;
	short freeSpace = PAGE_SIZE - sizeof(LeafHeader);

	for (short i = 0; i < numOfRecords; i++) {
		// decrement the free space offset
		offset -= slotPtr->length;
		// copy key
		memcpy(copyPage + offset, (char *)page + slotPtr->offset, slotPtr->length);
		// change the start offset
		copySlotPtr->offset = offset;
		// update free space
		freeSpace -= slotPtr->length + sizeof(IndexSlot);

		// increment the slot pointer
		slotPtr++;
		copySlotPtr++;
	}

	copyHeaderPtr->freeSpaceOffset = offset;
	copyHeaderPtr->freeSpace = freeSpace;

	memcpy(page, copyPage, PAGE_SIZE);

	free(copyPage);
}


RC IndexManager::scan(FileHandle &fileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
    if(fileHandle.getFile() == NULL) {
        return -1;
    }
    
	int returnValue = 0;
	EID startEid;
	EID endEid;

	/**
	 * data entries scanned should be >= startRid && < endRid
	 * which means startRid will be set to the first qualified eid
	 * and endRid will be set to one eid after the last qualified eid
	 *
	 */

	// find the starting data entry
	if (lowKey == NULL) {
		startEid.pageNum = LEFT_MOST_PAGE_NUM;
		startEid.slotNum = -1;

		returnValue = findNextValidSlot(fileHandle, startEid);
	}
	else {
		returnValue = findSuccessor(fileHandle, lowKey, attribute.type, startEid, lowKeyInclusive);
	}

	if (returnValue != 0)
		return returnValue;

	// find the ending data entry
	if (highKey == NULL) {
		endEid.pageNum = NO_PAGE;
		endEid.slotNum = 0;
	}
	else {
		returnValue = findSuccessor(fileHandle, highKey, attribute.type, endEid, false);
	}

	if (returnValue != 0)
		return returnValue;

	return ix_ScanIterator.initialize(fileHandle, startEid, endEid, attribute.type);
}

IX_ScanIterator::IX_ScanIterator() : attrType(TypeInt)
{
	page = (char *)malloc(PAGE_SIZE);
	headerPtr = (LeafHeader *)page;
}

IX_ScanIterator::~IX_ScanIterator()
{
	free(page);
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	int returnValue = 0;

	// check to see if reaching the end of file
	if (currentEid.pageNum == NO_PAGE)
		return IX_EOF;

	if (currentEid.pageNum == stopEid.pageNum && currentEid.slotNum == stopEid.slotNum)
		return IX_EOF;

	// read data entry
	LeafSlot *slotPtr = (LeafSlot *)(page + sizeof(LeafHeader) + currentEid.slotNum * sizeof(LeafSlot));
	short offset = slotPtr->offset;
	short length = slotPtr->length;

	if (attrType == TypeInt) {
		memcpy(key, page + offset, length);
	}
	if (attrType == TypeReal) {
		memcpy(key, page + offset, length);
	}
	if (attrType == TypeVarChar) {
		int stringLength = length;
		memcpy(key, &stringLength, sizeof(int));
		memcpy((char *)key + sizeof(int), page + offset, stringLength);
	}

	rid.pageNum = slotPtr->pageNum;
	rid.slotNum = slotPtr->slotNum;

	// move currentPtr to next valid position, update void *page and headerPtr
	currentEid.slotNum++;
	if (currentEid.slotNum >= (unsigned)headerPtr->numOfRecords) {
		currentEid.slotNum = 0;
		unsigned nextPage = headerPtr->nextPage;

		// a "real" page
		if (nextPage != NO_PAGE) {
			// find the first page which has nonzero data entry
			do {
				currentEid.pageNum = nextPage;
				returnValue = fileHandle.readPage(currentEid.pageNum, page);

				if (returnValue != 0)
					return returnValue;

				headerPtr = (LeafHeader *)page;
				nextPage = headerPtr->nextPage;
			}
			while (headerPtr->numOfRecords == 0 && nextPage != NO_PAGE);

			// if all the following pages have no data entries, reaches the end of the file
			if (headerPtr->numOfRecords == 0 && nextPage == NO_PAGE)
				currentEid.pageNum = NO_PAGE;
		}
		else {
			currentEid.pageNum = NO_PAGE;
		}
	}

	return returnValue;
}

RC IX_ScanIterator::close()
{
	currentEid.pageNum = NO_PAGE;
	currentEid.slotNum = 0;
	stopEid.pageNum = NO_PAGE;
	stopEid.slotNum = 0;
	attrType = TypeInt;

	return 0;
}

RC IX_ScanIterator::initialize(FileHandle &fileHandle, const EID &startEid, const EID &endEid, AttrType type) {
	currentEid.pageNum = startEid.pageNum;
	currentEid.slotNum = startEid.slotNum;
	stopEid.pageNum = endEid.pageNum;
	stopEid.slotNum = endEid.slotNum;
	attrType = type;

	this->fileHandle = fileHandle;
	int returnValue = fileHandle.readPage(currentEid.pageNum, page);

	if (returnValue == 0) {
		headerPtr = (LeafHeader *)page;
	}

	return returnValue;
}

void IX_PrintError (RC rc)
{
	switch (rc) {
	case 0: cout << "Success!" << endl; break;
	case 1: cout << "Entry Not Found!" << endl; break;
	case 2: cout << "Fail to destroy file, some other FileHandle is handling this file!" << endl; break;
	case 3: cout << "Wrong RID in Delete Operation! " << endl; break;
	case 4: cout << "Key has already exists!" << endl;break;
	default: cout << "PFM error!" << endl; break;
	}
}
