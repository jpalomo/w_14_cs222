
#include "../rbf/rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
	for (map<string, vector<short> * >::iterator it = filePageDirectory.begin(); it != filePageDirectory.end(); ++it) {
		delete it->second;
	}

	pfm = NULL;
	_rbf_manager = NULL;
}

/**
 * Method creates a file named in the argument.  The method is responsible for constructing the new file
 * through the PagedFileManager, but also for creating meta file associated with this file.
 */
RC RecordBasedFileManager::createFile(const string &fileName) {

	int returnValue = pfm->createFile(fileName.c_str()); //create the file for the relation
    
	if (returnValue == 0) {
		returnValue = pfm->createFile(("meta_" + fileName).c_str());  //create the meta file for the relation
	}

	FileHandle metaFileHandle;

	if (returnValue == 0) {
		returnValue = pfm->openFile(("meta_" + fileName).c_str(), metaFileHandle);
	}

	if (returnValue == 0) {

		void *page = malloc(PAGE_SIZE);
		*(short *)page = 0; // number of pages

		returnValue = metaFileHandle.appendPage(page);

		if (returnValue == 0) {
			returnValue = pfm->closeFile(metaFileHandle);
		}

		free(page);
	}

	return returnValue;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    int returnValue = pfm->destroyFile(fileName.c_str());

    if (returnValue == 0) {
    	returnValue = pfm->destroyFile(("meta_" + fileName).c_str());
    }

    if (returnValue == 0) { // if successfully destroy file through pfm
    	filePageDirectory.erase(fileName);
    }

    //otherwise some other fileHandle may be open, cannot destroy the file
    return returnValue;
}

/**
 * This method opens the file indicated by fileName in the arguments.  Once the file is open and a handle is
 * retrieved, the meta file is read which contains the page sizes within the file and these sizes are loaded
 * into a vector.  Once the header file is read and vector is loaded, the fileName along with its vector of
 * page sizes is loaded to the map.
 *
 * Format of meta file starting from byte 0:  [short numPagesInFile][short page 0 free size][short page 1 free size]...
 */
RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	
    int returnValue = pfm->openFile(fileName.c_str(), fileHandle);

	if (returnValue == 0) { //successful file open
		if (filePageDirectory.find(fileName) == filePageDirectory.end()) {  //filePageDirectory doesn't have an entry for this file
			// open meta file
			FileHandle metaFileHandle;
			returnValue = pfm->openFile(("meta_" + fileName).c_str(), metaFileHandle);

			if (returnValue != 0)
				return returnValue;

			vector<short> * spaceLeft = new vector<short>();

			for (unsigned currentHeaderPage = 0; currentHeaderPage < metaFileHandle.getNumberOfPages(); currentHeaderPage++) {
				returnValue = readHeaderPage(metaFileHandle, currentHeaderPage, spaceLeft);
				if (returnValue != 0)
					return returnValue;
			}

			returnValue = pfm->closeFile(metaFileHandle);

			filePageDirectory[fileName] = spaceLeft; //add the file/pageSize entry to the filePageDirectory map
		}
	}

	return returnValue;
}

/**
 * This method close the file handled by fileHandle, the pageSize vector is written back to its associated meta file
 */
RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	int returnValue = -1;

	if (fileHandle.getFile() == NULL || filePageDirectory.find(fileHandle.getFileName()) == filePageDirectory.end()) {
		return returnValue;
	}

	vector<short> * spaceLeft = filePageDirectory[fileHandle.getFileName()];
	unsigned currentPage = 0;
	unsigned numOfPages = (int)spaceLeft->size();
	unsigned numOfHeaderPages = numOfPages / HEADER_PAGE_SLOT; // num of pages needed to store information in space left vector
	if (numOfPages % HEADER_PAGE_SLOT != 0)
		numOfHeaderPages++;


	FileHandle metaFileHandle;
	pfm->openFile(("meta_" + fileHandle.getFileName()).c_str(), metaFileHandle);

	// append extra pages to hold all information in space left vector
	char *page = (char *)malloc(PAGE_SIZE);
	while(metaFileHandle.getNumberOfPages() < numOfHeaderPages)
		metaFileHandle.appendPage(page);
	free(page);

	for (unsigned i = 0; i < numOfHeaderPages; i++) {
		returnValue = writeHeaderPage(metaFileHandle, i, spaceLeft, currentPage);

		if (returnValue != 0)
			return returnValue;
	}

	pfm->closeFile(metaFileHandle);

	// if is the only one fileHandle handling this particular file, remove file entry from file page directory;
	if (pfm->numOfFileHandle(fileHandle.getFileName()) == 1) {
		delete spaceLeft;
		filePageDirectory.erase(fileHandle.getFileName());
	}

	returnValue = pfm->closeFile(fileHandle);
	return returnValue;
}



RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	int returnValue = -1;
	//ensure we have a valid file handle
	if(fileHandle.getFile() == NULL) {
		return returnValue;
	}
	//make sure the file entry exists in the directory
	if(filePageDirectory.find(fileHandle.getFileName()) == filePageDirectory.end()) {
		return returnValue;
	}

	short recordLength = getRecordLength(recordDescriptor, data);
	void *record = malloc(recordLength);
	encodeRecord(recordDescriptor, data, record); // translate record into our format

	vector<short> * spaceLeftVect = filePageDirectory[fileHandle.getFileName()];

	char *page = (char *)malloc(PAGE_SIZE);  //create buffer to hold the file's page

	unsigned pageNum = 0;
	for(; pageNum < spaceLeftVect->size(); pageNum++) {
		//find the first page that has enough space to hold the record
		if (recordLength <= (*spaceLeftVect)[pageNum]) {
			fileHandle.readPage(pageNum, page);
			const char *endOfPagePtr = page + PAGE_SIZE;
			Footer *footerPtr = goToFooter(endOfPagePtr);
			short numOfSlots = footerPtr->numOfSlots;

			// find slot which is deleted and has not been reorganized
			unsigned slotNum = 1;
			Slot *slotPtr = goToSlot(endOfPagePtr, 1);
			for (; slotNum <= (unsigned)numOfSlots; slotNum++, slotPtr--) {
				if (slotPtr->beginAddr < 0) {
					// calculate the begin address
					short oriRecordBeginAddr = -slotPtr->beginAddr - 1;
					// calculate the capacity of this slot
					short oriRecordLength = slotPtr->endAddr - oriRecordBeginAddr;

					if (oriRecordLength >= recordLength) {
						memcpy(page + oriRecordBeginAddr, record, recordLength);
						slotPtr->beginAddr = oriRecordBeginAddr;
						slotPtr->endAddr = oriRecordBeginAddr + recordLength;

						(*spaceLeftVect)[pageNum] -= recordLength;
						returnValue = fileHandle.writePage(pageNum, page);

						if (returnValue == 0) {
							rid.pageNum = pageNum;
							rid.slotNum = slotNum;
						}

						free(record);
						free(page);
						return returnValue;
					}
				}
			}

			// find slot which has been reorganized
			slotNum = 1;
			slotPtr = goToSlot(endOfPagePtr, 1);
			for (; slotNum <= (unsigned)numOfSlots; slotNum++, slotPtr--) {
				if (slotPtr->beginAddr == 0 && slotPtr->endAddr == 0) {
					// calculate the capacity of free space zone
					short freeSpaceLeft = PAGE_SIZE - footerPtr->freeSpaceOffset - FOOTER_OVERHEAD - RECORD_OVERHEAD * footerPtr->numOfSlots;
					// if the capacity is not enough to hold the record, reorganize page
					if (freeSpaceLeft < recordLength) {
						reorganizePage(fileHandle, recordDescriptor, pageNum);
						fileHandle.readPage(pageNum, page); // reload page
					}

					// store record in free space zone
					appendRecord(page, record, recordLength, slotNum);
					(*spaceLeftVect)[pageNum] -= recordLength;

					returnValue = fileHandle.writePage(pageNum, page);
					if (returnValue == 0) {
						rid.pageNum = pageNum;
						rid.slotNum = slotNum;
					}

					free(record);
					free(page);
					return returnValue;
				}
			}

			// end of tow for loop, no available existing slot is found
			// create a new slot, append record to free space zone
			// if free space zone is not large enough to hold new record, reorganize page
			short freeSpaceLeft = PAGE_SIZE - footerPtr->freeSpaceOffset - FOOTER_OVERHEAD - RECORD_OVERHEAD * (footerPtr->numOfSlots + 1);
			if (freeSpaceLeft < recordLength) {
				reorganizePage(fileHandle, recordDescriptor, pageNum);
				fileHandle.readPage(pageNum, page); // reload page
			}

			// store record in free space zone
			appendRecord(page, record, recordLength, slotNum);

			(*spaceLeftVect)[pageNum] -= recordLength + RECORD_OVERHEAD;
			returnValue = fileHandle.writePage(pageNum, page);
			if (returnValue == 0) {
				rid.pageNum = pageNum;
				rid.slotNum = slotNum;
			}

			free(record);
			free(page);
			return returnValue;
		}
	}

	// all pages were in vector did not have enough space to write the current record
	// append a new page and write the record in the new page
	returnValue = appendPageWithOneRecord(fileHandle, record, recordLength);
	if(returnValue == 0) {
		rid.pageNum = pageNum;
		rid.slotNum = 1;
	}

	free(record);
	free(page);
	return returnValue;
}


RC RecordBasedFileManager::appendPageWithOneRecord(FileHandle &fileHandle, const void *data, int recordLength) {
	void *page = malloc(PAGE_SIZE);

	// put record at beginning and create the footer slot information
	prepareDataForNewPageWrite(data, page, recordLength);

	// append the newly created page to the file
	int returnValue = fileHandle.appendPage(page);

	if(returnValue == 0) {
		vector<short> * pageSizeVector = filePageDirectory[fileHandle.getFileName()];  //add the page to the directory
		pageSizeVector->push_back(PAGE_SIZE - recordLength - FOOTER_OVERHEAD - RECORD_OVERHEAD * 2);  //update the available bytes of the page
	}

	free(page);
	return returnValue;
}

RC RecordBasedFileManager::prepareDataForNewPageWrite(const void *record, void *page, int recordLength){
	memcpy(page, record, recordLength);  //write the record to the beginning of the file

	char *endOfPagePtr = (char *)page + PAGE_SIZE;

	// initialize footer
	Footer *footerPtr = goToFooter(endOfPagePtr);
	footerPtr->reOrg = 0; // reOrg flag
	footerPtr->numOfSlots = 1; // number of records
	footerPtr->freeSpaceOffset = recordLength; // free space pointer

	// set slot information
	Slot *slotPtr = goToSlot(endOfPagePtr, 1);
	slotPtr->beginAddr = 0;
	slotPtr->endAddr = recordLength;

	return 0;
}

/**
 * this is a helper method which write the record in the free space zone of a page
 * update the begin and end address stored slot, update the footer
 */
RC RecordBasedFileManager::appendRecord(char *page, const void *record, short recordLength, unsigned slotNum) {
	const char *endOfPagePtr = page + PAGE_SIZE;
	Footer *footerPtr = goToFooter(endOfPagePtr);

	// wrong slotNum
	if (slotNum > (unsigned)footerPtr->numOfSlots + 1)
			return -1;

	Slot *slotPtr = goToSlot(endOfPagePtr, slotNum);
	short freeSpaceOffset = footerPtr->freeSpaceOffset;
	memcpy(page + freeSpaceOffset, record, recordLength);

	slotPtr->beginAddr = freeSpaceOffset;
	slotPtr->endAddr = freeSpaceOffset + recordLength;

	footerPtr->freeSpaceOffset = slotPtr->endAddr;

	if (slotNum > (unsigned)footerPtr->numOfSlots) {
			footerPtr->numOfSlots++;
	}

	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
	int returnValue = -1;
	if(fileHandle.getFile() == NULL) {
		return returnValue;
	}
    
	if(filePageDirectory.find(fileHandle.getFileName()) == filePageDirectory.end()) {
		return returnValue;
	}
    
	vector<short> *spaceLeftVect = filePageDirectory[fileHandle.getFileName()];
    
	unsigned pageNum = rid.pageNum;
	unsigned slotNum = rid.slotNum;
	unsigned oriPageNum = rid.pageNum;
    
	short updatedRecordLength = getRecordLength(recordDescriptor, data);
	void *updatedRecord = malloc(updatedRecordLength);
	encodeRecord(recordDescriptor, data, updatedRecord);
    
	char *page = (char *)malloc(PAGE_SIZE);
	returnValue = fileHandle.readPage(pageNum, page);
	if (returnValue != 0) {
		free(page);
		return returnValue;
	}
    
	const char *endOfPagePtr = page + PAGE_SIZE;
    
	Slot *slotPtr = goToSlot(endOfPagePtr, slotNum);
	// make sure this record is not deleted
	if (slotPtr->beginAddr < 0) {
		free(page);
		return -1;
	}
    
	char *recordPtr = page + slotPtr->beginAddr;
	bool isTomb = isTombStone(recordPtr, pageNum, slotNum); // read tomb flag
    
	if (isTomb) {
		// delete the real record
		RID replaceRid;
		replaceRid.pageNum = pageNum;
		replaceRid.slotNum = slotNum;
		returnValue = deleteRecord(fileHandle, recordDescriptor, replaceRid);
		if (returnValue != 0) {
			free(page);
			return returnValue;
		}
        
		// insert the record in a new place
		short temp = (*spaceLeftVect)[oriPageNum];
		(*spaceLeftVect)[oriPageNum] = 0;
        
		returnValue = insertRecord(fileHandle, recordDescriptor, data, replaceRid);
        
		(*spaceLeftVect)[oriPageNum] = temp;
        
		if (returnValue != 0) {
			free(page);
			return returnValue;
		}
        
		// change the content in this tomb stone
		setAsTomb(recordPtr, replaceRid.pageNum, replaceRid.slotNum);
	}
	else {
		short oriRecordLength = slotPtr->endAddr - slotPtr->beginAddr;
        
		// if this slot is enough to hold updated record
		if (updatedRecordLength <= oriRecordLength) {
			// update record
			memcpy(recordPtr, updatedRecord, updatedRecordLength);
			// update end address
			slotPtr->endAddr = slotPtr->beginAddr + updatedRecordLength;
			// release free space
			(*spaceLeftVect)[oriPageNum] += oriRecordLength - updatedRecordLength;
		}
		else {
			RID replaceRid;
			short temp = (*spaceLeftVect)[oriPageNum];
			(*spaceLeftVect)[oriPageNum] = 0;
            
			returnValue = insertRecord(fileHandle, recordDescriptor, data, replaceRid);
            
			(*spaceLeftVect)[oriPageNum] = temp;
			if (returnValue != 0) {
				free(page);
				return returnValue;
			}
            
			// set the original slot to a tomb stone
			setAsTomb(recordPtr, replaceRid.pageNum, replaceRid.slotNum);
			// update its begin and end address in slot
			slotPtr->endAddr = slotPtr->beginAddr + SMALLEST_RECORD_LENGTH;
			// release free space
			(*spaceLeftVect)[oriPageNum] += oriRecordLength - SMALLEST_RECORD_LENGTH;
		}
	}
    
	returnValue = fileHandle.writePage(oriPageNum, page);
    
	free(page);
	return returnValue;
}


RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	int returnValue = -1;

	//ensure we have a valid file handle
	if(fileHandle.getFile() == NULL) {
		return returnValue;
	}

	unsigned pageNum = rid.pageNum;
	unsigned slotNum = rid.slotNum;

	char *page = (char *)malloc(PAGE_SIZE);

	bool isTomb = true;
	while (isTomb) {
		returnValue = fileHandle.readPage(pageNum, page);
		if (returnValue != 0) // unsuccessful read
			break;

		const char *endOfPagePtr = page + PAGE_SIZE;
		Slot *slotPtr = goToSlot(endOfPagePtr, slotNum);

		// make sure this slot is not deleted
		if (slotPtr->beginAddr < 0) {
			returnValue = -1;
			break;
		}

		char *recordPtr = page + slotPtr->beginAddr; // go to the record
		isTomb = isTombStone(recordPtr, pageNum, slotNum); // read isTomb flag

		if (!isTomb) { // this is real data
			// read record length from slot directory
			short recordLength = slotPtr->endAddr - slotPtr->beginAddr;
			void *record = malloc(recordLength);
			memcpy(record, recordPtr, recordLength); // read record
			returnValue = decodeRecord(recordDescriptor, record, data); // translate record back
			free(record);
		}
	}

	free(page);
	return returnValue;
}


RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data) {
	int returnValue = -1;

	if(fileHandle.getFile() == NULL) {
		return returnValue;
	}

	// get the attribute number and type
	unsigned attributeNum = 0;
	AttrType type;
	for (; attributeNum < recordDescriptor.size(); attributeNum++) {
		Attribute attr = recordDescriptor[attributeNum];
		if (attr.name.compare(attributeName) == 0) {
			type = attr.type;
			break;
		}
	}

	// attribute not found
	if (attributeNum == recordDescriptor.size())
		return returnValue;

	unsigned pageNum = rid.pageNum;
	unsigned slotNum = rid.slotNum;
	char *page = (char *)malloc(PAGE_SIZE);

	bool isTomb = true;
	while (isTomb) {
		returnValue = fileHandle.readPage(pageNum, page);
		if (returnValue != 0)
			break;

		const char *endOfPagePtr = page + PAGE_SIZE;
		Slot *slotPtr = goToSlot(endOfPagePtr, slotNum);

		if (slotPtr->beginAddr < 0) {
			returnValue = -1;
			break;
		}

		char *recordPtr = page + slotPtr->beginAddr; // go to the record
		isTomb = isTombStone(recordPtr, pageNum, slotNum); // read tomb flag

		if (!isTomb) {
			short attrBeginAddr = *((short *)(recordPtr + sizeof(short) * (attributeNum + 1))); // read attribute start address
			short attrEndAddr = *((short *)(recordPtr + sizeof(short) * (attributeNum + 2))); // read attribute end address
			int attrLength = (int)(attrEndAddr - attrBeginAddr);

			// set recordPtr to the begin of the attribute and read attribute
			recordPtr = recordPtr + attrBeginAddr;
			if (type == TypeInt)
				memcpy(data, recordPtr, sizeof(int));

			else if (type == TypeReal)
				memcpy(data, recordPtr, sizeof(float));

			else if (type == TypeVarChar) {
				memcpy(data, &attrLength, sizeof(int));
				memcpy((char *)data + sizeof(int), recordPtr, attrLength);
			}
		}
	}

	free(page);
	return returnValue;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
	int returnValue = -1;
	if(fileHandle.getFile() == NULL) {
		return returnValue;
	}

	if(filePageDirectory.find(fileHandle.getFileName()) == filePageDirectory.end()) {
		return returnValue;
	}

	vector<short> *spaceLeftVect = filePageDirectory[fileHandle.getFileName()];

	unsigned pageNum = rid.pageNum;
	unsigned slotNum = rid.slotNum;

	char *page = (char *)malloc(PAGE_SIZE);

	bool isTomb = true;
	while (isTomb) {
		returnValue = fileHandle.readPage(pageNum, page);
		if (returnValue != 0)
			break;

		const char *endOfPagePtr = page + PAGE_SIZE;
		Slot *slotPtr = goToSlot(endOfPagePtr, slotNum);

		if (slotPtr->beginAddr < 0) { // record has been deleted
			returnValue = -1;
			break;
		}

		char *recordPtr = page + slotPtr->beginAddr;

		// get the current pageNum, in case isTombStone change the pageNum and lose information
		unsigned tempPageNum = pageNum;

		isTomb = isTombStone(recordPtr, pageNum, slotNum); // read tomb flag
		// get record Length;
		short recordLength = slotPtr->endAddr - slotPtr->beginAddr;
		// delete this slot
		slotPtr->beginAddr = -1 - slotPtr->beginAddr;
		// release free space
		(*spaceLeftVect)[tempPageNum] = (*spaceLeftVect)[tempPageNum] + recordLength;

		returnValue = fileHandle.writePage(tempPageNum, page);
		if (returnValue != 0)
			break;
	}

	free(page);
	return returnValue;
}

RC RecordBasedFileManager::deleteRecords(FileHandle & fileHandle) {
	int returnValue = -1;

	string fileName = fileHandle.getFileName();
	if (closeFile(fileHandle) != 0)
		return returnValue;

	if (destroyFile(fileName) != 0)
		return returnValue;

	if (createFile(fileName) != 0)
		return returnValue;

	if (openFile(fileName, fileHandle) != 0)
		return returnValue;

	return 0;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber) {
	int returnValue = -1;
	if(fileHandle.getFile() == NULL) {
		return returnValue;
	}

	if(filePageDirectory.find(fileHandle.getFileName()) == filePageDirectory.end()) {
		return returnValue;
	}

	vector<short> *spaceLeftVect = filePageDirectory[fileHandle.getFileName()];

	char *page = (char *)malloc(PAGE_SIZE);
	char *reorgPage = (char *)malloc(PAGE_SIZE);
	returnValue = fileHandle.readPage(pageNumber, page);
	memcpy(reorgPage, page, PAGE_SIZE);

	if (returnValue == 0) {
		const char *endOfPagePtr = page + PAGE_SIZE;
		Footer *footerPtr = goToFooter(endOfPagePtr);
		Slot *slotPtr = goToSlot(endOfPagePtr, 1);

		const char *endOfReorgPagePtr = reorgPage + PAGE_SIZE;
		Footer *reorgFooterPtr = goToFooter(endOfReorgPagePtr);
		Slot *reorgSlotPtr = goToSlot(endOfReorgPagePtr, 1);

		short offset = 0;

		for (short i = 0; i < footerPtr->numOfSlots; i++) {
			short recordBeginAddr = slotPtr->beginAddr;
			short recordEndAddr = slotPtr->endAddr;

			// this is a real record
			if (recordBeginAddr >= 0 && recordEndAddr > 0) {
				short recordLength = recordEndAddr - recordBeginAddr;
				memcpy(reorgPage + offset, page + recordBeginAddr, recordLength); // move record;
				reorgSlotPtr->beginAddr = offset; // reset begin addr and end addr
				reorgSlotPtr->endAddr = offset + recordLength;
				offset += recordLength;
			}

			// this is a deleted record
			else if (recordBeginAddr < 0) {
				reorgSlotPtr->beginAddr = 0; // set begin and end addr to zero, meaning this is a slot which has no record associated and can be recycled
				reorgSlotPtr->endAddr = 0;
			}

			slotPtr--; // go to next slot;
			reorgSlotPtr--;
		}

		reorgFooterPtr->reOrg = 0; // reset reOrg counter;
		reorgFooterPtr->freeSpaceOffset = offset;

		(*spaceLeftVect)[pageNumber] = PAGE_SIZE - offset - FOOTER_OVERHEAD - RECORD_OVERHEAD * (reorgFooterPtr->numOfSlots + 1);

		returnValue = fileHandle.writePage(pageNumber, reorgPage);
	}

	free(page);
	free(reorgPage);
	return returnValue;
};



RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	int offset = 0;
	for (unsigned i = 0; i < recordDescriptor.size(); i++) {
		Attribute attr = recordDescriptor[i];

		if (attr.type == TypeInt) {
			printf("%d\t", *((int *)((char *)data + offset)));
			offset += attr.length;
		}
		else if (attr.type == TypeReal) {
			printf("%.4f\t", *((float *)((char *)data + offset)));
			offset += attr.length;
		}
		else if (attr.type == TypeVarChar) {
			int length = *((int *)((char *)data + offset));
			offset += sizeof(int);
			for (int j = 0; j < length; j++) {
				printf("%c", *((char *)data + offset));
				offset++;
			}
			printf("\t");
		}
	}
	printf("\n");

	return 0;
}

RC RecordBasedFileManager::printAttribute(const void *data, AttrType type) {
	switch (type) {
	case TypeInt:
		printf("%d", *(int *)data); break;
	case TypeReal:
		printf("%.4f", *(float *)data); break;
	case TypeVarChar:
		int length = *(int *)data;
		int offset = sizeof(int);
		for (int i = 0; i < length; i++) {
			printf("%c", *((char *)data + offset));
			offset++;
		}
		break;
	}

	printf("\n");
	return 0;
}


/*
 * record length equals to the length of overhead plus length of read data
 *
 * e.g. if there are four fields in a record, the length of overhead is 6 * sizeof(short)
 *
 * including one short for isTomb indicator, 4 shorts for start address of 4 fields and 1 short for end address of last field
 *
 *  [isTomb][startField1][startField2][startField3]...[startFieldN][endFieldN][Field1][Field2][Field3]...[FieldN]
 *
 * if length is shorter than 10 ( one short(2 bytes) plus two unsigned(2*4 bytes) which are need when this record becomes a 
 * tomb and need to hold new pageNum and slotNum), return 10
 *
 */
short RecordBasedFileManager::getRecordLength(const vector<Attribute> &recordDescriptor, const void *data) {
	short length = 2 * sizeof(short);  //add the length of tombstone and end field directory entries
	short offset = 0;
	int varCharLength = 0;

	for (unsigned i = 0; i < recordDescriptor.size(); i++) {
		length += sizeof(short); // increment by the size of record-directory entry

		Attribute attr = recordDescriptor[i];

		if (attr.type == TypeInt) {
			length += attr.length;
			offset += attr.length;
		}
		else if (attr.type == TypeReal) {
			length += attr.length;
			offset += attr.length;
		}
		else if (attr.type == TypeVarChar) {
			memcpy(&varCharLength, (char*)data + offset, sizeof(int));
			length += varCharLength;
			offset += varCharLength + sizeof(int);
		}
	}

	return length > SMALLEST_RECORD_LENGTH ? length : SMALLEST_RECORD_LENGTH;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor,
			const string &conditionAttribute,
			const CompOp compOp,
			const void *value,
			const vector<string> &attributeNames,
			RBFM_ScanIterator &rbfm_ScanIterator) {
	return rbfm_ScanIterator.initialize(fileHandle, recordDescriptor, compOp, value, attributeNames, conditionAttribute);
}

/**
 * this method translate record provided to record of our format
 * inputRecord is pointer to provided record, outputRecord is pointer to record of our format
 *
 * record format: [short isTomb][short startOfField1][short startOfField2]...[short startOfFieldN][short endOfFieldN][Field 1][Field 2]...[FieldN]
 * NOTE: start and end are all relative offset, which means offset from the start of this record
 */
RC RecordBasedFileManager::encodeRecord(const vector<Attribute> recordDescriptor, const void *inputRecord, void *outputRecord) {
	short outputOffset = (recordDescriptor.size() + 2) * sizeof(short); // skip overhead
	short inputOffset = 0;
	int varCharLength = 0;

	*((short *)outputRecord) = 0; // this is not a tomb stone for a record
	unsigned i = 0;
	for (; i < recordDescriptor.size(); i++) {
		*((short *)outputRecord + i + 1) = outputOffset; // set the overhead directory

		Attribute attr = recordDescriptor[i];

		if (attr.type == TypeInt) {
			memcpy((char*)outputRecord + outputOffset, (char*)inputRecord + inputOffset, sizeof(int));
			inputOffset += sizeof(int);
			outputOffset += sizeof(int);
		}
		else if (attr.type == TypeReal) {
			memcpy((char*)outputRecord + outputOffset, (char*)inputRecord + inputOffset, sizeof(float));
			inputOffset += sizeof(float);
			outputOffset += sizeof(float);
		}
		else if (attr.type == TypeVarChar) {
			memcpy(&varCharLength, (char*)inputRecord + inputOffset, sizeof(int)); // get the length of VarChar
			inputOffset += sizeof(int); // input skip this integer
			memcpy((char*)outputRecord + outputOffset, (char*)inputRecord + inputOffset, varCharLength); // copy data
			inputOffset += varCharLength;
			outputOffset += varCharLength;
		}
	}
	*((short *)outputRecord + i + 1) = outputOffset;
	return 0;
}

/**
 * this method translate the record of our format to record required by the project
 * inputRecord is pointer to our record, outputRecord is pointer to required record
 */
RC RecordBasedFileManager::decodeRecord(const vector<Attribute> recordDescriptor, const void *inputRecord, void *outputRecord) {
	short inputStart = 0;
	short outputOffset = 0;

	for (unsigned i = 0; i < recordDescriptor.size(); i++) {
		Attribute attr = recordDescriptor[i];
		inputStart = *((short *)inputRecord + i + 1);

		if (attr.type == TypeInt) {
			memcpy((char *)outputRecord + outputOffset, (char *)inputRecord + inputStart, sizeof(int));
			outputOffset += sizeof(int);
		}
		else if (attr.type == TypeReal) {
			memcpy((char *)outputRecord + outputOffset, (char *)inputRecord + inputStart, sizeof(float));
			outputOffset += sizeof(float);
		}
		else if (attr.type == TypeVarChar) {
			short inputEnd = *((short *)inputRecord + i + 2);
			int length = inputEnd - inputStart;

			memcpy((char *)outputRecord + outputOffset, &length, sizeof(int)); // write varChar Length
			outputOffset += sizeof(int);

			memcpy((char *)outputRecord + outputOffset, (char *)inputRecord + inputStart, length);
			outputOffset += length;
		}
	}
	return 0;
}

/**
 * this method read one single page into vector "spaceLeft"
 */
RC RecordBasedFileManager::readHeaderPage(FileHandle &metaFileHandle, unsigned currentHeaderPage, vector<short> * spaceLeft) {
	char *page = (char *)malloc(PAGE_SIZE);

	int returnValue = metaFileHandle.readPage(currentHeaderPage, page);

	if (returnValue == 0) {

		short numOfPages = *(short *)page;

		for (short i = 1; i <= numOfPages; i++)
			spaceLeft->push_back(*((short *)(page + sizeof(short) * i)));
	}

	free(page);
	return returnValue;
}

/**
 * this method write free space information in vector "spaceLeft" to one single headerPage
 * NOTE: every header page is allow to store HEADER_PAGE_SLOT entries of free space information
 */
RC RecordBasedFileManager::writeHeaderPage(FileHandle &metaFileHandle, unsigned currentHeaderPage, vector<short> * spaceLeft, unsigned &currentPage) {
	int returnValue = -1;
	char *page = (char *)malloc(PAGE_SIZE);
	short numOfPage = 0;
	int offset = sizeof(short);

	while (numOfPage < HEADER_PAGE_SLOT && currentPage < spaceLeft->size()) {
		*(short *)(page + offset) = (*spaceLeft)[currentPage];
		currentPage++;
		numOfPage++;
		offset += sizeof(short);
	}

	*(short *)page = numOfPage; // write numOfPage info in the first two bytes

	returnValue = metaFileHandle.writePage(currentHeaderPage, page);
	free(page);

	return returnValue;
}

bool RecordBasedFileManager::fexist(string fileName) {
    return pfm->fexist(fileName);
}

/*******RBFM SCAN ITERATOR**********/


RBFM_ScanIterator::RBFM_ScanIterator() {
	op = NO_OP;
	condition = NULL;
	conditionAttrType = TypeInt;
	conditionAttrNum = 0;
    
	pageNum = 0;
	slotNum = 0;
    
	page = NULL;
	endOfPagePtr = NULL;
	footerPtr = NULL;
    
	projAttrNum.clear();
	projAttrType.clear();
	tombStoneMap.clear();
}

RBFM_ScanIterator::~RBFM_ScanIterator(){}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
	void *currentAttr = malloc(PAGE_SIZE);
	char *recordPtr;
	Slot *slotPtr;
	int recordLength = 0;
	bool result = false;
    
	do {
		slotNum++;
        
		// all records in this page have been scanner, read the next page
		if (slotNum > (unsigned)footerPtr->numOfSlots) {
			slotNum = 1;
			pageNum++;
            
			// all pages have been scanned
			if (pageNum >= fileHandle.getNumberOfPages())
				return RBFM_EOF;
            
			// read next page
			fileHandle.readPage(pageNum, page);
		}
        
		rid.pageNum = pageNum;
		rid.slotNum = slotNum;
		slotPtr = (Slot *)(endOfPagePtr - FOOTER_OVERHEAD - slotNum * RECORD_OVERHEAD);
        
		if (slotPtr->beginAddr < 0) // the record in this slot is deleted
			continue;
        
		recordPtr = page + slotPtr->beginAddr;
        
		if (*(short *)recordPtr == -1) {// this is a tomb stone
			RID replaceRid;
			replaceRid.pageNum = *(unsigned *)(recordPtr + sizeof(short));
			replaceRid.slotNum = *(unsigned *)(recordPtr + sizeof(short) + sizeof(unsigned));
            
			tombStoneMap[replaceRid.pageNum * 1000 + replaceRid.slotNum] = rid.pageNum * 1000 + slotNum;
			continue;
		}
        
		if (tombStoneMap.find(rid.pageNum * 1000 + slotNum) != tombStoneMap.end()) {
			unsigned replaceRid = tombStoneMap[rid.pageNum * 1000 + slotNum];
			tombStoneMap.erase(rid.pageNum * 1000 + slotNum);
			rid.pageNum = replaceRid / 1000;
			rid.slotNum = replaceRid % 1000;
		}
        
		readAttr(recordPtr, currentAttr, conditionAttrNum, conditionAttrType, recordLength);
		result = compare(currentAttr, condition, conditionAttrType, op);
	}
	while (!result);
    
	free(currentAttr);
	// project attributes
	return projectAttr(recordPtr, data, projAttrType, projAttrNum);
}

RC RBFM_ScanIterator::close() {
	op = NO_OP;
	condition = NULL;
	conditionAttrType = TypeInt;
	conditionAttrNum = 0;
    
	pageNum = 0;
	slotNum = 0;
    
	free(page);
	page = NULL;
	endOfPagePtr = NULL;
	footerPtr = NULL;
    
	projAttrNum.clear();
	projAttrType.clear();
	tombStoneMap.clear();
    
	return 0;
}

/*
 * this method is an initialization method, called by rbfm::scan
 */
RC RBFM_ScanIterator::initialize(FileHandle &fileHandle,
                                 const vector<Attribute> &recordDescriptor,
                                 const CompOp compOp,
                                 const void *value,
                                 const vector<string> &attributeNames,
                                 const string &conditionAttribute) {
	op = compOp;
	condition = value;
	this->fileHandle = fileHandle;
	pageNum = 0;
	slotNum = 0;
    
	// read the first record page and set related pointers
	page = (char *)malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum, page);
	endOfPagePtr = page + PAGE_SIZE;
	footerPtr = (Footer *)(endOfPagePtr - FOOTER_OVERHEAD);
    
	for (unsigned i = 0, j = 0; i < recordDescriptor.size() && j < attributeNames.size(); i++) {
		Attribute attr = recordDescriptor[i];
		// find the attribute number and type of condition attribute
		if (attr.name.compare(conditionAttribute) == 0) {
			conditionAttrType = attr.type;
			conditionAttrNum = (short)i;
		}
        
		// find the attribute number and type of project attributes
		if (attr.name.compare(attributeNames[j]) == 0) {
			j++;
			projAttrNum.push_back((short)i);
			projAttrType.push_back(attr.type);
		}
	}
    
	return 0;
}


/*
 * this method compare the value of attribute with condition
 */
bool RBFM_ScanIterator::compare(void *attribute, const void *condition, AttrType type, CompOp compOp) {
	if (condition == NULL)
		return true;
    
	bool result = true;
    
	switch (type) {
        case TypeInt: {
            int attr = *(int *)attribute;
            int cond = *(int *)condition;
            
            switch(compOp) {
                case EQ_OP: result = attr == cond; break;
                case LT_OP: result = attr < cond; break;
                case GT_OP: result = attr > cond; break;
                case LE_OP: result = attr <= cond; break;
                case GE_OP: result = attr >= cond; break;
                case NE_OP: result = attr != cond; break;
                case NO_OP: break;
            }
            
            break;
        }
            
        case TypeReal: {
            float attr = *(float *)attribute;
            float cond = *(float *)condition;
            
            switch(compOp) {
                case EQ_OP: result = attr == cond; break;
                case LT_OP: result = attr < cond; break;
                case GT_OP: result = attr > cond; break;
                case LE_OP: result = attr <= cond; break;
                case GE_OP: result = attr >= cond; break;
                case NE_OP: result = attr != cond; break;
                case NO_OP: break;
            }
            
            break;
        }
            
        case TypeVarChar: {
            int attriLeng = *(int *)attribute;
            string attr((char *)attribute + sizeof(int), attriLeng);
            int condiLeng = *(int *)condition;
            string cond((char *)condition + sizeof(int), condiLeng);
            
            switch(compOp) {
                case EQ_OP: result = strcmp(attr.c_str(), cond.c_str()) == 0; break;
                case LT_OP: result = strcmp(attr.c_str(), cond.c_str()) < 0; break;
                case GT_OP: result = strcmp(attr.c_str(), cond.c_str()) > 0; break;
                case LE_OP: result = strcmp(attr.c_str(), cond.c_str()) <= 0; break;
                case GE_OP: result = strcmp(attr.c_str(), cond.c_str()) >= 0;break;
                case NE_OP: result = strcmp(attr.c_str(), cond.c_str()) != 0; break;
                case NO_OP: break;
            }
            
            break;
        }
	}
	return result;
}


/*
 * this method write attribute indicated by type and attrNum to void *attribute
 * and save the length of this attribute in attrLength
 */
RC RBFM_ScanIterator::readAttr(char *recordPtr, void *attribute, short attrNum, AttrType type, int &attrLength) {
	short attrBeginAddr = *(short *)(recordPtr + sizeof(short) * (attrNum + 1));
	short attrEndAddr = *(short *)(recordPtr + sizeof(short) * (attrNum + 2));
	attrLength = (int)(attrEndAddr - attrBeginAddr);
    
	if (type == TypeInt)
		memcpy(attribute, recordPtr + attrBeginAddr, sizeof(int));
    
	else if (type == TypeReal)
		memcpy(attribute, recordPtr + attrBeginAddr, sizeof(float));
    
	else if (type == TypeVarChar) {
		memcpy(attribute, &attrLength, sizeof(int));
		memcpy((char *)attribute + sizeof(int), recordPtr + attrBeginAddr, attrLength);
		attrLength += sizeof(int);
	}
    
	return 0;
}

/*
 * this method project the attributes indicated by projAttrType and projAttrNum to void *data
 */
RC RBFM_ScanIterator::projectAttr(char *recordPtr, void *data, vector<AttrType> projAttrType, vector<short> projAttrNum) {
	int offset = 0;
	int attrLength = 0;
    
	void *currentAttr = malloc(PAGE_SIZE);
    
	for (unsigned i = 0; i < projAttrNum.size(); i++) {
		readAttr(recordPtr, currentAttr, projAttrNum[i], projAttrType[i], attrLength);
		memcpy((char *)data + offset, currentAttr, attrLength);
		offset += attrLength;
	}
    
	free(currentAttr);
	return 0;
}
