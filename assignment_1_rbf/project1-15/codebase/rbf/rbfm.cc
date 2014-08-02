
#include "rbfm.h"

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
 * through the PagedFileManager, but also for create the initial file header page in the file.
 */
RC RecordBasedFileManager::createFile(const string &fileName) {
	int returnValue = pfm->createFile(fileName.c_str());

	FileHandle fileHandle;

	if (returnValue == 0) {
		returnValue = pfm->openFile(fileName.c_str(), fileHandle);
	}

	if (returnValue == 0) {

		void *data = malloc(PAGE_SIZE);

		*((int *)data) = -1; // link to next header page
		*((int *)data + 1) = 1; // count for how many pages info stored in this header page
		*((short *)((char *)data + sizeof(int) * 2)) = 0; // set the free space of page0 (header page) to zero

		returnValue = fileHandle.appendPage(data);

		if (returnValue == 0) {
			returnValue = pfm->closeFile(fileHandle);
		}

		free(data);
	}

	return returnValue;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    int returnValue = pfm->destroyFile(fileName.c_str());
    if (returnValue == 0) { // if successfully destroy file through pfm
    	filePageDirectory.erase(fileName);
    }

    //otherwise some other fileHandle may be open, cannot delete it from filePageDirectory
    return returnValue;
}

/**
 * This method opens the file indicated by fileName in the arguments.  Once the file is open and a handle is
 * retreived, the header page is read which contains the page sizes within the file and these sizes are loaded
 * into a vector.  Once the header file is read and vector is loaded, the fileName along with its vector of
 * page sizes is loaded to the map.
 *
 * Format of file header page starting from byte 0:  [int nextHeaderPage][int numPagesInFile][short page 0 free size][short page 1 free size]...
 */
RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	int returnValue = pfm->openFile(fileName.c_str(), fileHandle);

	if (returnValue == 0) { //successful file open

		if (filePageDirectory.find(fileName) == filePageDirectory.end()) {  //filePageDirectory doesn't have an entry for this file
			vector<short> * spaceLeft = new vector<short>();
			int currentHeaderPage = 0; // page 0 is always header page

			while (currentHeaderPage >= 0) {  // if there are more header pages, read free space into vector one by one
				returnValue = readHeaderPage(fileHandle, currentHeaderPage, spaceLeft);

				if (returnValue != 0)
					return returnValue;
			}

			filePageDirectory[fileName] = spaceLeft; //add the file/pageSize entry to the filePageDirectory map
		}

	}

	return returnValue;
}



RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	//  write data back to the header page in the file, or create new header page if current header pages are full and create link to new header page
	//  remove the file entry from the file Page directory

	if (fileHandle.getFile() == NULL || filePageDirectory.find(fileHandle.getFileName()) == filePageDirectory.end()) {
		return -1;
	}

	int returnValue;
	vector<short> * spaceLeft = filePageDirectory[fileHandle.getFileName()];
	unsigned currentPage = 0;
	int nextHeaderPage = 0;
	int currentHeaderPage = 0;


	while (currentHeaderPage >= 0) { // more header pages need to write
		returnValue = writeHeaderPage(fileHandle, currentHeaderPage, nextHeaderPage, spaceLeft, currentPage);

		if (returnValue != 0) {
			return returnValue;
        }
        
        currentHeaderPage = nextHeaderPage;
	}

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

	int dataLength = getRecordLength(recordDescriptor, data);
	void *record = malloc(dataLength);
	encodeRecord(recordDescriptor, data, record); // translate record into our format

	vector<short> * pageSizeVect = filePageDirectory[fileHandle.getFileName()];

	void *beginningOfPagePtr = malloc(PAGE_SIZE);  //create buffer to hold the file's page and perform arithmetic on it

	unsigned i = 0;
	//find a page that has enough space
	for(; i < pageSizeVect->size(); i++) {

		if (dataLength <= (*pageSizeVect)[i]) {  //page potentially has enough space
			void *filePageData, *endOfPagePtr, *currentRecordSlotPtr;  //pointers to access beginning and end of page

			fileHandle.readPage(i, beginningOfPagePtr);  //read the page into the buffer

			endOfPagePtr = ((char*) beginningOfPagePtr) + PAGE_SIZE;

			short reorganizationFlag, numberOfRecords, freeSpaceOffset;  //to hold page's footer values
			readFooter(endOfPagePtr, reorganizationFlag, freeSpaceOffset, numberOfRecords);

			short recordBeginAddress, previousRecordsEnd, beginAddressToInsert, nextRecordBeginAddress = 0;

			filePageData = (char *)endOfPagePtr - FOOTER_OVERHEAD;
			//go through all the records in the current page footer to try and find an empty slot
			for (short j = 1; j <= numberOfRecords; j++) {

				//get the beginning memory address of the record j in current slot
				filePageData = ((char*)filePageData) - sizeof(short);
				currentRecordSlotPtr = filePageData;
				recordBeginAddress = *((short*) filePageData);

				if (recordBeginAddress < 0) {  //deleted slot
					unsigned slotNum = j; // potential slotNum to insert
					// get the previous record ending address
					// if the first record is deleted, set previousRecordsEnd to 0;
					if (j == 1) {
						previousRecordsEnd = 0;
					}
					else {
						previousRecordsEnd = *((short *)(((char*)filePageData) + sizeof(short)));
					}

					//base on the fact that the last record is not deleted
					//it means when implementing deleteRecord, need to check if it is the last record and the records before
					//it is deleted or not, then reset free space offset and number of records
					while (j <= numberOfRecords) {
						j++;
						filePageData = ((char*)filePageData) - (sizeof(short) * 2); //go to beginning of next record

						nextRecordBeginAddress = *((short*) filePageData);

						if (nextRecordBeginAddress > 0) {
							short slotSize = nextRecordBeginAddress - previousRecordsEnd;
							if (slotSize >= dataLength && slotSize < 3 * dataLength) {
								beginAddressToInsert = previousRecordsEnd;

								//write the begin address
								*((short*)currentRecordSlotPtr) = beginAddressToInsert;

								//write the end address
								currentRecordSlotPtr = ((char*) currentRecordSlotPtr) - sizeof(short);
								*((short*)currentRecordSlotPtr) = beginAddressToInsert + dataLength;

								//write the record
								filePageData = ((char*) beginningOfPagePtr) + beginAddressToInsert;
								memcpy(filePageData, record, dataLength);

								//Update VECTOR information in the filePageDirectory to reflect the free bytes now available to the page
								(*pageSizeVect)[i] = (*pageSizeVect)[i] - dataLength;

								returnValue = fileHandle.writePage(i, beginningOfPagePtr);
								if (returnValue == 0) {
									rid.pageNum = i;
									rid.slotNum = slotNum;
								}

								free(record);
								free(beginningOfPagePtr);

								return returnValue;
							}
							else { // the length of record is smaller than the slot or too bigger
								filePageData = (char *)filePageData - sizeof(short);
								break; // break while loop and find the next deleted record
							}
						}
					} //end while
				} // end if
				else { // if this slot is not deleted
					filePageData = ((char*)filePageData) - sizeof(short); // skip end address of the record
				}


			}// end looping through records in the page


			/**
			 * couldnt find an empty slot in a page that says it has enough bytes available
			 * so try to add to free space of filePageDirectory vector page i
			 */

			//make sure there is enough space in page to accomodate a new record (numberOfRecords + 1)
			void *beginningOfFooter= ((char*) endOfPagePtr) - ((numberOfRecords + 1) * RECORD_OVERHEAD) - FOOTER_OVERHEAD;

			short footerLength = ((char*)endOfPagePtr) - ((char*)beginningOfFooter);
			short totalFreeSpaceBytes = PAGE_SIZE - freeSpaceOffset - footerLength;

			if (totalFreeSpaceBytes >= dataLength) {
				void *freeSpacePtr = ((char*)beginningOfPagePtr) + freeSpaceOffset;
				void *tmpPtr = endOfPagePtr;

				memcpy(freeSpacePtr, record, dataLength);

				//set the free space offset
				tmpPtr = ((short*) tmpPtr) - 1;
				*((short*) tmpPtr) = freeSpaceOffset + dataLength;

				//set the new number of records skipping over the reorg count
				tmpPtr = ((short*) tmpPtr) - 2;
				*((short*) tmpPtr) = numberOfRecords + 1;

				//go to end of slot and one more short length to insert the beginning address
				tmpPtr = ((char*) beginningOfFooter) + sizeof(short);
				*((short*) tmpPtr) = freeSpaceOffset;

				//update the end of address of the new record
				tmpPtr = ((char*) tmpPtr) - sizeof(short);
				*((short*) tmpPtr) = freeSpaceOffset + dataLength;

				//  Update vector information in the filePageDirectory to reflect the free bytes now avaiable to the page
				(*pageSizeVect)[i] = (*pageSizeVect)[i] - (dataLength + RECORD_OVERHEAD);

				returnValue = fileHandle.writePage(i, beginningOfPagePtr);  //write the page with new contents back to disk

				if(returnValue == 0) {
					rid.pageNum = i;
					rid.slotNum = numberOfRecords + 1;
				}

				free(record);
				free(beginningOfPagePtr);

				return returnValue;  //there was enough space but something went wrong in the write
			}
			else{
				//page showed enough available bytes but coudln't find an empty slot/no room in the free space, update the reorgFlagCount in current page
				void *reOrgPtr = endOfPagePtr;
				reOrgPtr = ((short*) reOrgPtr) - 2;

				*((short*)reOrgPtr) = reorganizationFlag + 1;

				fileHandle.writePage(i, beginningOfPagePtr);  //write the page with new contents back to disk to save the updated reorg flag count
			}
		}
	}

	//all pages were in vector did not have enough space to write the current record
	returnValue = appendPageWithOneRecord(fileHandle, record, dataLength);

	if(returnValue == 0) {  //only update if the record was inserted successfully
		rid.pageNum = i;
		rid.slotNum = 1;
	}

	free(record);
	free(beginningOfPagePtr);

	return returnValue;
}


RC RecordBasedFileManager::appendPageWithOneRecord(FileHandle &fileHandle, const void *data, int dataLength) {
	void *pageData = malloc(PAGE_SIZE);

	//put record at beginning and create the footer slot information
	prepareDataForNewPageWrite(data, pageData, dataLength);

	//append the newly created page to the file
	int returnValue = fileHandle.appendPage(pageData);

	if(returnValue == 0) {
		vector<short> * pageSizeVector = filePageDirectory[fileHandle.getFileName()];  //add the page to the directory
		pageSizeVector->push_back(PAGE_SIZE - FOOTER_OVERHEAD - RECORD_OVERHEAD - dataLength);  //update the available bytes of the page
	}

	free(pageData);
	return returnValue;
}

RC RecordBasedFileManager::prepareDataForNewPageWrite(const void *record, void *pageData, int dataLength){

	memcpy(pageData, record, dataLength);  //write the record to the beginning of the file

	void *dataptr = pageData; //pointer to the beginning

	//BEGIN: INITIALIZE FOOTERS
	dataptr = ((char*)dataptr) + (PAGE_SIZE - sizeof(short));
	*((short*) dataptr) = dataLength; //freespace pointer

	dataptr = ((char*)dataptr) - sizeof(short);
	*((short*) dataptr) = 0; //reorg flag

	dataptr = ((char*)dataptr) - sizeof(short);
	*((short*) dataptr) = 1; //number of records
	//END: INITIALIZE FOOTERS

	//BEGIN: RECORD ENTRY IN PAGE FOOTER SLOT
	dataptr = ((char*)dataptr) - sizeof(short);
	*((short*) dataptr) = 0; //start of the record

	dataptr = ((char*)dataptr) - sizeof(short);
	*((short*) dataptr) = dataLength; //end of the record
	//END: RECORD ENTRY IN PAGE FOOTER SLOT

	return 0;
}

void RecordBasedFileManager::readFooter(void *endOfPagePtr, short &reorgFlag, short &freeSpaceOffset, short &numberOfRecords){

	void *footerPtr = endOfPagePtr;

	footerPtr = ((char*) footerPtr) - sizeof(short);  //read first slot (pointer to free space)
	freeSpaceOffset = *((short*) footerPtr);

	footerPtr = ((char*) footerPtr) - sizeof(short); //read the second slot (reorg)
	reorgFlag = *((short*) footerPtr);

	footerPtr = ((char*) footerPtr) - sizeof(short);  //read 3rd slot (num records in page)
	numberOfRecords = *((short*) footerPtr);
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	int returnValue = -1;

	//ensure we have a valid file handle
	if(fileHandle.getFile() == NULL) {
		return returnValue;
	}

	unsigned pageNum = rid.pageNum;
	unsigned slotNum = rid.slotNum;

	void *buffer = malloc(PAGE_SIZE);

	int isTomb = -1;
	while (isTomb == -1) {
		returnValue = fileHandle.readPage(pageNum, buffer);

		if (returnValue != 0) // unsuccessful read
			break;

		// read start offset, end offset and record length from slot directory
		void *filePageData = (char *)buffer + PAGE_SIZE; // set to the end of page
		filePageData = (char *)filePageData - FOOTER_OVERHEAD - slotNum * RECORD_OVERHEAD; // jump to slot slotNum in slot directory

		short recordEndAddress = *((short *)filePageData);
		short recordBeginningAddress = *((short *)filePageData + 1);
		short recordLength = recordEndAddress - recordBeginningAddress;

		filePageData = (char *)buffer + recordBeginningAddress; // jump to the beginning of record

		void *record = malloc(recordLength);
		memcpy(record, filePageData, recordLength); // read record
		// translate formatted record back
		// if (pageNum, slotNum) is not a tomb, return value should be 0 and break while loop
		// else return value is -1 and (pageNum, slotNum) is changed, continue to read the real record
		isTomb = decodeRecord(recordDescriptor, record, data, pageNum, slotNum);
		free(record);
	}

	free(buffer);

	return returnValue;
}

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


/*
 * record length equals to the length of overhead plus length of read data
 * e.g. if there are four fields in a record, the length of overhead is 6 * sizeof(short)
 * including one short for isTomb indicator, 4 shorts for start address of 4 fields and 1 short for end address of last field
 *
 * if length is shorter than 10 ( one short plus two unsigned which are need when this record becomes a tomb to hold new pageNum and slotNum)
 * return 10
 */
unsigned RecordBasedFileManager::getRecordLength(const vector<Attribute> &recordDescriptor, const void *data) {
	unsigned length = 2 * sizeof(short);
	unsigned offset = 0;
	int varCharLength = 0;

	for (unsigned i = 0; i < recordDescriptor.size(); i++) {
		length += sizeof(short); // increment by the size of directory entry

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

	*((short *)outputRecord) = 0; // this is not a tomb for a record
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
RC RecordBasedFileManager::decodeRecord(const vector<Attribute> recordDescriptor, const void *inputRecord, void *outputRecord, unsigned &pageNum, unsigned &slotNum) {
	short isTomb = *((short *)inputRecord);
	if (isTomb == 0) { // not a record tomb
		short inputStart = 0;
		short outputOffset = 0;

		unsigned i = 0;
		for (; i < recordDescriptor.size(); i++) {
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
	else { // this is a record tomb, real record has been moved to somewhere else
		// set the pageNum and slotNum the real record is moved to
		pageNum = *((unsigned *)((char *)inputRecord + sizeof(short)));
		slotNum = *((unsigned *)((char *)inputRecord + sizeof(short) + sizeof(unsigned)));

		return -1;
	}
}

/*
 * this method read one single page into vector "spaceLeft"
 */
RC RecordBasedFileManager::readHeaderPage(FileHandle &fileHandle, int &currentHeaderPage, vector<short> * spaceLeft) {
	void *data = malloc(PAGE_SIZE);	//read the first page(header) of data
	unsigned offset = 0;

	int returnValue = fileHandle.readPage(currentHeaderPage, data);

	if (returnValue == 0) {
		currentHeaderPage = *((int *)data);
		offset += sizeof(int);

		int numPages = *((int *)((char *)data + offset));
		offset += sizeof(int);

		for (int i = 0; i < numPages; i++) {
			spaceLeft->push_back(*((short *)((char *)data + offset)));
			offset += sizeof(short);
		}
	}

	free(data);
	return returnValue;
}

/*
 * this method write free space information in vector "spaceLeft" to one single headerPage
 * if this header page is enough to hold all information in vector spaceLeft, nextHeaderPage will be set to -1, currentPage should equals to spaceLeft.size()
 * if this header page is not enough to store all information, nextHeaderPage will be set to the pageNum of next HeaderPage
 * and currentPage will equals to the last PageNum stored in this header page plus one.
 * NOTE: every header page is allow to store HEADER_PAGE_SLOT (2040) entries of free space information
 */
RC RecordBasedFileManager::writeHeaderPage(FileHandle &fileHandle, int currentHeaderPage, int &nextHeaderPage, vector<short> * spaceLeft, unsigned &currentPage) {
	void *data = malloc(PAGE_SIZE);
	int returnValue = fileHandle.readPage(currentHeaderPage, data);

	if (returnValue == 0) {
		nextHeaderPage = *((int *)data);

		// if this header page is the last header page and it is not enough to hold
		// left information in free space vector, we need to create a new header Page
		if (nextHeaderPage < 0 && spaceLeft->size() - currentPage > HEADER_PAGE_SLOT) {
			//initial new header page
			void *newHeaderPage = malloc(PAGE_SIZE);
			*((int*)newHeaderPage) = -1; // next header page of new header page
			*((int*)newHeaderPage + 1) = 0; // number of page info in new header page
			returnValue = fileHandle.appendPage(newHeaderPage);

			//set the pageNum of next header Page
			nextHeaderPage = fileHandle.getNumberOfPages() - 1;

			//set the left space of new header page to zero
			spaceLeft->push_back(0);
		}

		int numOfPage = 0;
		int offset = 2 * sizeof(int);

		// currentPage is start page number for next writHeaderPage, if necessary
		while (numOfPage < HEADER_PAGE_SLOT && currentPage < spaceLeft->size()) {
			*((short *)((char *)data + offset)) = (*spaceLeft)[currentPage];
			currentPage++;
			numOfPage++;
			offset += sizeof(short);
		}

		*((int *)data) = nextHeaderPage;
		*((int *)data + 1) = numOfPage;

		returnValue = fileHandle.writePage(currentHeaderPage, data);
	}

	free(data);

	return returnValue;
}

