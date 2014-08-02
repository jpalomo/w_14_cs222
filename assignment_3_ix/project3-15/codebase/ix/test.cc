/*


 * test.cc
 *
 *  Created on: Feb 26, 2014
 *      Author: Flj





#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"

IndexManager *indexManager;

void prepareLeafPage(void *page) {

	LeafHeader * header = (LeafHeader *)page;

	header->freeSpace = PAGE_SIZE - sizeof(LeafHeader);
	header->freeSpaceOffset = PAGE_SIZE;
	header->pageType = Leaf;
	header->numOfRecords = 0;
	header->nextPage = NO_PAGE;
	header->prevPage = NO_PAGE;
}

void prepareIndexPage(void *page) {
	IndexHeader *header = (IndexHeader *)page;

	header->firstPtr = 1;
	header->freeSpace = PAGE_SIZE - sizeof(IndexHeader);
	header->freeSpaceOffset = PAGE_SIZE;
	header->pageType = Index;
	header->numOfRecords = 0;
}

void * testForLeafPageInsertion(IndexManager *indexManager) {
	void *page = malloc(PAGE_SIZE);
	prepareLeafPage(page);
	LeafHeader *leafHeader = (LeafHeader *)page;
	Attribute attribute;
	attribute.type = TypeVarChar;

	int returnValue;
	int numOfKeys = 50;
	// prepare keys
	for (int i = 0; i < numOfKeys; i++) {
		int r = rand() % 26;

		void *key = malloc(sizeof(int) + i + 1);
		*(int *)key = i + 1;
		char c = 'a' + r;
		cout << i << ": " << c;
		for (int j = 0; j < i + 1; j++) {
			memcpy((char *)key + sizeof(int) + j, &c, 1);
		}
		RID rid;
		rid.pageNum = i;
		rid.slotNum = i;

		returnValue = indexManager->insertKeyInLeafPage((char *)page, key, leafHeader, attribute, rid);
		if (returnValue != 0) {
			cout << " Duplicated!";
		}
		free(key);
		cout << endl;
	}

	cout << "freeSpace: " << leafHeader->freeSpace << endl;
	cout << "freeSpaceOffset: " << leafHeader->freeSpaceOffset << endl;
	cout << "number of records: " << leafHeader->numOfRecords << endl;
	cout << "------------------------------------------" << endl;

	for (int i = 0; i < leafHeader->numOfRecords; i++) {
		LeafSlot *slot;
		slot = indexManager->goToLeafSlot((char *)page, i);

		cout << "Slot " << i << ": " << endl;
		cout << "length: " << slot->length << endl;
		cout << "offset: " << slot->offset << endl;
		cout << "pageNum: " << slot->pageNum << endl;

		string s((char *)page + slot->offset, slot->length);
		cout << s << endl;

		cout << "------------------------------------------" << endl;
	}

	// prepare another key
	void *key = malloc(8 + sizeof(int));
	*(int *)key = 8;
	char c = 'b';
	for (int i = 0; i < 8; i++) {
		memcpy((char *)key + sizeof(int) + i, &c, 1);
	}
	RID rid;
	rid.pageNum = 23;
	rid.slotNum = 23;

	// should be inserted after 'bb'
	indexManager->insertKeyInLeafPage((char *)page, key, leafHeader, attribute, rid);

	for (int i = 0; i < leafHeader->numOfRecords; i++) {
		LeafSlot *slot;
		slot = indexManager->goToLeafSlot((char *)page, i);

		cout << slot->length << endl;
		cout << slot->offset << endl;
		cout << slot->pageNum << endl;

		string s((char *)page + slot->offset, slot->length);
		cout << s << endl;

		cout << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << endl;
	}


	free(key);

	cout << "freeSpace: " << leafHeader->freeSpace << endl;
	cout << "freeSpaceOffset: " << leafHeader->freeSpaceOffset << endl;
	cout << "number of records: " << leafHeader->numOfRecords << endl;
	cout << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << endl;

	return page;
}


void * testForIndexPageInsertion(IndexManager *indexManager) {
	void *page = malloc(PAGE_SIZE);
	prepareIndexPage(page);
	IndexHeader *indexHeader = (IndexHeader *)page;

	Attribute attribute;
	attribute.type = TypeVarChar;

	unsigned numOfKeys = 50;

	for (unsigned i = 0; i < numOfKeys; i++) {
		int r = rand() % 26;
		char c = 'a' + r;
		cout << i << ": " << c << endl;

		void *key = malloc(i + 1 + sizeof(int));
		*(int *)key = i + 1;
		for (unsigned j = 0; j < i + 1; j++)
			memcpy((char *)key + sizeof(int) + j, &c, 1);

		indexManager->insertEntryInIndexPage((char *)page, key, indexHeader, attribute, i);
		free(key);
	}

	cout << "free space: " << indexHeader->freeSpace << endl;
	cout << "free space offset: " << indexHeader->freeSpaceOffset << endl;
	cout << "number of records: " << indexHeader->numOfRecords << endl;
	cout << "------------------------------------" << endl;

	for (short i = 0; i < indexHeader->numOfRecords; i++) {
		IndexSlot *slot = indexManager->goToIndexSlot((char *)page, i);

		cout << "slot " << i << ": " << endl;
		cout << "length: " << slot->length << endl;
		cout << "offset: " << slot->offset << endl;
		cout << "pointer: " << slot->ptr << endl;

		string s((char *)page + slot->offset, slot->length);
		cout << "key: " << s << endl;

		cout << "------------------------------------" << endl;
	}

	return page;
}


void testForLeafSplit(IndexManager *indexManager, void *page) {
	LeafHeader *header = (LeafHeader *)page;

	void *newPage = malloc(PAGE_SIZE);
	LeafHeader *newPageHeader = (LeafHeader *)newPage;

	indexManager->copyLeafKeysInOrder((char *)page, (char *)newPage, 3, 4, TypeVarChar);

	cout << "existing page: " << endl;
	cout << "free space: " << header->freeSpace << endl;
	cout << "free space offset: " << header->freeSpaceOffset << endl;
	cout << "number of records: " << header->numOfRecords << endl;
	cout << "previous page: " << header->prevPage << endl;
	cout << "next page: " << header->nextPage << endl;
	cout << "------------------------------------------" << endl;

	for (int i = 0; i < header->numOfRecords; i++) {
		LeafSlot *slot = indexManager->goToLeafSlot((char *)page, i);

		cout << "Slot " << i << ": " << endl;
		cout << "length: " << slot->length << endl;
		cout << "offset: " << slot->offset << endl;
		cout << "pageNum: " << slot->pageNum << endl;

		string s((char *)page + slot->offset, slot->length);
		cout << s << endl;

		cout << "------------------------------------------" << endl;
	}

	cout << "new created page: " << endl;
	cout << "free space: " << newPageHeader->freeSpace << endl;
	cout << "free space offset: " << newPageHeader->freeSpaceOffset << endl;
	cout << "number of records: " << newPageHeader->numOfRecords << endl;
	cout << "previous page: " << newPageHeader->prevPage << endl;
	cout << "next page: " << newPageHeader->nextPage << endl;
	cout << "------------------------------------------" << endl;

	for (int i = 0; i < newPageHeader->numOfRecords; i++) {
		LeafSlot *slot = indexManager->goToLeafSlot((char *)newPage, i);

		cout << "Slot " << i << ": " << endl;
		cout << "length: " << slot->length << endl;
		cout << "offset: " << slot->offset << endl;
		cout << "pageNum: " << slot->pageNum << endl;

		string s((char *)newPage + slot->offset, slot->length);
		cout << s << endl;

		cout << "------------------------------------------" << endl;
	}

	free(page);
	free(newPage);

}


void testForIndexSplit(IndexManager *indexManager, void *page) {
	IndexHeader *header = (IndexHeader *)page;

	void *newPage = malloc(PAGE_SIZE);
	IndexHeader *newPageHeader = (IndexHeader *)newPage;

	SplitInfo splitInfo;
	splitInfo.handleSplit = false;
	indexManager->copyIndexEntriesInOrder((char *)page, (char *)newPage, TypeVarChar, splitInfo);

	cout << "existing page: " << endl;
	cout << "free space: " << header->freeSpace << endl;
	cout << "free space offset: " << header->freeSpaceOffset << endl;
	cout << "number of records: " << header->numOfRecords << endl;
	cout << "first pointer: " << header->firstPtr << endl;
	cout << "------------------------------------------" << endl;

	for (int i = 0; i < header->numOfRecords; i++) {
		IndexSlot *slot = indexManager->goToIndexSlot((char *)page, i);

		cout << "Slot " << i << ": " << endl;
		cout << "length: " << slot->length << endl;
		cout << "offset: " << slot->offset << endl;
		cout << "pointer: " << slot->ptr << endl;

		string s((char *)page + slot->offset, slot->length);
		cout << s << endl;

		cout << "------------------------------------------" << endl;
	}

	cout << "new created page: " << endl;
	cout << "free space: " << newPageHeader->freeSpace << endl;
	cout << "free space offset: " << newPageHeader->freeSpaceOffset << endl;
	cout << "number of records: " << newPageHeader->numOfRecords << endl;
	cout << "first pointer: " << newPageHeader->firstPtr << endl;
	cout << "------------------------------------------" << endl;

	for (int i = 0; i < newPageHeader->numOfRecords; i++) {
		IndexSlot *slot = indexManager->goToIndexSlot((char *)newPage, i);

		cout << "Slot " << i << ": " << endl;
		cout << "length: " << slot->length << endl;
		cout << "offset: " << slot->offset << endl;
		cout << "pointer: " << slot->ptr << endl;

		string s((char *)newPage + slot->offset, slot->length);
		cout << s << endl;

		cout << "------------------------------------------" << endl;
	}

	cout << "middle entry: " << endl;
	cout << "------------------------------------------" << endl;

	cout << "backup SplitInfo: " << splitInfo.handleSplit << endl;
	string s((char *)splitInfo.key + sizeof(int), *(int *)splitInfo.key);
	cout << s << endl;

	free(page);
	free(newPage);

}


void insertTest(IndexManager *im) {
	string fileName = "bplustest";
	// PagedFileManager * pfm = PagedFileManager::instance();

	if (pfm->fexist(fileName)) {
		im->destroyFile(fileName);
	}

	int returnValue = im->createFile(fileName);

	if(returnValue == SUCCESS) {
		cout << "successfully create the file" << endl;
	}
	else{
		cout << "couldn't create the file" << endl;
		return;
	}

	FileHandle fileHandle;
	returnValue = im->openFile(fileName, fileHandle);

	if(returnValue == SUCCESS) {
		cout << "successfully opened the file" << endl;
	}
	else{
		cout << "couldn't open the file" << endl;
		return;
	}

	Attribute attr;
	attr.type = TypeInt;
	attr.length = 4;

	RID rid;
	rid.pageNum = 0;
	rid.slotNum = 0;

	for (int i = 1; i <= 275; i++) {
		if (i == 258){
			cout << i <<endl;
		}
		void * key = malloc(sizeof(int));

		memcpy(key, &i, sizeof(int));
		returnValue = im->insertEntry(fileHandle, attr, key, rid);

		if(returnValue != SUCCESS) {
			cout << "couldn't insert" << i << endl;
			free(key);
		}

		rid.slotNum++;
		rid.pageNum++;
		free(key);

	}

	im->closeFile(fileHandle);


	// returnValue = pfm->openFile(fileName.c_str(), fileHandle);

	if (returnValue != SUCCESS) {
		cout << "couldn't open file " << fileName << " through pfm" << endl;
		return;
	}

	//read the header
	unsigned header = 0;
	char * page = (char*) malloc(PAGE_SIZE);
	returnValue = fileHandle.readPage(header, page);

	if (returnValue != SUCCESS) {
		cout << "couldn't read header page " << header << endl;
		free(page);
		return;
	}

	int rootPage;
	memcpy(&rootPage, page, sizeof(unsigned));
	cout << "root page page number is: " << rootPage << endl;

	returnValue = fileHandle.readPage(rootPage, page);

	if (returnValue != SUCCESS) {
		cout << "couldn't read index page: " << rootPage << endl;
		free(page);
		return;
	}

	IndexHeader * ih = (IndexHeader * ) malloc(sizeof(IndexHeader));
	memcpy(ih, page, sizeof(IndexHeader));

	cout << endl;
	cout << "Index Header Values....." << endl;
	cout << "page type: " << ih->pageType << endl;
	cout << "num of records: " << ih->numOfRecords << endl;
	cout << "first pointer page: " << ih->firstPtr << endl;
	cout << "free space: " << ih->freeSpace << endl;
	cout << "free space offset: " << ih->freeSpaceOffset << endl;

	IndexSlot * firstSlot = (IndexSlot*) malloc(sizeof(IndexSlot));

	memcpy(firstSlot, page + sizeof(IndexHeader), sizeof(IndexSlot));


	if(ih->numOfRecords > 0) {

		//        IndexSlot * firstSlot = (IndexSlot*) malloc(sizeof(IndexSlot));
		//
		//        memcpy(firstSlot, page + sizeof(IndexHeader), sizeof(IndexSlot));
		//
		//
		////        char * rec = (char*) malloc(firstSlot->length);
		////
		////        memcpy(rec, page + firstSlot->offset, firstSlot->length);
		//
		//        cout << *(int*) (page + firstSlot->offset) << endl;
		//
		//        free(firstSlot);

	}

	free(ih);

	for (int k = 2; k<=3; k++){
		returnValue = fileHandle.readPage(k, page);


		if(returnValue != SUCCESS) {
			cout << "couldnt read the leaf page" << endl;
			free(page);
			return;
		}

		LeafHeader * lh = (LeafHeader *) malloc(sizeof(LeafHeader));
		memcpy(lh, page, sizeof(LeafHeader));

		cout << endl;
		cout << "Leaf Header Values....." << endl;
		cout << "page type: " << lh->pageType << endl;
		cout << "num of records: " << lh->numOfRecords << endl;
		cout << "free space: " << lh->freeSpace << endl;
		cout << "free space offset: " << lh->freeSpaceOffset << endl;
		cout << "next o-flow page: " << lh->nextOverFlowPage << endl;
		cout << "next page: " << lh->nextPage << endl;
		cout << "prev page: " << lh->prevPage << endl;

		LeafSlot *ls;

		for(short i = 0; i < lh->numOfRecords; i++) {

			if(i == 127){
				;
			}

			char * beginPage = (page + sizeof(LeafHeader)) + (sizeof(LeafSlot) * (i));
			ls = (LeafSlot*)beginPage;


			cout << endl;
			cout << endl;
			cout << "ENTRY :" << i << endl;
			cout << "length: " << ls->length << endl;
			cout << "offset: " << ls->offset << endl;
			cout << "pageNum: " << ls->pageNum << endl;
			cout << "slotNum: " << ls->slotNum << endl;

			int * key = (int*) malloc(sizeof(int));
			memcpy(key, page + ls->offset, sizeof(int));
			cout << "offset  :" << ls->offset << endl;
			cout << "Key Entry is: " << *key << endl;
		}
		free(lh);
	}
}


int main() {
	indexManager = IndexManager::instance();

//	void *page = testForLeafPageInsertion(indexManager);

//	testForLeafSplit(indexManager, page);

//    void *indexPage = testForIndexPageInsertion(indexManager);

//    testForIndexSplit(indexManager, indexPage);

    insertTest(indexManager);
}

*/
