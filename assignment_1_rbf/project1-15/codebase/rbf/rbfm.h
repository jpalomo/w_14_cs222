
#ifndef _rbfm_h_
#define _rbfm_h_

#include <string.h>
#include <vector>
#include <iostream>
#include <stdlib.h>

#include "../rbf/pfm.h"

using namespace std;


// Record ID
typedef struct
{
	unsigned pageNum;
	unsigned slotNum;
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
	string   name;     // attribute name
	AttrType type;     // attribute type
	AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0,  // =
	LT_OP,      // <
	GT_OP,      // >
	LE_OP,      // <=
	GE_OP,      // >=
	NE_OP,      // !=
	NO_OP       // no condition
} CompOp;



/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project 
*****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator
# define HEADER_PAGE_SLOT 2040
# define RECORD_OVERHEAD 4
# define FOOTER_OVERHEAD 6
# define SMALLEST_RECORD_LENGTH 10


//  RBFM_ScanIterator is an iteratr to go through records
//  The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();


class RBFM_ScanIterator {
public:
	RBFM_ScanIterator() {};
	~RBFM_ScanIterator() {};

	// "data" follows the same format as RecordBasedFileManager::insertRecord()
	RC getNextRecord(RID &rid, void *data) { return RBFM_EOF; };
	RC close() { return -1; };
};


class RecordBasedFileManager
{
public:
	static RecordBasedFileManager* instance();

	RC createFile(const string &fileName);

	RC destroyFile(const string &fileName);

	RC openFile(const string &fileName, FileHandle &fileHandle);

	RC closeFile(FileHandle &fileHandle);

	//  Format of the data passed into the function is the following:
	//  1) data is a concatenation of values of the attributes
	//  2) For int and real: use 4 bytes to store the value;
	//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
	//  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
	RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

	RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

	// This method will be mainly used for debugging/testing
	RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

	/**************************************************************************************************************************************************************
	 ***************************************************************************************************************************************************************
     IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
	 ***************************************************************************************************************************************************************
	 ***************************************************************************************************************************************************************/
	RC deleteRecords(FileHandle &fileHandle);

	RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

	// Assume the rid does not change after update
	RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

	RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data);

	RC reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber);

	// scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor,
			const string &conditionAttribute,
			const CompOp compOp,                  // comparision type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RBFM_ScanIterator &rbfm_ScanIterator);


	// Extra credit for part 2 of the project, please ignore for part 1 of the project
public:

	RC reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor);


protected:
	RecordBasedFileManager();
	~RecordBasedFileManager();

private:
	static RecordBasedFileManager *_rbf_manager;
	PagedFileManager * pfm;
	map<string, vector<short> * > filePageDirectory;

	void readFooter(void *footerPtr, short &reorgFlag, short &freeSpaceOffset, short &numberOfRecords);
	RC prepareDataForNewPageWrite(const void *data, void *pageData, int dataLength);
	RC appendPageWithOneRecord(FileHandle &fileHandle, const void *data, int dataLength);
	// read one single header page, return the number of next header page, -1 if no next header page
	RC readHeaderPage(FileHandle &fileHandle, int &currentHeaderPage, vector<short> * spaceLeft);
	RC writeHeaderPage(FileHandle &fileHandle, int currentHeaderPage, int &nextHeaderPage, vector<short> * spaceLeft, unsigned &startPage);

public:
	unsigned getRecordLength(const vector<Attribute> &recordDescriptor, const void *data); //get length of a tuple from descriptor and data
	RC encodeRecord(const vector<Attribute> recordDescriptor, const void *inputRecord, void *outputRecord);
	RC decodeRecord(const vector<Attribute> recordDescriptor, const void *inputRecord, void *outputRecord, unsigned &pageNum, unsigned &slotNum);
};

#endif
