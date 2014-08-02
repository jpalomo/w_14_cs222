
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

struct Slot {
	short beginAddr; // begin offset of record
	short endAddr; // end offset of record
};

struct Footer {
	short numOfSlots;
	short reOrg;
	short freeSpaceOffset;
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
# define HEADER_PAGE_SLOT 2000
# define RECORD_OVERHEAD sizeof(Slot)
# define FOOTER_OVERHEAD sizeof(Footer)
# define SMALLEST_RECORD_LENGTH 10
//# define EMPTY_RECORD_PAGE_FREE_SPACE 4090


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
	RBFM_ScanIterator();
	~RBFM_ScanIterator();
    
	// "data" follows the same format as RecordBasedFileManager::insertRecord()
	RC getNextRecord(RID &rid, void *data);
	RC close();
	RC initialize(FileHandle &fileHandle,
                  const vector<Attribute> &recordDescriptor,
                  const CompOp compOp,
                  const void *value,
                  const vector<string> &attributeNames,
                  const string &conditionAttribute);
    
private:
	CompOp op;
	const void *condition;
	AttrType conditionAttrType;
	short conditionAttrNum;
    
	unsigned pageNum;
	unsigned slotNum;
    
	FileHandle fileHandle;
    
	vector<short> projAttrNum;
	vector<AttrType> projAttrType;
	map<unsigned, unsigned> tombStoneMap;
    
	char *page;
	char *endOfPagePtr;
	Footer *footerPtr;
    
	bool compare(void *attribute, const void *condition, AttrType type, CompOp compOp);
	RC readAttr(char *recordPtr, void *attribute, short attrNum, AttrType type, int &attrLength);
	RC projectAttr(char *recordPtr, void *data, vector<AttrType> projAttrType, vector<short> attrNum);

	int compareFloat(float a, float b) {
		if (a - b > 0.00001)
			return 1;
		else if (a - b < -0.00001)
			return -1;
		else
			return 0;
	}
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
			const CompOp compOp,                  // comparison type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RBFM_ScanIterator &rbfm_ScanIterator);
    
	RC printAttribute(const void *data, AttrType type);
    
    
	// Extra credit for part 2 of the project, please ignore for part 1 of the project
	RC reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor);
    bool fexist(string fileName);
    
    
protected:
	RecordBasedFileManager();
	~RecordBasedFileManager();
    
private:
	static RecordBasedFileManager *_rbf_manager;
	PagedFileManager * pfm;
	map<string, vector<short> * > filePageDirectory;
    
	void readFooter(void *footerPtr, short &reorgFlag, short &freeSpaceOffset, short &numberOfRecords);
	void initializeFooter(void *endOfPagePtr);
    
	RC prepareDataForNewPageWrite(const void *data, void *pageData, int dataLength);
	RC appendPageWithOneRecord(FileHandle &fileHandle, const void *data, int dataLength);
	// read one single header page, return the number of next header page, -1 if no next header page
	RC readHeaderPage(FileHandle &metaFileHandle, unsigned currentHeaderPage, vector<short> * spaceLeft);
	RC writeHeaderPage(FileHandle &metaFileHandle, unsigned currentHeaderPage, vector<short> * spaceLeft, unsigned &currentPage);
    
	short getRecordLength(const vector<Attribute> &recordDescriptor, const void *data); //get length of a tuple from descriptor and data
	RC encodeRecord(const vector<Attribute> recordDescriptor, const void *inputRecord, void *outputRecord);
	RC decodeRecord(const vector<Attribute> recordDescriptor, const void *inputRecord, void *outputRecord);
	RC appendRecord(char *page, const void *record, short recordLength, unsigned slotNum);
    
	/**
	 * this is a helper method
	 * if this record is a tomb stone, update pageNum and slotNum, return true, else return false
	 */
	bool isTombStone(const void *recordPtr, unsigned &pageNum, unsigned &slotNum) {
		if (*(short *)recordPtr == -1) {
			pageNum = *(unsigned *)((char *)recordPtr + sizeof(short));
			slotNum = *(unsigned *)((char *)recordPtr + sizeof(short) + sizeof(unsigned));
			return true;
		}
		return false;
	}
    
	void setAsTomb(char *recordPtr, unsigned pageNum, unsigned slotNum) {
		*(short *)recordPtr = -1;
		*(unsigned *)(recordPtr + sizeof(short)) = pageNum;
		*(unsigned *)(recordPtr + sizeof(short) + sizeof(unsigned)) = slotNum;
	}
    
	Slot * goToSlot(const void *endOfPagePtr, unsigned slotNum) {
		char *result = (char *)endOfPagePtr;
		result = result - FOOTER_OVERHEAD - slotNum * RECORD_OVERHEAD;
		return (Slot *)result;
	}
    
	Footer * goToFooter(const void *endOfPagePtr) {
		char *result = (char *)endOfPagePtr;
		result = result - FOOTER_OVERHEAD;
		return (Footer *)result;
	}


};

#endif
