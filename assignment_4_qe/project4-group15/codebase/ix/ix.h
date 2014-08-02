
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define NO_PAGE (0)
# define DEBUG 1
# define SUCCESS 0
# define LEFT_MOST_PAGE_NUM 2

typedef enum {Root=0, Index, Leaf, Overflow } PageType;

struct LeafHeader {
	PageType pageType;
	short numOfRecords;
	short freeSpace;
	short freeSpaceOffset;
	unsigned nextOverFlowPage;
	unsigned nextPage;
	unsigned prevPage;
};



struct LeafSlot {
	short offset;
	short length;
	unsigned pageNum;
	unsigned slotNum;
};



struct IndexHeader {
	PageType pageType;
	short numOfRecords;
	short freeSpace;
	short freeSpaceOffset;
	unsigned firstPtr;
};



struct IndexSlot {
	short offset;
	short length;
	unsigned ptr;
};

struct EID {
	unsigned pageNum;
	unsigned slotNum;
};

struct SplitInfo {
	bool handleSplit;
    void * key;
    unsigned pageNo;
};



class IX_ScanIterator;

class IndexManager {
public:
	static IndexManager* instance();

	RC createFile(const string &fileName);

	RC destroyFile(const string &fileName);

	RC openFile(const string &fileName, FileHandle &fileHandle);

	RC closeFile(FileHandle &fileHandle);

	// The following two functions are using the following format for the passed key value.
	//  1) data is a concatenation of values of the attributes
	//  2) For int and real: use 4 bytes to store the value;
	//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
	RC insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Insert new index entry
	RC deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Delete index entry
	RC searchEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, RID &rid, EID &entryId);  // search index entry according to the key


	// scan() returns an iterator to allow the caller to go through the results
	// one by one in the range(lowKey, highKey).
	// For the format of "lowKey" and "highKey", please see insertEntry()
	// If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
	// should be included in the scan
	// If lowKey is null, then the range is -infinity to highKey
	// If highKey is null, then the range is lowKey to +infinity
	RC scan(FileHandle &fileHandle,
			const Attribute &attribute,
			const void        *lowKey,
			const void        *highKey,
			bool        lowKeyInclusive,
			bool        highKeyInclusive,
			IX_ScanIterator &ix_ScanIterator);

private:
	RC findNextValidSlot(FileHandle &fileHandle, EID &entryId);
	RC findSuccessorForStart(FileHandle &fileHandle, const void *key, AttrType type, EID &entryId, bool isInclusive);
	RC findSuccessorForEnd(FileHandle &fileHandle, const void *key, AttrType type, EID &entryId, bool isInclusive);

protected:
	IndexManager   ();                            // Constructor
	~IndexManager  ();                            // Destructor

private:
	static IndexManager *_index_manager;
	PagedFileManager *pfm;
	map<string, unsigned> rootPageMap;


	int compare(const void *key, const void *data, AttrType attrType, int dataLength);
	short indexBinarySearch(const void *key, void *page, short numOfRecords, AttrType attrType);
	short leafBinarySearch(const void *key, void *page, short numOfRecords, AttrType attrType, bool &isEqual);
	void reorgLeafPage(void *page);
	void reorgIndexPage(void *page);

	LeafSlot * BTreeSearch(FileHandle &fileHandle, unsigned pageNum, AttrType attrType, const void *key, EID &entryId, bool &isSuccess, bool &isNegOne);

	IndexSlot *goToIndexSlot(const char *page, short slotNum) {
		return (IndexSlot *)(page + sizeof(IndexHeader) + slotNum * sizeof(IndexSlot));
	}

	LeafSlot *goToLeafSlot(const char *page, short slotNum) {
		return (LeafSlot *)(page + sizeof(LeafHeader) + slotNum * sizeof(LeafSlot));
	}

	RC insert(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid, unsigned pageNo, SplitInfo &splitInfo);
	int getKeyLength(const void *key, AttrType attrType);

	RC insertKeyInLeafPage(char * pageIn, const void * key, LeafHeader * leafHeader, const Attribute &attribute, const RID &rid);
	RC insertEntryInIndexPage(char *pageIn, const void *key, IndexHeader *indexHeader, const Attribute &attribute, unsigned &pagePointer);


	void copyLeafKeysInOrder(char * leafPage, char * newLeafPage, unsigned currentPageNo, unsigned newPageNo, AttrType type);
	void copyIndexEntriesInOrder(char * indexPage, char * newIndexPage, AttrType attrType, SplitInfo &splitInfo);
};

class IX_ScanIterator {
public:
	IX_ScanIterator();  							// Constructor
	~IX_ScanIterator(); 							// Destructor

	RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
	RC close();             						// Terminate index scan
	RC initialize(FileHandle &fileHandle, const EID &startEid, const EID &endEid, AttrType type);

private:
	EID currentEid;
	EID stopEid;
	AttrType attrType;

	FileHandle fileHandle;

	char *page;
	LeafHeader *headerPtr;

};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
