
#ifndef _rm_h_
#define _rm_h_

#include <string.h>
#include <vector>
#include <map>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define SUCCESS 0
# define MAX_TABLE_RECORD_SIZE 768
# define MAX_COLUMNS_RECORD_SIZE 784
# define RM_EOF (-1)  // end of a scan operator
# define MAX_ATTRIBUTE_LENGTH 260

// RM_ScanIterator is an iteratr to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();


class RM_ScanIterator {
public:
	RM_ScanIterator();
	~RM_ScanIterator();

	FileHandle fileHandle;

	RC initialize(const vector<Attribute> &recordDescriptor, const CompOp compOp, const void *value,
			const vector<string> &attributeNames, const string &conditionAttribute);

	// "data" follows the same format as RelationManager::insertTuple()
	RC getNextTuple(RID &rid, void *data);
	RC close();

private:

	RBFM_ScanIterator rbfm_scanner;
	RecordBasedFileManager *rbfm;
};


class RM_IndexScanIterator {
public:
	RM_IndexScanIterator();  	// Constructor
	~RM_IndexScanIterator(); 	// Destructor

	// "key" follows the same format as in IndexManager::insertEntry()
	RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
	RC close();             			// Terminate index scan

	RC initialize(const Attribute keyAttribute,
			const void *lowKey,
			const void *highKey,
			bool lowKeyInclusive,
			bool highKeyInclusive);

	FileHandle indexFileHandle;

private:
	IX_ScanIterator ix_scanner;
	IndexManager *ix;
};


// Relation Manager
class RelationManager
{
public:
	static RelationManager* instance();

	RC createTable(const string &tableName, const vector<Attribute> &attrs);

	RC deleteTable(const string &tableName);

	RC getAttributes(const string &tableName, vector<Attribute> &attrs);

	RC insertTuple(const string &tableName, const void *data, RID &rid); //read the attributes from the attribute system table

	RC deleteTuples(const string &tableName); //just call deleteRecords in rbf

	RC deleteTuple(const string &tableName, const RID &rid);   //read the attributes from the attribute system table

	// Assume the rid does not change after update
	RC updateTuple(const string &tableName, const void *data, const RID &rid);   //read the attributes from the attribute system table

	RC readTuple(const string &tableName, const RID &rid, void *data);   //read the attributes from the attribute system table

	RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

	RC reorganizePage(const string &tableName, const unsigned pageNumber);  //call method in rbf

	// scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(const string &tableName,
			const string &conditionAttribute,
			const CompOp compOp,                  // comparision type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RM_ScanIterator &rm_ScanIterator);

	RC createIndex(const string &tableName, const string &attributeName);

	RC destroyIndex(const string &tableName, const string &attributeName);

	// indexScan returns an iterator to allow the caller to go through qualified entries in index
	RC indexScan(const string &tableName,
			const string &attributeName,
			const void *lowKey,
			const void *highKey,
			bool lowKeyInclusive,
			bool highKeyInclusive,
			RM_IndexScanIterator &rm_IndexScanIterator);

	// Extra credit
public:
	RC dropAttribute(const string &tableName, const string &attributeName);

	RC addAttribute(const string &tableName, const Attribute &attr);

	RC reorganizeTable(const string &tableName);

protected:
	RelationManager();
	~RelationManager();

private:
	static RelationManager *_rm;

	RecordBasedFileManager * rbfm;
	IndexManager * ix;
	PagedFileManager * pfm;


	RBFM_ScanIterator rbfmScanner;
	vector<Attribute> tableVec;
	vector<Attribute> columnVec;
	vector<Attribute> indexVec;

	// [tableName -> [tableID -> RID in tables.tbl]
	map<string, map<int, RID> *> tablesMap;

	// [tableID -> [column position -> RID in columns.tbl]]
	map<int, map<int, RID> *> columnsMap;

	// [tableID -> [column position -> RID in indice.tbl]]
	map<int, map<int, RID> *> indexMap;


	int TABLE_ID_COUNTER;


	void appendData(int fieldLength, int &offset, char * pageBuffer, const char * dataToWrite, AttrType attrType);

	RC insertTablesEntry(string tableName, string tableType, string fileName, FileHandle &fileHandle, int numOfCol, RID &rid);

	RC insertColumnsEntry(string tableName, string columnName, FileHandle &fileHandle, int colPosition, int maxLength, RID &rid, AttrType colType);

	RC insertIndexEntry(string tableName, string columnName, int tableID, int columnPos, FileHandle & fileHandle, RID &rid);

	short determineMemoryNeeded(const vector<Attribute> &attributes);

	void populateColumnsMap(RID &rid, int columnIndex);

	RC loadSystem();

	RC createTableHelper(const string &tableName, const vector<Attribute> & attr, const string & type);

	bool isSystemTableRequest(string tableName);

	int readFieldOffset(const void *data, int attrPosition, vector<Attribute> recordDescriptor);

	bool isFieldEqual(const char *a, const char *b, AttrType type);

};

#endif
