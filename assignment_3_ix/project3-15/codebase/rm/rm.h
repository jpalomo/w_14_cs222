
#ifndef _rm_h_
#define _rm_h_

#include <string.h>
#include <vector>
#include <map>

#include "../rbf/rbfm.h"

using namespace std;

# define SUCCESS 0
# define MAX_TABLE_RECORD_SIZE 768
# define MAX_COLUMNS_RECORD_SIZE 784
# define RM_EOF (-1)  // end of a scan operator

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
    RBFM_ScanIterator rbfmScanner;
    vector<Attribute> tableVec;
    vector<Attribute> columnVec;

    map<string, map<int, RID> *> tablesMap;
  
    map<int, map<int, RID> *> columnsMap;

    int TABLE_ID_COUNTER;
    

    /***
     * Utiltity method to push an attribute onto the attribute vector.  
     * Wanted to centralize this logic so that code would not have to be 
     * duplicated.
     **/
    void pushAttribute(vector<Attribute> &tableTableDesc, const string attrName, AttrType attrType, int attrLength);
    
    void appendData(int fieldLength, int &offset, char * pageBuffer, const char * dataToWrite, AttrType attrType);
    
    void createTablesAttributesVector(const char * tableName, vector<Attribute> &tableAttribs, const char * tableType, const char * fileName);
    RC insertTablesEntry(string tableName, string tableType, string fileName, FileHandle &fileHandle, int numOfCol, RID &rid);
    
    void createAttributesForColumnsTable(const char * tableName, vector<Attribute> &tableAttribs, const char * columnName, const int colPosition);
    RC insertColumnsEntry(string tableName, string columnName, FileHandle &fileHandle, int colPosition, int maxLength, RID &rid, AttrType colType);

    short determineMemoryNeeded(const vector<Attribute> &attributes);
    
    void populateColumnsMap(RID &rid, int columnIndex);
    
    RC loadSystem();

    RC createTable(const string &tableName, const vector<Attribute> & attr, const string & type);
    
    bool isSystemTableRequest(string tableName);

};

#endif
