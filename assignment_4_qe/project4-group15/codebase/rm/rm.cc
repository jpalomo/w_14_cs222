
#include "rm.h"
#include <iostream>
RelationManager* RelationManager::_rm = 0;

/**************************************************************************************************************
 * Checks the value of _rm and if it is 0, creates a new instance
 * of this class.
**************************************************************************************************************/
RelationManager* RelationManager::instance()
{
    if(!_rm) {
        _rm = new RelationManager();
    }

    return _rm;
}

RelationManager::~RelationManager()
{
    //set this singleton to null
    _rm = NULL;

    // delete information in tableMap and columnMap
    for (map<string, map<int, RID> *>::iterator it = tablesMap.begin(); it != tablesMap.end(); ++it) {
    	delete it->second;
    }

    for (map<int, map<int, RID> *>::iterator it = columnsMap.begin(); it != columnsMap.end(); ++it) {
    	delete it->second;
    }

    for (map<int, map<int, RID> *>::iterator it = indexMap.begin(); it != indexMap.end(); ++it) {
    	delete it->second;
    }
}

RelationManager::RelationManager() : TABLE_ID_COUNTER(1)
{
    //get the instance
    rbfm = RecordBasedFileManager::instance();
    ix = IndexManager::instance();
    pfm = PagedFileManager::instance();
    
    //build a column record descriptors for catalog tables for ease of use
    Attribute attr;
    attr.name = "TableId";
    attr.type = TypeInt;
    attr.length = 4;
    
    tableVec.push_back(attr);
    attr.name = "TableName";
    attr.type = TypeVarChar;
    attr.length = 256;
    tableVec.push_back(attr);

    attr.name = "TableType";
    attr.type = TypeVarChar;
    attr.length = 256;
    tableVec.push_back(attr);

    attr.name = "FileName";
    attr.type = TypeVarChar;
    attr.length = 256;
    tableVec.push_back(attr);

    attr.name = "NumOfColumns";
    attr.type = TypeInt;
    attr.length = 4;
    tableVec.push_back(attr);
    
    attr.name = "TableId";
    attr.length = 4;
    attr.type = TypeInt;
    columnVec.push_back(attr);
    
    attr.name = "TableName";
    attr.length = 256;
    attr.type = TypeVarChar;
    columnVec.push_back(attr);
    
    attr.name = "ColumnName";
    attr.length = 256;
    attr.type = TypeVarChar;
    columnVec.push_back(attr);
    
    attr.name = "ColumnType";
    attr.length = 256;
    attr.type = TypeVarChar;
    columnVec.push_back(attr);
    
    attr.name = "ColumnPosition";
    attr.length = 4;
    attr.type = TypeInt;
    columnVec.push_back(attr);
    
    attr.name = "MaxLength";
    attr.length = 4;
    attr.type = TypeInt;
    columnVec.push_back(attr);
    
    attr.name = "TableID";
    attr.length = 4;
    attr.type = TypeInt;
    indexVec.push_back(attr);

    attr.name = "TableName";
    attr.length = 256;
    attr.type = TypeVarChar;
    indexVec.push_back(attr);

    attr.name = "ColumnPosition";
    attr.length = 4;
    attr.type = TypeInt;
    indexVec.push_back(attr);

    attr.name = "ColumnName";
    attr.length = 256;
    attr.type = TypeVarChar;
    indexVec.push_back(attr);


    //this is not the first time the database has been initialized, load up the maps
    if (rbfm->fexist("tables.tbl")) {
        loadSystem();
    }
    else {
        createTableHelper("tables", tableVec, "System");
        createTableHelper("columns", columnVec, "System");
        createTableHelper("indices", indexVec, "System");
    }
}



RC RelationManager::loadSystem() {
    
    RM_ScanIterator rmsi;
    vector<string> attributeNames;
    RID rid;

    // load tables.tbl into tableMap
    attributeNames.push_back(tableVec[0].name); //table id
    attributeNames.push_back(tableVec[1].name); //table name
    
    scan("tables", tableVec[0].name , NO_OP, NULL, attributeNames, rmsi);

    char * data = (char*) malloc(MAX_TABLE_RECORD_SIZE);
    char * beginOfData = data;
        
    int maxTableId = 0;
    int tableId = 0;
    while(rmsi.getNextTuple(rid, data) != RM_EOF) {
        // read table id
    	memcpy(&tableId, data, sizeof(int));
        
    	// read table name
        int tableNameLength;
        data = data + sizeof(int); //jump over the just read value
        memcpy(&tableNameLength, data , sizeof(int));
        
        data = data + sizeof(int);
        string tableName = string(data, tableNameLength);


        if(tableId > maxTableId)
            maxTableId = tableId;
        
        map<int, RID> * tableIDToRidMap = new map<int, RID>();
        (*tableIDToRidMap)[tableId] = rid;
        
        tablesMap[tableName] = tableIDToRidMap;
        data = beginOfData;
    }
    
    TABLE_ID_COUNTER = maxTableId + 1;
    rmsi.close();
    
    // load columns.tbl into columnMap
    attributeNames.clear();
    attributeNames.push_back(columnVec[0].name); //table id
    attributeNames.push_back(columnVec[4].name); //column position
    scan("columns", columnVec[0].name , NO_OP, NULL, attributeNames, rmsi);

    int columnPosition;
    while(rmsi.getNextTuple(rid, data) != RM_EOF) {
        // read table id
    	memcpy(&tableId, data, sizeof(int));
        
    	// read column position
        data = data + sizeof(int);
        memcpy(&columnPosition, data, sizeof(int));

        // if this entry in tableMap has been created
        if(columnsMap.find(tableId) != columnsMap.end()) {
            (*columnsMap[tableId])[columnPosition] = rid;
        }
        else {
        	map<int, RID> * colEntryMap = new map<int, RID>();
        	(*colEntryMap)[columnPosition] = rid;
        	(columnsMap[tableId]) = colEntryMap;
        }

        data = beginOfData;
    }
    rmsi.close();
    
    // load indices.tbl into indexMap
    attributeNames.clear();
    attributeNames.push_back(indexVec[0].name); //table id
    attributeNames.push_back(indexVec[2].name); //column position
    scan("indices", indexVec[0].name , NO_OP, NULL, attributeNames, rmsi);
    while(rmsi.getNextTuple(rid, data) != RM_EOF) {
    	// read table id
    	memcpy(&tableId, data, sizeof(int));

    	// read column position
    	data = data + sizeof(int);
    	memcpy(&columnPosition, data, sizeof(int));

    	// if this entry in tableMap has been created
    	if(indexMap.find(tableId) != indexMap.end()) {
    		(*indexMap[tableId])[columnPosition] = rid;
    	}
    	else {
    		map<int, RID> * indexEntryMap = new map<int, RID>();
    		(*indexEntryMap)[columnPosition] = rid;
    		(indexMap[tableId]) = indexEntryMap;
    	}

    	data = beginOfData;
    }
    rmsi.close();

    free(beginOfData);
    
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	if (tableName.compare("tables") == 0 || tableName.compare("columns") == 0 || tableName.compare("indices") == 0) {
		std::cout << "Table name has been used by the system, please change table name!" << std::endl;
		return -1;
	}

    return createTableHelper(tableName, attrs, "user");
}


// a helper method to create table
RC RelationManager::createTableHelper(const string &tableName, const vector<Attribute> & attrs, const string & type) {
    
	int returnValue = -1;
	FileHandle fileHandle;
	string fileName = tableName + ".tbl";
	RID rid;

	if (fileName.compare("columns.tbl") != 0) {
		returnValue = rbfm->createFile(fileName);
		if (returnValue != SUCCESS) {
			return -1;
		}
	}

	if (fileName.compare("tables.tbl") == 0) {
		returnValue = rbfm->createFile("columns.tbl");
		if(returnValue != SUCCESS) {
			return -1;
		}
	}

	returnValue = rbfm->openFile("tables.tbl", fileHandle);
	if(returnValue != SUCCESS) {
		return -1;
	}

	int numOfCol = (int)attrs.size();
	//insert the entry for the new table in table.tbl
	returnValue = insertTablesEntry(tableName, type, fileName, fileHandle, numOfCol, rid);

	map<int, RID> * tableIDToRidMap = new map<int, RID>;
	(*tableIDToRidMap)[TABLE_ID_COUNTER] = rid;
	tablesMap[tableName] = tableIDToRidMap;

	if(returnValue != SUCCESS) {
		return -1;
	}

	// close the file Handle for "tables.tbl"
	returnValue = rbfm->closeFile(fileHandle);
	if(returnValue != SUCCESS) {
		return -1;
	}

	returnValue = rbfm->openFile("columns.tbl", fileHandle);
	if(returnValue != SUCCESS) {
		return -1;
	}

	//create the columns entries for the columns.tbl
	for (int i = 0; i < numOfCol; i++){
		int columnPosition = i + 1;
		insertColumnsEntry(tableName, attrs[i].name, fileHandle, columnPosition, attrs[i].length, rid, attrs[i].type);
		populateColumnsMap(rid, columnPosition);
	}

	if(returnValue == SUCCESS) {
		TABLE_ID_COUNTER += 1;
	}
	return rbfm->closeFile(fileHandle);
}


void RelationManager::populateColumnsMap(RID &rid, int columnPosition)
{
    if(columnsMap.find(TABLE_ID_COUNTER) != columnsMap.end()) {
        (*columnsMap[TABLE_ID_COUNTER])[columnPosition] = rid;
    }
    else {
    	map<int, RID> * colEntryMap = new map<int, RID>();
    	(*colEntryMap)[columnPosition] = rid;
    	(columnsMap[TABLE_ID_COUNTER]) = colEntryMap;
    }
}



/**************************************************************************************************************
 * This method will insert a single entry into the TABLES table.  It takes the given attribute vector
 * describing the record that will be inserted, mallocs the size of memory needed to house this record based on
 * the attribute vector, then lays the values of the attribute in the memory.  It then uses the RBFM to insert
 * a single entry into the table.
**************************************************************************************************************/
RC RelationManager::insertTablesEntry(string tableName, string tableType, string fileName, FileHandle &fileHandle, int numOfCol, RID &rid)
{
    char * recordBuffer = (char*) malloc(determineMemoryNeeded(tableVec));  //get a pointer to some raw memory

    int offset = 0;
    appendData(tableVec[0].length, offset, recordBuffer, (char*) &TABLE_ID_COUNTER, tableVec[0].type); //copy table id to buffer
    appendData((int)tableName.size(), offset, recordBuffer, tableName.c_str(), tableVec[1].type); //copy the table name to buffer
    appendData((int)tableType.size(), offset, recordBuffer, tableType.c_str(), tableVec[2].type);  //copy table type to buffer
    appendData((int)fileName.size(), offset, recordBuffer, fileName.c_str(), tableVec[3].type);  //copy the file name of the table to buffer
    appendData(tableVec[4].length, offset, recordBuffer, (char*) &numOfCol, tableVec[4].type); //copy number of columns fortable to buffer

    int returnValue = rbfm->insertRecord(fileHandle, tableVec, recordBuffer, rid);

    if(returnValue != SUCCESS) {
        free(recordBuffer);  //release the memory
        return -1;
    }

    free(recordBuffer);  //release the memory

    return returnValue;
}


RC RelationManager::insertColumnsEntry(string tableName, string columnName, FileHandle &fileHandle, int colPosition, int maxLength, RID &rid, AttrType colType)
{
    int memorySize = determineMemoryNeeded(columnVec);
    char * recordBuffer = (char*) malloc(memorySize);
    
    int offset = 0;
    
    appendData(columnVec[0].length, offset, recordBuffer, (char*)&TABLE_ID_COUNTER, columnVec[0].type);  //table_id
    appendData((int)tableName.size(), offset, recordBuffer, tableName.c_str(), columnVec[1].type); //table_name
    appendData((int)columnName.size(), offset, recordBuffer, columnName.c_str(), columnVec[2].type); //column_name

    string type;
    if (colType == TypeVarChar) {
        type = "var_char";
    }
    else if (colType == TypeInt) {
        type = "int";
    }
    else if (colType == TypeReal) {
        type = "real";
    }
    
    appendData((int)type.size(), offset, recordBuffer, type.c_str(), columnVec[3].type); //column_type
    appendData(columnVec[4].length, offset, recordBuffer, (char*)&colPosition, columnVec[4].type); //column_position
    appendData(columnVec[5].length, offset, recordBuffer,  (char*) &maxLength, columnVec[5].type); //column_length
    
    int returnValue = rbfm->insertRecord(fileHandle, columnVec, recordBuffer, rid);

    free(recordBuffer);
    
    return returnValue;
}

RC RelationManager::insertIndexEntry(string tableName, string columnName, int tableID, int columnPos, FileHandle &fileHandle, RID &rid) {
	char * recordBuffer = (char *)malloc(determineMemoryNeeded(indexVec));

	int offset = 0;

	appendData(indexVec[0].length, offset, recordBuffer, (char *)&tableID, indexVec[0].type); //table id
	appendData(tableName.size(), offset, recordBuffer, tableName.c_str(), indexVec[1].type); //table name
	appendData(indexVec[2].length, offset, recordBuffer, (char *)&columnPos, indexVec[2].type); // column Position
	appendData(columnName.size(), offset, recordBuffer, columnName.c_str(), indexVec[3].type); // column name

	int returnValue = rbfm->insertRecord(fileHandle, indexVec, recordBuffer, rid);

	free(recordBuffer);

	return returnValue;
}


RC RelationManager::deleteTable(const string &tableName)
{
    int returnValue = -1;
    
    if (isSystemTableRequest(tableName)) {
    	cout << "Invalid request to delete a system table: " + tableName << endl;
    	return -1;
    }

    //check if table does not exist in the map
    if (tablesMap.find(tableName) == tablesMap.end()) {
    	return returnValue;
    }


    map<int, RID> * tableID = tablesMap[tableName];
    int table_ID = (*tableID).begin()->first;


    RID rid;
    FileHandle fileHandle;

    //********operations for deleting associated index files, delete tuples in indices.tbl and clear indexMap*********


    vector<Attribute> recordDescriptor;
    returnValue = getAttributes(tableName, recordDescriptor);

    if (returnValue != SUCCESS)
    	return returnValue;

    if (indexMap.find(table_ID) != indexMap.end()) { // if this table has index file(s)
    	map<int, RID> * indexEntry = indexMap[table_ID];

    	returnValue = rbfm->openFile("indices.tbl", fileHandle);
        
        if(returnValue != SUCCESS) {
            return returnValue;
        }

    	for (map<int, RID>::iterator itr = indexEntry->begin(); itr != indexEntry->end(); itr++) {
    		int position = itr->first;
    		rid = itr->second;

    		string columnName = recordDescriptor[position - 1].name;
    		string indexFileName = tableName + "_" + columnName + ".idx";

    		// destroy index file
    		returnValue = ix->destroyFile(indexFileName);
    		if (returnValue != SUCCESS) {
    			rbfm->closeFile(fileHandle);
    			return -1;
    		}

    		// delete tuple in "indices.tbl"
    		returnValue = rbfm->deleteRecord(fileHandle, indexVec, rid);
    		if (returnValue != SUCCESS) {
    			rbfm->closeFile(fileHandle);
    			return -1;
    		}
    	}

    	delete(indexEntry);
    	indexMap.erase(table_ID);

    	returnValue = rbfm->closeFile(fileHandle);
    	if (returnValue != SUCCESS) {
    		return -1;
    	}
    }

    //*******************operations for deleting tuples in columns.tbl and clear columnsMap******************
    //get the map of all the columns for a particular table id
    map<int, RID> * columnsEntries = columnsMap[table_ID];

    //open the columns table to get the handle

    returnValue = rbfm->openFile("columns.tbl", fileHandle);
    if (returnValue != SUCCESS) {
    	rbfm->closeFile(fileHandle);
    	return returnValue;
    }

    //delete column entries for COLUMNS table
    for (map<int,RID>::iterator it=(*columnsEntries).begin(); it!= (*columnsEntries).end(); ++it)
    {
    	//get the rec id of the associated column entry in the COLUMNS table
    	rid = it->second;
    	returnValue = rbfm->deleteRecord(fileHandle, columnVec, rid);
    	if (returnValue != SUCCESS) {
    		rbfm->closeFile(fileHandle);
    		return -1;
    	}
    }

    //erase the entry for the table in the columnsMap
    delete(columnsEntries);
    columnsMap.erase(table_ID);

    //close the file associated with the COLUMNS table
    returnValue = rbfm->closeFile(fileHandle);
    if (returnValue != SUCCESS) {
    	return -1;
    }


    //***********************operations for delete tuple in tables.tbl and clear tablesMap**************
    //get the table RID
    rid = (*tableID).begin()->second;

    //delete the entry from the TABLES table file
    returnValue = rbfm->openFile("tables.tbl", fileHandle);

    if (returnValue != SUCCESS) {
    	return -1;
    }

    //TABLES file was opened successfully, delete the record
    returnValue = rbfm->deleteRecord(fileHandle, tableVec, rid);

    if (returnValue != SUCCESS) {
    	rbfm->closeFile(fileHandle);
    	return -1;
    }

    //delete entry from table map
    delete(tableID);
    tablesMap.erase(tableName);

    returnValue = rbfm->closeFile(fileHandle);

    if (returnValue != SUCCESS) {
    	return -1;
    }

    //**************delete the file associated with the filename********************
    string fileName = tableName + ".tbl";
    returnValue = rbfm->destroyFile(fileName);

    return returnValue;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    int returnValue = -1;
    
    attrs.clear();

    if (tablesMap.find(tableName) != tablesMap.end())  //check if table exists in the map
    {
        map<int, RID> * tableID = tablesMap[tableName];
        
        int table_ID = (*tableID).begin()->first;
        
        //get the map of all the columns for a particular table id
        map<int, RID> * columnsEntries = columnsMap[table_ID];
        
        //open the columns table to get the handle
        FileHandle fileHandle;
        returnValue = rbfm->openFile("columns.tbl", fileHandle);

        if (returnValue == SUCCESS) {
            
            RID rid;
            
            char * columnsRecord = (char*) malloc(MAX_COLUMNS_RECORD_SIZE);
            char * beginOfData = columnsRecord;

            for (map<int,RID>::iterator it=(*columnsEntries).begin(); it!= (*columnsEntries).end(); ++it){
                columnsRecord = beginOfData;
                //get the rec id of the associated column entry in the COLUMNS table
                rid = it->second;
                
                Attribute attr;
                
                if (returnValue == SUCCESS) {
                    rbfm->readRecord(fileHandle, columnVec, rid, columnsRecord);

                    //read the column name
                    columnsRecord = columnsRecord + sizeof(int); //skip over table id
                    
                    int stringLength;
                    memcpy(&stringLength, columnsRecord, sizeof(int));
                    columnsRecord = columnsRecord + sizeof(int) + stringLength; //skip over the table name
                    
                    memcpy(&stringLength, columnsRecord, sizeof(int));
                    columnsRecord = columnsRecord + sizeof(int); //skip over the col name length
                    
                    attr.name = string(columnsRecord, stringLength);
                    columnsRecord = columnsRecord + stringLength;
                    
                    memcpy(&stringLength, columnsRecord, sizeof(int)); //get the length of the column type string
                    columnsRecord = columnsRecord + sizeof(int); //skip over the col type length
                    
                    string colType = string(columnsRecord, stringLength);
                    
                    if (colType.compare("var_char") == 0){
                        attr.type = TypeVarChar;
                    }else if (colType.compare("int") == 0) {
                        attr.type = TypeInt;
                    }else{
                        attr.type = TypeReal;
                    }
                    
                    columnsRecord = columnsRecord + stringLength + sizeof(int);  //skip over the col pos
                    
                    unsigned length;
                    memcpy(&length, columnsRecord, sizeof(AttrLength)); //copy the col type
                    attr.length = length;
                    
                    attrs.push_back(attr);

                }
            }
            
            free(beginOfData);
        }
        rbfm->closeFile(fileHandle);
    }
    return returnValue;

}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    if(isSystemTableRequest(tableName)) {
        cout << "Invalid request to insert tuple into system table: " + tableName << endl;
        return -1;
    }
    
    FileHandle fileHandle;
    
    vector<Attribute> recordDescriptor;
    
    map<int, RID> * tableID = tablesMap[tableName];
    int table_ID = tableID->begin()->first;
    
    getAttributes(tableName, recordDescriptor);
    string fileName = tableName + ".tbl";
    
    int returnValue = rbfm->openFile(fileName, fileHandle);
    
    if (returnValue != SUCCESS) {
        return -1;
    }
    
    returnValue = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);

    if (returnValue != SUCCESS) {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    
    returnValue = rbfm->closeFile(fileHandle);
    if (returnValue != SUCCESS)
    	return returnValue;

    // **********operations for inserting index**********
    if (indexMap.find(table_ID) == indexMap.end())
    	return returnValue;

    for (map<int, RID>::iterator itr = indexMap[table_ID]->begin(); itr != indexMap[table_ID]->end(); ++itr) {
    	int position = itr->first;
    	Attribute keyAttribute = recordDescriptor[position - 1];

    	string indexFileName = tableName + "_" + keyAttribute.name + ".idx";
    	FileHandle indexFileHandle;
    	// open file
    	returnValue = ix->openFile(indexFileName, indexFileHandle);
    	if (returnValue != SUCCESS)
    		return returnValue;

    	// insert key
    	int startOffset = readFieldOffset(data, position, recordDescriptor);

    	returnValue = ix->insertEntry(indexFileHandle, keyAttribute, (char *)data + startOffset, rid);
    	if (returnValue != SUCCESS) {
    		ix->closeFile(indexFileHandle);
    		return returnValue;
    	}

    	// close file
    	returnValue = ix->closeFile(indexFileHandle);
    	if (returnValue != SUCCESS) {
    		return returnValue;
    	}
    }

    return returnValue;
}
    
RC RelationManager::deleteTuples(const string &tableName)
{
    if(isSystemTableRequest(tableName)) {
        cout << "Invalid request to delete tuples in system table: " + tableName << endl;
        return -1;
    }
    
    string fileName = tableName + ".tbl";
    FileHandle fileHandle;
    int returnValue = rbfm->openFile(fileName, fileHandle);
    
    if (returnValue != SUCCESS) {
        return -1;
    }
    
    returnValue = rbfm->deleteRecords(fileHandle);

    if (returnValue != SUCCESS) {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    
    returnValue = rbfm->closeFile(fileHandle);
    if (returnValue != SUCCESS) {
    	return returnValue;
    }

    //*********operations for associated index files***********
    // delete associated index files and then create them again
    int table_ID = tablesMap[tableName]->begin()->first;

    if (indexMap.find(table_ID) == indexMap.end())
    	return returnValue;

    vector<Attribute> recordDescriptor;
    returnValue = getAttributes(tableName, recordDescriptor);
    if (returnValue != SUCCESS)
    	return returnValue;

    map<int, RID> *indexEntry = indexMap[table_ID];
    for (map<int, RID>::iterator itr = indexEntry->begin(); itr != indexEntry->end(); itr++) {
    	int position = itr->first;
    	string columnName = recordDescriptor[position - 1].name;
    	string fileName = tableName + "_" + columnName + ".idx";

    	returnValue = ix->destroyFile(fileName);
    	if (returnValue != SUCCESS)
    		return returnValue;

    	returnValue = ix->createFile(fileName);
    	if (returnValue != SUCCESS)
    		return returnValue;
    }

    return returnValue;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    if(isSystemTableRequest(tableName)) {
        cout << "Invalid request to delete tuple in system table: " + tableName << endl;
        return -1;
    }
    
    map<int, RID> * tableID = tablesMap[tableName];
    int table_ID = tableID->begin()->first;


    FileHandle fileHandle;
    
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    
    string fileName = tableName + ".tbl";
    int returnValue = rbfm->openFile(fileName, fileHandle);
    
    if(returnValue != SUCCESS) {
        return -1;
    }
    
    // read record into data, while be used later in deleting indices
    void *data = malloc(PAGE_SIZE);

    returnValue = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);

    if(returnValue != SUCCESS) {
    	free(data);
    	rbfm->closeFile(fileHandle);
    	return -1;
    }

    returnValue = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
    
    if(returnValue != SUCCESS) {
    	free(data);
        rbfm->closeFile(fileHandle);
        return -1;
    }
    
    returnValue = rbfm->closeFile(fileHandle);

    if(returnValue != SUCCESS) {
    	free(data);
    	return -1;
    }

    //**************delete associated index***************
    if (indexMap.find(table_ID) == indexMap.end()) {
    	free(data);
    	return returnValue;
    }

    for (map<int, RID>::iterator itr = indexMap[table_ID]->begin(); itr != indexMap[table_ID]->end(); ++itr) {
    	int position = itr->first;
    	Attribute keyAttribute = recordDescriptor[position - 1];

    	string indexFileName = tableName + "_" + keyAttribute.name + ".idx";
    	FileHandle indexFileHandle;
    	// open file
    	returnValue = ix->openFile(indexFileName, indexFileHandle);
    	if (returnValue != SUCCESS) {
    		free(data);
    		return returnValue;
    	}

    	// prepare key
    	int startOffset = readFieldOffset(data, position, recordDescriptor);

    	returnValue = ix->deleteEntry(indexFileHandle, keyAttribute, (char *)data + startOffset, rid);
    	if (returnValue != SUCCESS) {
    		free(data);
    		ix->closeFile(indexFileHandle);
    		return returnValue;
    	}

    	returnValue = ix->closeFile(indexFileHandle);
    	if (returnValue != SUCCESS) {
    		free(data);
    		return returnValue;
    	}
    }

    free(data);

    return returnValue;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    if(isSystemTableRequest(tableName)) {
        cout << "Invalid request to update tuple in system table: " + tableName << endl;
        return -1;
    }
    
    map<int, RID> * tableID = tablesMap[tableName];
    int table_ID = tableID->begin()->first;

    FileHandle fileHandle;
    
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    
    string fileName = tableName + ".tbl";
    int returnValue = rbfm->openFile(fileName, fileHandle);
    
    if (returnValue != SUCCESS) {
        return -1;
    }
    
    void *oldData = malloc(PAGE_SIZE);
    returnValue = rbfm->readRecord(fileHandle, recordDescriptor, rid, oldData);
    if(returnValue != SUCCESS) {
    	free(oldData);
    	rbfm->closeFile(fileHandle);
    	return -1;
    }

    returnValue = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
    
    if (returnValue != SUCCESS) {
    	free(oldData);
        rbfm->closeFile(fileHandle);
        return -1;
    }
    
    returnValue = rbfm->closeFile(fileHandle);

    if (returnValue != SUCCESS) {
    	free(oldData);
    	return -1;
    }

    //**************operations for updata indices*********************
    if (indexMap.find(table_ID) == indexMap.end()) {
    	free(oldData);
    	return returnValue;
    }

    //compare the new key with old key, if different, delete old one, then insert new one
    for (map<int, RID>::iterator itr = indexMap[table_ID]->begin(); itr != indexMap[table_ID]->end(); ++itr) {
    	int position = itr->first;
    	Attribute keyAttribute = recordDescriptor[position - 1];

    	// prepare key
    	int oldKeyStartOffset = readFieldOffset(oldData, position, recordDescriptor);
    	int newKeyStartOffset = readFieldOffset(oldData, position, recordDescriptor);

    	if (!isFieldEqual((char *)oldData + oldKeyStartOffset, (char *)data + newKeyStartOffset, keyAttribute.type)) {
    		string indexFileName = tableName + "_" + keyAttribute.name + ".idx";
    		FileHandle indexFileHandle;

    		returnValue = ix->openFile(indexFileName, indexFileHandle);
    		if (returnValue != SUCCESS) {
    			free(oldData);
    			return returnValue;
    		}

    		returnValue = ix->deleteEntry(indexFileHandle, keyAttribute, (char *)oldData + oldKeyStartOffset, rid);
    		if (returnValue != SUCCESS) {
    			free(oldData);
    			ix->closeFile(indexFileHandle);
    			return returnValue;
    		}

    		returnValue = ix->insertEntry(indexFileHandle, keyAttribute, (char *)data + newKeyStartOffset, rid);
    		if (returnValue != SUCCESS) {
    			free(oldData);
    			ix->closeFile(indexFileHandle);
    			return returnValue;
    		}

    		returnValue = ix->closeFile(indexFileHandle);
    		if (returnValue != SUCCESS) {
    			free(oldData);
    			return returnValue;
    		}
    	}

    }

    free(oldData);

    return returnValue;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    
    if (tablesMap.find(tableName) == tablesMap.end())  //check if table exists in the map
    {
        return -1;
    }
    
    FileHandle fileHandle;
    string fileName = tableName + ".tbl";
    vector<Attribute> recordDescriptor;
    
    int returnValue = getAttributes(tableName, recordDescriptor);
    
    if (returnValue != SUCCESS) {
        return -1;
    }
    
    returnValue = rbfm->openFile(fileName, fileHandle);
    
    if (returnValue != SUCCESS) {
        return -1;
    }
    
    returnValue = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
    
    if (returnValue != SUCCESS) {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    
    return rbfm->closeFile(fileHandle);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    FileHandle fileHandle;
    string fileName = tableName + ".tbl";
    vector<Attribute> recordDescriptor;
    
    int returnValue = getAttributes(tableName, recordDescriptor);
    
    if (returnValue != SUCCESS) {
        return -1;
    }
    
    returnValue = rbfm->openFile(fileName, fileHandle);
    
    if (returnValue != SUCCESS) {
        return -1;
    }
    
    returnValue = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    
    if (returnValue != SUCCESS) {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    
    return rbfm->closeFile(fileHandle);
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
    FileHandle fileHandle;
    string fileName = tableName + ".tbl";
    rbfm->openFile(fileName, fileHandle);
    
    //getTable Record Descriptor
    vector<Attribute> recordDescriptor;
   
    int returnValue = getAttributes(tableName, recordDescriptor);
    
    if (returnValue == SUCCESS) {
        returnValue = rbfm->reorganizePage(fileHandle, recordDescriptor, pageNumber);
    }
    
    if (returnValue == SUCCESS) {
        returnValue = rbfm->closeFile(fileHandle);
    }
    
    return returnValue;
}



RC RelationManager::createIndex(const string & tableName, const string & attributeName) {
	int returnValue = -1;

	string indexFileName = tableName + "_" + attributeName + ".idx";

	// STEP1: check whether "tableName" and "attributeName" are valid
	// if is valid, get tableId for tableName and attribute position for attributeName
	if (tablesMap.find(tableName) == tablesMap.end())
		return returnValue;

	// get tableId
	map<int, RID> * tableIDMap = tablesMap[tableName];
	int table_ID = (*tableIDMap).begin()->first;

	// get attribute position
	vector<Attribute> recordDescriptor;
	returnValue = getAttributes(tableName, recordDescriptor);
	if (returnValue != SUCCESS)
		return returnValue;

	int attrPos = 1;
	for (; attrPos <= (int)recordDescriptor.size(); attrPos++) {
		if (recordDescriptor[attrPos - 1].name.compare(attributeName) == 0)
			break;
	}

	// "attributeName" in "tableName" is not found, return error
	if (attrPos > (int)recordDescriptor.size())
		return -1;

	// STEP2: check whether index file has already been created
	// if not, create entry[attribute position, RID] and put this entry in [tableId]
	map<int, RID> *indexEntryMap;
	if (indexMap.find(table_ID) == indexMap.end()) {
		indexEntryMap = new map<int, RID>();
		indexMap[table_ID] = indexEntryMap;
	}
	else {
		indexEntryMap = indexMap[table_ID];

		if (indexEntryMap->find(attrPos) != indexEntryMap->end()) {
			cout << "This index has already been created!";
			return -1;
		}
	}

	// STEP3: create .idx file
	returnValue = ix->createFile(indexFileName);
	if (returnValue != SUCCESS)
		return returnValue;


	// STEP4: insert this index in indices.tbl and update indexMap
	FileHandle fileHandle;
	returnValue = rbfm->openFile("indices.tbl", fileHandle);
	if (returnValue != SUCCESS)
		return returnValue;

	RID indexRid;
	returnValue = insertIndexEntry(tableName, attributeName, table_ID, attrPos, fileHandle, indexRid);
	if (returnValue != SUCCESS) {
		rbfm->closeFile(fileHandle);
		return returnValue;
	}

	returnValue = rbfm->closeFile(fileHandle);
	if (returnValue != SUCCESS)
		return returnValue;

	// update the index map
	(*indexEntryMap)[attrPos] = indexRid;

	// STEP5: scan the file and insert [attribute, RID] in the new created .idx file
	FileHandle indexFileHandle;
	returnValue = ix->openFile(indexFileName, indexFileHandle);
	if (returnValue != SUCCESS)
		return returnValue;


	RM_ScanIterator rmsi;
	// get key attribute
	Attribute keyAttribute = recordDescriptor[attrPos - 1];

	// projected attribute
	vector<string> attributeNames;
	attributeNames.push_back(keyAttribute.name);

	returnValue = scan(tableName, keyAttribute.name, NO_OP, NULL, attributeNames, rmsi);
	if (returnValue != SUCCESS) {
		ix->closeFile(indexFileHandle);
		return returnValue;
	}

	char *data = (char *) malloc(MAX_ATTRIBUTE_LENGTH);
	RID rid;

	while (rmsi.getNextTuple(rid, data) != RM_EOF)
		ix->insertEntry(indexFileHandle, keyAttribute, data, rid);


	rmsi.close();
	free(data);

	returnValue = ix->closeFile(indexFileHandle);

	return returnValue;
}



RC RelationManager::destroyIndex(const string & tableName, const string &attributeName) {
	int returnValue = SUCCESS;
	string indexFileName = tableName + "_" + attributeName + ".idx";

	// STEP1: check if this .idx file exists;
	// get table id
	map<int, RID> * tableIDMap = tablesMap[tableName];
	int table_ID = (*tableIDMap).begin()->first;

	// get attribute position
	vector<Attribute> attributes;
	returnValue = getAttributes(tableName, attributes);
	if (returnValue != SUCCESS) return returnValue;

	int attrPos = 1;
	for (; attrPos <= (int)attributes.size(); attrPos++) {
		if (attributes[attrPos - 1].name.compare(attributeName) == 0)
			break;
	}

	// "attributeName" in "tableName" is not found, return error
	if (attrPos > (int)attributes.size())
		return -1;

	// STEP2: remove [attribute position, indexRid] from indexMap
	if (indexMap.find(table_ID) == indexMap.end())
		return -1;

	map<int, RID> *indexEntryMap = indexMap[table_ID];

	if (indexEntryMap->find(attrPos) == indexEntryMap->end())
		return -1;
	RID indexRid = (*indexEntryMap)[attrPos];

	indexEntryMap->erase(attrPos);
	if (indexEntryMap->size() == 0) {
		delete(indexMap[table_ID]);
		indexMap.erase(table_ID);
	}

	// STEP3: delete tuple from indices.tbl
	returnValue = deleteTuple("indices", indexRid);
	if (returnValue != SUCCESS)
		return returnValue;

	// STEP4: delete .idx file
	returnValue = ix->destroyFile(indexFileName);

	return returnValue;
}



RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute,
		const CompOp compOp,
		const void *value,
		const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator) {
    string fileName = tableName + ".tbl";

    int returnValue = rbfm->openFile(fileName, rm_ScanIterator.fileHandle);
    if (returnValue != SUCCESS) {
        return -1;
    }
    
    if (tableName.compare("tables") == 0)
    	return rm_ScanIterator.initialize(tableVec, compOp, value, attributeNames, conditionAttribute);
    else if (tableName.compare("columns") == 0)
    	return rm_ScanIterator.initialize(columnVec, compOp, value, attributeNames, conditionAttribute);
    else {
    	vector<Attribute> recordDescriptor;
    	returnValue = getAttributes(tableName, recordDescriptor);
    	if (returnValue != SUCCESS) {
    		return -1;
    	}
    	return rm_ScanIterator.initialize(recordDescriptor, compOp, value, attributeNames, conditionAttribute);
    }
}



RC RelationManager::indexScan(const string &tableName,
			const string &attributeName,
			const void *lowKey,
			const void *highKey,
			bool lowKeyInclusive,
			bool highKeyInclusive,
			RM_IndexScanIterator &rm_IndexScanIterator)
{
	int returnValue = -1;
	string indexFileName = tableName + "_" + attributeName + ".idx";

	// STEP1 : check if this .idx file exists;
	if (!pfm->fexist(indexFileName))
		return returnValue;

	// STEP2 : open file with fileHandle
	returnValue = ix->openFile(indexFileName, rm_IndexScanIterator.indexFileHandle);
	if (returnValue != SUCCESS) return returnValue;

	// STEP3 : get Attribute;
	Attribute keyAttribute;
	vector<Attribute> attributes;
	returnValue = getAttributes(tableName, attributes);
	if (returnValue != SUCCESS) return returnValue;

	unsigned i = 0;
	for (; i < attributes.size(); i++) {
		if (attributes[i].name.compare(attributeName) == 0) {
			keyAttribute = attributes[i];
			break;
		}
	}

	// attribute not found
	if (i == attributes.size()) {
		ix->closeFile(rm_IndexScanIterator.indexFileHandle);
		return -1;
	}

	returnValue = rm_IndexScanIterator.initialize(keyAttribute, lowKey, highKey,
			lowKeyInclusive, highKeyInclusive);

	return returnValue;
}



// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
    return -1;
}

void RelationManager::appendData(int fieldLength, int &offset, char * pageBuffer, const char * dataToWrite, AttrType attrType)
{
    
    if (attrType == TypeVarChar) {
        memcpy(pageBuffer + offset, &fieldLength , sizeof(int));
        offset += sizeof(int);
    }
    
    memcpy(pageBuffer + offset, dataToWrite , fieldLength);
    offset += fieldLength;
}

/**************************************************************************************************************
 * Utility method that will return a short value that will represent the total number of bytes needed to
 * represent the record given the record format.
**************************************************************************************************************/
short RelationManager::determineMemoryNeeded(const vector<Attribute> &attributes)
{
    short size = 0;
    
    for (int i = 0; i < (int)attributes.size(); i++) {
        if(attributes[i].type == TypeVarChar) {
            size += sizeof(int);
        }
        
        size += attributes[i].length;
    }
    
    return size;
}

bool RelationManager::isSystemTableRequest(string tableName) {
    return false;

    bool isSysTbl = false;
    
    map<int, RID> * tableID = tablesMap[tableName];
    
    //get the rec ID of the table to be deleted
    RID rid = (*tableID).begin()->second;

    //enough memory for the description of the table and its size (int)
    char * tableType = (char*) malloc(sizeof(int) + tableVec[2].length);
    char * data = tableType;
    
    string attrName = "TableType";
    readAttribute(tableName, rid, attrName, tableType);
    
    int tblTypeLength;
    memcpy(&tblTypeLength, tableType, sizeof(int));
    
    tableType = tableType + sizeof(int);
    
    string tbltype(tableType, tblTypeLength);
    
    string system = "system";
    if (tbltype.compare(system) == 0) {
        isSysTbl = true;
    }
        
    free(data);

    return isSysTbl;
}

int RelationManager::readFieldOffset(const void *data, int attrPosition, vector<Attribute> recordDescriptor) {
	int offset = 0;

	for (int i = 0; i < attrPosition - 1; i++) {
		Attribute attr = recordDescriptor[i];

		if (attr.type == TypeInt)
			offset += sizeof(int);
		else if (attr.type == TypeReal)
			offset += sizeof(float);
		else {
			int fieldLength = *(int *)((char *)data + offset);
			offset += sizeof(int) + fieldLength;
		}
	}

	return offset;
}

bool RelationManager::isFieldEqual(const char *a, const char *b, AttrType type){
	if (type == TypeInt) {
		int A = *(int *)a;
		int B = *(int *)b;

		return A == B;
	}
	else if (type == TypeReal) {
		float A = *(float *)a;
		float B = *(float *)b;

		if (A - B < 0.00001 && A - B > -0.00001)
			return true;
		else
			return false;
	}
	else {
		int lengthA = *(int *)a;
		int lengthB = *(int *)b;
		string A(a + sizeof(int), lengthA);
		string B(b + sizeof(int), lengthB);

		return A.compare(B) == 0;
	}
}

/**********RECORD SCAN ITERATOR****************/

RM_ScanIterator::RM_ScanIterator() {
	rbfm = RecordBasedFileManager::instance();
}

RM_ScanIterator::~RM_ScanIterator(){}

RC RM_ScanIterator::initialize(const vector<Attribute> &recordDescriptor,
                               const CompOp compOp,
                               const void *value,
                               const vector<string> &attributeNames,
                               const string &conditionAttribute) {
    return rbfm_scanner.initialize(fileHandle, recordDescriptor, compOp, value, attributeNames, conditionAttribute);
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    return rbfm_scanner.getNextRecord(rid, data);
}

RC RM_ScanIterator::close() {
	rbfm_scanner.close();
	return rbfm->closeFile(fileHandle);
}

/**********INDEX SCAN ITERATOR****************/
RM_IndexScanIterator::RM_IndexScanIterator() {
	ix = IndexManager::instance();
}

RM_IndexScanIterator::~RM_IndexScanIterator(){}

RC RM_IndexScanIterator::initialize(const Attribute keyAttribute,
		const void *lowKey,
		const void *highKey,
		bool lowKeyInclusive,
		bool highKeyInclusive)
{
	return ix->scan(indexFileHandle, keyAttribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_scanner);
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
	return ix_scanner.getNextEntry(rid, key);
}

RC RM_IndexScanIterator::close() {
	int returnValue = ix_scanner.close();

	if (returnValue != SUCCESS) {
		ix->closeFile(indexFileHandle);
		return returnValue;
	}

	return ix->closeFile(indexFileHandle);
}

