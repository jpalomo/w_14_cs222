
#include <fstream>
#include <iostream>
#include <cassert>

#include "rm.h"

using namespace std;

void rmTest()
{
    RelationManager *rm = RelationManager::instance();
    //rm->deleteTable("tables");
    RID rid;
    //rm->insertTuple("tables", NULL, rid);
    
    rid.pageNum = 0;
    rid.slotNum = 1;
    
    char * data = (char*) malloc(1000);
    char * begin = data;
    rm->readAttribute("tables", rid, "TableType", data);
    
    int length;
    memcpy(&length, data, sizeof(int));
    
    data = data + 4;
    
    string type = string(data, length);
    cout << type << endl;
    
    free (begin);
    
    //rm->updateTuple("tables", NULL, rid);
    //rm->deleteTuple("tables", rid);
    //rm->deleteTuples("tables");
  // write your own testing cases here
}

int main() 
{
  cout << "test..." << endl;

  rmTest();
  // other tests go here

  cout << "OK" << endl;
}
