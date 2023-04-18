//Filename: driver.cpp
//Authors: Ishan Parikh, Bryan Duong
//Date 4-1-23
//CPSC 5300
//Purpose: set of simple tests to run for milestone 2

#include <iostream>
#include <cstdlib> 
#include <string>
#include "db_cxx.h"
#include "SQLParser.h"
#include "SQLParserResult.h"
#include "heap_storage.h"
using namespace std;
using namespace hsql;

extern DbEnv *_DB_ENV;

namespace MS2Tests {
  bool slottedPageTests() {
    // constructing a slotted page
    char blankSpace[DbBlock::BLOCK_SZ];
    Dbt blockDbt[blankSpace, sizeof(blankSpace));
    SlottedPage page(blockDbt, 1, true);
                 
    char rec1[] = "apples";
    Dbt rec1Dbt(rec1, sizeof(rec1));
    RecordID id = page.add(&rec1Dbt);
    if(id != 1)
    {
        return false;
    }
     
    // adding in a record to fetch back
    char rec2[] = "bananas";
    Dbt rec2Dbt(rec2, sizeof(rec2));
    id = page.add(&rec2Dbt);
    if(id != 2)
    {
        return false;
    }
     
    // fetching the record back
    Dbt *getDbt = page.get(id);
    getDbt = slot.get(id);
    std::string expected = std::string(rec2, sizeof(rec2));
    std::string result((char *) get_dbt->get_data(), get_dbt->get_size());
    delete getDbt;
    if(expected != result) {
        return true;
    }
                 
    // testing slide and ids with larger record
    char r2[] = "testing slide function";
    rec1Dbt = Dbt(r2, sizeof(r2));
    page.put(1, rec1Dbt);
    getDbt = page.get(2);
    expected = std::string(rec2, sizeof(rec2));
    result = std::string((char*) getDbt->get_data(), getDbt->get_size());
    delete getDbt;
    if (expected != result) {
        return true;
    }
    getDbt = page.get(1);
    expected = std::string(r2, sizeof(r2));
    actual = std::string((char *) getDbt->get_data(), getDbt->get_size());
    delete getDbt;
    if (expected != result) {
        return true;
    }
    
    // testing delete      
    RecordIDs *idList = page.ids();
    if(idList->size() != 2 || ifList->at(0) != 1 || idList->at(1) != 2)
    {
        return true;
    }
    delete idList;
    page.del(1);
    idList = page.ids();
    if (idList->size() != 1 || idList->at(0) != 2) {
        return true;
    }
    delete idList;
    getDbt = page.get(1);
    if(getDbt != nullptr)
    {
        return false;
    }
    
    // testing add with a big record
    rec2Dbt = Dbt(nullptr, DbBlock::BLOCK_SZ - 5);
    try {
        page.add(&rec2Dbt);
        return false;
    } catch(DbBlockNoRoomError &e){
        return true;
    }
                 
    return true;             
  }

  bool heapFileAndTableTests() {
    return false;
  }
};
