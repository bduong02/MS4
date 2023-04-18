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
    return false;
  }

  bool heapFileAndTableTests() {
    return false;
  };
}

