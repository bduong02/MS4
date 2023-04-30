#include <iostream>
#include <cstdlib> 
#include <string>
#include "db_cxx.h"
#include "SQLParser.h"
#include "SQLParserResult.h"
#include "heap_storage.h"
#include "schema_tables.h"
#include "SQLExec.h"
using namespace std;
using namespace hsql;

// extern DbEnv *_DB_ENV;

namespace MS3Tests {
    void createTest(){
        // CreateStatement* stmt = SQLParser::parseSQLString("CREATE TABLE z (x int, y string)")
        // SQLExec::create(stmt);
    }
}