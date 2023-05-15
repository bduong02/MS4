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
    bool tests(){        
        // CreateStatement* stmt = SQLParser::parseSQLString("CREATE TABLE z (x int, y text)")
        // SQLExec::execute(stmt);
        cout << "in test"<<endl;
        // const ShowStatement* stmt = new ShowStatement(ShowStatement::EntityType::kTables); // SHOW TABLES

        // cout << "declared Showstmt"<<endl;
        // SQLExec::execute(stmt);

        const ShowStatement* stmt = new ShowStatement(ShowStatement::EntityType::kIndex); // SHOW TABLES
        SQLExec::execute(stmt);

        cout << "Executed showStmt" << endl;
        return true;
    }
}