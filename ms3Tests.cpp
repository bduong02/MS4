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
        cout << "in test"<<endl;

        // create table foo
        CreateStatement* stmt = new CreateStatement(CreateStatement::CreateType::kTable);
        stmt->tableName = "foo";
        stmt->columns = new vector<ColumnDefinition*>();
        columns->push_back(new ColumnDefinition("col1", ColumnDefinition::DataType::TEXT));
        columns->push_back(new ColumnDefinition("col2", ColumnDefinition::DataType::INT));
        columns->push_back(new ColumnDefinition("col3", ColumnDefinition::DataType::INT));

        cout << "Executing create table" << endl;
        SQLExec::execute(stmt);
        delete stmt;

        cout << "Executing show tables" << endl;
        const ShowStatement* stmt = new ShowStatement(ShowStatement::EntityType::kTables); // SHOW TABLES
        stmt->tableName;

        cout << "declared Showstmt"<<endl;
        SQLExec::execute(stmt);
        delete stmt;

        // const ShowStatement* stmt = new ShowStatement(ShowStatement::EntityType::kIndex); // SHOW TABLES
        // SQLExec::execute(stmt);

        cout << "Executed showStmt" << endl;
        return true;
    }
}