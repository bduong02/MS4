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

namespace MS4Tests{
    bool tests(){        
        cout << "in test"<<endl;

        // create table foo
        CreateStatement* createStmt = new CreateStatement(CreateStatement::CreateType::kTable);
        createStmt->tableName = (char*)"foo";
        createStmt->columns = new vector<ColumnDefinition*>();
        createStmt->columns->push_back(new ColumnDefinition((char*)"col1", ColumnDefinition::DataType::TEXT));
        createStmt->columns->push_back(new ColumnDefinition((char*)"col2", ColumnDefinition::DataType::INT));
        createStmt->columns->push_back(new ColumnDefinition((char*)"col3", ColumnDefinition::DataType::INT));

        cout << "Executing create table" << endl;
        SQLExec::execute(createStmt);
        delete createStmt->columns;
        createStmt->columns = nullptr;
        delete createStmt;
        createStmt = nullptr;

        cout << "Executing show tables" << endl;
        const ShowStatement* showStmt = new ShowStatement(ShowStatement::EntityType::kTables); // SHOW TABLES
        SQLExec::execute(showStmt);
        delete showStmt;
        showStmt = nullptr;

        cout << "Creating an index on foo" << endl;
        CreateStatement* createStmt = new CreateStatement(CreateStatement::CreateType::kTable);
        createStmt->tableName = (char*)"foo";
        createStmt->indexColumns = new vector<ColumnDefinition*>();
        createStmt->indexColumns->push_back(new ColumnDefinition((char*)"col1", ColumnDefinition::DataType::TEXT));
        createStmt->indexColumns->push_back(new ColumnDefinition((char*)"col2", ColumnDefinition::DataType::INT));
        createStmt->indexColumns->push_back(new ColumnDefinition((char*)"col3", ColumnDefinition::DataType::INT));

        // drop table foo
        DropStatement* dropStmt = new DropStatement(DropStatement::EntityType::kTable);
        dropStmt->name = (char*)"foo";
        cout << "Executing drop table foo" << endl;
        SQLExec::execute(dropStmt);
        delete dropStmt;

        cout << "Trying to drop a table that doesn't exist" << endl;
        dropStmt = new DropStatement(DropStatement::EntityType::kTable);
        dropStmt->name = (char*)"bar";
        cout << "Executing drop table bar" << endl;
        SQLExec::execute(dropStmt);
        delete dropStmt;
        dropStmt = nullptr;

        cout << "Executing show tables" << endl;
        const ShowStatement* showStmt = new ShowStatement(ShowStatement::EntityType::kTables); // SHOW TABLES
        SQLExec::execute(showStmt);
        delete showStmt;
        showStmt = nullptr;
        return true;
    }
}