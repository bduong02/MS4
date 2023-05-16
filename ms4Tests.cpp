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
    // Precondition: pass the empty string to tableName if showType is kTable. Otherwise, pass the table name
    void testShow(ShowStatement::EntityType showType, string tableName){
        ShowStatement* showStmt;
        if(showType == ShowStatement::kColumns){
            if(tableName == "") cout << "Error: didn't pass a table name for show columns" << endl;
            cout << "Calling show columns on " << tableName << endl;
            showStmt = new ShowStatement(ShowStatement::EntityType::kColumns);
            showStmt->tableName = (char*)tableName.c_str();
            SQLExec::execute(showStmt);
            delete showStmt->tableName;
        }else if(showType == ShowStatement::EntityType::kTables){
            cout << "Executing show tables" << endl;
            showStmt = new ShowStatement(ShowStatement::EntityType::kTables);
            SQLExec::execute(showStmt);
        }else if(showType == ShowStatement::EntityType::kIndex){
            if(tableName == "") cout << "Error: didn't pass a table name for show index" << endl;        
            cout << "Calling show index on " << tableName << endl;
            showStmt = new ShowStatement(ShowStatement::EntityType::kIndex);
            showStmt->tableName = (char*)tableName.c_str();
            SQLExec::execute(showStmt);
            delete showStmt->tableName;
        }

        delete showStmt;
        showStmt = nullptr;
    }

    void tests(){        
        cout << "in test"<<endl;

        // create table foo
        cout << "Creating table foo" << endl;
        CreateStatement* createStmt = new CreateStatement(CreateStatement::CreateType::kTable);
        createStmt->tableName = (char*)"foo";
        createStmt->columns = new vector<ColumnDefinition*>();
        createStmt->columns->push_back(new ColumnDefinition((char*)"col1", ColumnDefinition::DataType::TEXT));
        createStmt->columns->push_back(new ColumnDefinition((char*)"col2", ColumnDefinition::DataType::INT));
        createStmt->columns->push_back(new ColumnDefinition((char*)"col3", ColumnDefinition::DataType::INT));
        SQLExec::execute(createStmt);

        for(int i=0; i < createStmt->columns->size(); i++){
            delete createStmt->columns[i];
            createStmt->columns[i] = nullptr;
        }

        delete createStmt->columns;
        createStmt->columns = nullptr;
        delete createStmt->tableName;
        delete createStmt;
        createStmt = nullptr;
        
        testShow(ShowStatement::kTables, "");
        testshow(ShowStatement::kColumns, "foo")

        cout << "Creating an index on foo on columns (col1, col2)" << endl;
        CreateStatement* createStmt2 = new CreateStatement(CreateStatement::CreateType::kTable);
        createStmt2->tableName = (char*)"foo";
        createStmt2->indexColumns = new vector<char*>();
        createStmt2->indexColumns->push_back((char*)"col1");
        createStmt2->indexColumns->push_back((char*)"col2");
        createStmt2->indexName = (char*)"index1";
        createStmt2->indexType = (char*)"BTREE";
        SQLExec::execute(createStmt2);
        delete createStmt2->indexColumns;
        delete createStmt2;

        cout << "Creating an index on foo on column col3" << endl;
        CreateStatement* createStmt3 = new CreateStatement(CreateStatement::CreateType::kTable);
        createStmt3->tableName = (char*)"foo";
        createStmt3->indexColumns = new vector<char*>();
        createStmt3->indexColumns->push_back((char*)"col3");
        createStmt3->indexName = (char*)"index2";
        createStmt3->indexType = (char*)"BTREE";
        SQLExec::execute(createStmt3);
        delete createStmt3->indexColumns;
        delete createStmt3;

        cout << "Creating an index on foo, on a column that doesn't exist" << endl;
        CreateStatement* createStmt4 = new CreateStatement(CreateStatement::CreateType::kTable);
        createStmt4->tableName = (char*)"foo";
        createStmt4->indexColumns = new vector<char*>();
        createStmt4->indexColumns->push_back((char*)"col4");
        createStmt4->indexName = (char*)"index2";
        createStmt4->indexType = (char*)"BTREE";
        SQLExec::execute(createStmt4);
        delete createStmt4->indexColumns;
        delete createStmt4;

        cout << "Calling show index on foo" << endl;
        testShow(ShowStatement::kIndex, "foo");

        cout << "Dropping index1 from foo" << endl;
        DropStatement* dropStmt = new DropStatement(DropStatement::EntityType::kIndex);
        dropStmt->indexName = (char*)"index1";
        dropStmt->name = (char*)"foo";
        SQLExec::execute(dropStmt);
        delete dropStmt;

        cout << "Creating another table, bar" << endl;
        CreateStatement* createStmt = new CreateStatement(CreateStatement::CreateType::kTable);
        createStmt->tableName = (char*)"bar";
        createStmt->columns = new vector<ColumnDefinition*>();
        createStmt->columns->push_back(new ColumnDefinition((char*)"a", ColumnDefinition::DataType::INT));
        createStmt->columns->push_back(new ColumnDefinition((char*)"b", ColumnDefinition::DataType::TEXT));
        createStmt->columns->push_back(new ColumnDefinition((char*)"c", ColumnDefinition::DataType::TEXT));
        SQLExec::execute(createStmt);

        for(int i=0; i < createStmt->columns->size(); i++){
            delete createStmt->columns[i];
            createStmt->columns[i] = nullptr;
        }

        delete createStmt->columns;
        createStmt->columns = nullptr;
        delete createStmt;
        createStmt = nullptr;

        testShow(ShowStatement::kColumns, "bar");
        testShow(ShowStatement::kTables, "");

        cout << "Dropping table foo" << endl;
        DropStatement* dropStmt = new DropStatement(DropStatement::EntityType::kTable);
        dropStmt->name = (char*)"foo";
        SQLExec::execute(dropStmt);
        delete dropStmt->name;
        delete dropStmt;

        cout << "Trying to drop a table that doesn't exist" << endl;
        dropStmt = new DropStatement(DropStatement::EntityType::kTable);
        dropStmt->name = (char*)"bar";
        cout << "Executing drop table bar" << endl;
        SQLExec::execute(dropStmt);
        delete dropStmt->name;
        delete dropStmt;
        dropStmt = nullptr;

        cout << "Dropping table bar" << endl;
        DropStatement* dropStmt = new DropStatement(DropStatement::EntityType::kTable);
        dropStmt->name = (char*)"bar";
        SQLExec::execute(dropStmt);
        delete dropStmt->name;
        delete dropStmt;

        testShow(ShowStatement::EntityType::kTables)
        return true;
    }
}