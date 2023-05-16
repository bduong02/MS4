//Filename: Shell.cpp
//Authors: Ryan Silvera, David Stanko
//Date 5-15-23
//CPSC 4300
//Purpose: Milestone 4 Implementation
#include <iostream>
#include <cstdlib> 
#include <string>
#include <dirent.h>
#include "db_cxx.h"
#include "SQLParser.h"
#include "SQLParserResult.h"
#include "SQLExec.h"
#include "schema_tables.h"
#include "heap_storage.h"
#include "ms2Tests.cpp"
#include "ms4Tests.cpp"

using namespace std;
using namespace hsql;
const unsigned int BLOCK_SZ = 4096; // lines 20-21 added by David from Haley & David's milestone 1
const char *MILESTONE1 = "milestone1.db";

DbEnv *_DB_ENV;

//Preconditions: the SqlParseResult parse tree MUST be processed 
//               beforehand by the SQLParser::parseSQLString function
//Process:       just matches the result to a string and prints it
//Postconditions: none
void execute(SQLParserResult* &result);
string handleSelect(const SelectStatement* statement);
string handleCreate(const CreateStatement* statement);
string expressToString(const Expr* expression);

vector<Identifier> tables; // all the tables that have been created

int main(int argc, char* argv[]) {
    if(argc <= 1) {
        cout << "Invalid argument count" << endl;
        return 1;
    }

    string envDirPath = argv[1];
    
    //init db environment locally and globally
    DbEnv environment(0U);
    try {
        environment.set_message_stream(&cout);
	    environment.set_error_stream(&cerr);
	    environment.open(envDirPath.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
    } catch(DbException &E) {
        cout << "Error creating DB environment" << endl;
        exit(EXIT_FAILURE);
    }

    Db db(&environment, 0);
	db.set_message_stream(environment.get_message_stream());
	db.set_error_stream(environment.get_error_stream());
	db.set_re_len(BLOCK_SZ);
	int id = db.open(NULL, MILESTONE1, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644);
    if(id == 0) cout << "In main, opened successfully" << endl;

    _DB_ENV = &environment;

    //MILESTONE 2 TESTS
    if(MS2Tests::slottedPageTests() == false) {
        cout << "Slotted Page Tests failed!" << endl;
    } else {
        cout << "Slotted Page Tests passed!" << endl;
    }

    if(MS2Tests::heapFileAndTableTests() == false) {
        cout << "Heap Tests failed! Moving on to shell...." << endl << endl;
    } else {
        cout << "Heap Tests passed! Moving on to shell...." << endl << endl;
    }

    MS4Tests::tests();

    //read, parse, & handle sql statements 
    const string EXIT_RESPONSE = "quit";
    string statementInput = "";
    while(statementInput != EXIT_RESPONSE) {
        cout << "SQL> ";
        getline(cin, statementInput);

        SQLParserResult* result = nullptr;

        //handle input with appropriate action or quit
        if(statementInput != EXIT_RESPONSE) {
	        result = SQLParser::parseSQLString(statementInput);
		    
            if(!result->isValid()) {
                delete result;
                cout << "Invalid SQL: " <<  statementInput << endl;
            } else {
                execute(result);
                delete result;
            }     
	    }
    } 

    db.close(0);
    environment.close(0);

    return 0;
}

void execute(SQLParserResult* &result) {
    //handle sql statements accordingly
    for(uint i = 0; i < result->size(); ++i) {
        switch(result->getStatement(i)->type()) {
            case kStmtSelect:
                cout << handleSelect((const SelectStatement *) result->getStatement(i)) << endl;
                break;
            case kStmtCreate:
                cout << handleCreate((const CreateStatement *) result->getStatement(i)) << endl;
                cout << "Calling SQLExec.execute" << endl;
                SQLExec::execute(result->getStatement(i));

                break;
            default: 
                cout << "Invalid SQL/Not supported: " <<  endl;
                break;
        }
    }
}

//note this is not super extensive
string handleSelect(const SelectStatement* statement) {
    string output = "SELECT ";
    bool hasComma = false;

    //handle each expression
    for (Expr *expr : *statement->selectList) {
        if (hasComma)
            output += ", ";

        //handle typess
        switch (expr->type) {
            case kExprOperator:
                if(expr == nullptr)
                    output += "NULL";
			
                else {
                    //if unary not
                    if (expr->opType == Expr::NOT)
                        output += "NOT ";

                    // Left expression
                    output += expressToString(expr->expr) + " ";

                    //handle operator converted
                    switch (expr->opType) {
                        case Expr::OR:
                            output += "OR";
                            break;
                        case Expr::SIMPLE_OP:
                            output += expr->opChar;
                            break;
                        case Expr::AND:
                            output += "AND";
                            break;
                        default:
                            break;
                    }

                    // Right expression converted
                    if (expr->expr2 != nullptr)
                        output += " " + expressToString(expr->expr2);
                }
                break;

            //convert nums to string again.... yayyyyy
            case kExprLiteralFloat:
                output += to_string(expr->fval);
                break;
            case kExprLiteralInt:
                output += to_string(expr->ival);
                break;

            case kExprColumnRef:
                if (expr->table != nullptr)
                    output += string(expr->table) + ".";
            case kExprLiteralString:
                output += expr->name;
                break;
            case kExprStar:
                output += "*";
                break;

            default:
                output += "(UNDEFINED)";  //undefined expression tyep
                break;
        }

        hasComma = true;
    }
    output += " FROM ";

    return output;
}

string handleCreate(const CreateStatement* statement) {
    string output = "CREATE TABLE ";

    // add the table or index name to "tables"
    if(statement->type == CreateStatement::CreateType::kIndex)
        tables.push_back(statement->indexName);
    else if(statement->type == CreateStatement::CreateType::kTable)
        tables.push_back(statement->tableName);
    
    if (statement->type != CreateStatement::kTable)
        return output + "(placeholder)";
    
    if (statement->ifNotExists)
        output += "IF NOT EXISTS ";
    
    //handle table name 
    output += string(statement->tableName) + " (";
    bool hasComma = false;

    //handle column types for table
    for (ColumnDefinition *columnEntry : *statement->columns) {
        if (hasComma)
            output += ", ";

        //account for different types
        switch (columnEntry->type) {
	    case ColumnDefinition::INT:
                output += " INT";
                break;
            case ColumnDefinition::TEXT:
                output += " TEXT";
                break;
            case ColumnDefinition::DOUBLE:
                output += " DOUBLE";
                break;
           
            default:
                output += "(Undefined)";
                break;
        }
        hasComma = true;
    }

    //close output and return
    output += ")";
    return output;
}

//note this is not super extensive
string expressToString(const Expr* expression) {
    string output = "";

    //handle different expression types (handles most cases)
    switch(expression->type) {
        //handle special symbols
        case kExprStar:
            output += "*";
            break;
        case kExprLiteralString:
            output += expression->name;
            break;
        case kExprColumnRef:
            if (expression->table != NULL)
                output += string(expression->table) + ".";


        //convert the float and long values to a string
        case kExprLiteralFloat:
            output += to_string(expression->fval);
            break;
        case kExprLiteralInt:
            output += to_string(expression->ival);
            break;
        
        //undefined
        default:
            output += "(UNDEFINED)";
            break;
    }

    return output;
}
