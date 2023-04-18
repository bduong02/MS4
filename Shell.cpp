//Filename: driver.cpp
//Authors: Ishan Parikh, Bryan Duong
//Date 4-1-23
//CPSC 5300
//Purpose: Milestone 1 Impelementation
#include <iostream>
#include <cstdlib> 
#include <string>
#include "db_cxx.h"
#include "SQLParser.h"
#include "SQLParserResult.h"
#include "heap_storage.h"
#include "ms2Tests.cpp"
using namespace std;
using namespace hsql;

DbEnv *_DB_ENV;

//Preconditions: the SqlParseResult parse tree MUST be processed 
//               beforehand by the SQLParser::parseSQLString function
//Process:       just matches the result to a string and prints it
//Postconditions: none
void execute(SQLParserResult* &result);
string handleSelect(const SelectStatement* statement);
string handleCreate(const CreateStatement* statement);
string expressToString(const Expr* expression);

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
	    environment.open(envDirPath.c_str(), DB_CREATE, 0);
    } catch(DbException &E) {
        cout << "Error creating DB environment" << endl;
        exit(EXIT_FAILURE);
    }
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
                    output += "null";
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

        if (expr->alias != nullptr) output += string(" AS ") + string(expr->alias);


        hasComma = true;
    }
    output += " FROM ";

    if (statement->whereClause != nullptr)
        output += " WHERE " + expressToString(statement->whereClause);

    return output;
}

string handleCreate(const CreateStatement* statement) {
    string output = "CREATE TABLE ";
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
            case ColumnDefinition::DOUBLE:
                output += " DOUBLE";
                break;
            case ColumnDefinition::INT:
                output += " INT";
                break;
            case ColumnDefinition::TEXT:
                output += " TEXT";
                break;
            default:
                output += " ...";
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
        case kExprColumnRef:
            if (expression->table != NULL)
                output += string(expression->table) + ".";
        case kExprLiteralString:
            output += expression->name;
            break;

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

    if (expression->alias != nullptr) output+= string(" AS ") + expression->alias;

    return output;
}