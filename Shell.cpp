//Filename: driver.cpp
//Authors: Ishan Parikh, Bryan Duong
//Date 4-1-23
//CPSC 5300
//Purpose: SQL statement parser impelementation in C++
//--not extensive, test-driven only to sample input at the moment
#include <iostream>
#include <cstdlib> 
#include <string>
#include "db_cxx.h"
#include "SQLParser.h"
#include "SQLParserResult.h"
using namespace std;
using namespace hsql;

//Preconditions: the SqlParseResult parse tree MUST be processed 
//               beforehand by the SQLParser::parseSQLString function
//Process: for now, just returns the resulting statement result as a string
//Postconditions: none
string execute(SQLParserResult* &result, string statementInput);

int main(int argc, char* argv[]) {
    /*
    //handle cmd arguments (will ignore extra args)
    if(argc <= 1) {
        return 1;
    }
    string envDirPath = argv[1];
    //initialize DB environment in passed directory path
    DbEnv environment(0U);
    try {
        environment.set_message_stream(&cout);
	    environment.set_error_stream(&cerr);
	    environment.open(envDirPath.c_str(), DB_CREATE, 0);
    } catch(DbException &E) {
        cout << "Error creating DB environment" << endl;
        exit(EXIT_FAILURE);
    }

    //read sql statements, parse & handle accordingly
    const string EXIT_RESPONSE = "quit";
    string statementInput = "";
    while(statementInput != EXIT_RESPONSE) {
        cout << "SQL> ";
        cin >> statementInput;
        SQLParserResult* result = nullptr;
        //handle input with appropriate action or quit
        if(statementInput != EXIT_RESPONSE) {
	        result = SQLParser::parseSQLString(statementInput);
		    cout << execute(result, statementInput) << endl;
            delete result;
	    } else {
	        exit(EXIT_FAILURE);
	    } 
    } 
    */
    return 0;
}
string execute(SQLParserResult* &result, string statementInput) {
    string resultStr = "";
    //handle invalid statement
    if(!result->isValid()) {
        return "Invalid SQL: " + statementInput;
    } 
    SQLStatement* inputStatement = result->getMutableStatement(0);
    switch(inputStatement->type()) {
        case kStmtSelect:
            break;
        case kStmtCreate:
            break;
        default:
            resultStr = "Invalid SQL: " + statementInput;
            break;
    }
    return resultStr;   
}
//Preconditions: the statement must be of specified type 
//               and be verified asa  valid result
//Process: formats a select statement into a printable string
string formatSelect(SelectStatement* result) {
    return "";
}
//Preconditions: the statement must be of specified type 
//               and be verified asa  valid result
//Process: formats a create statement into a printable string
string formatCreate(CreateStatement* result) {
    return "";
}
