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
#include "ms2Tests.cpp"
using namespace std;
using namespace hsql;

DbEnv *_DB_ENV;

//Preconditions: the SqlParseResult parse tree MUST be processed 
//               beforehand by the SQLParser::parseSQLString function
//Process: for now, just returns the resulting statement result as a string
//Postconditions: none
string execute(SQLParserResult* &result, string statementInput);

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

    if(MS2Tests::slottedPageTests() == false) {
        cout << "Slotted Page Tests failed! Moving on to shell...." << endl << endl;
    } else {
        cout << "Slotted Page Tests passed! Moving on to shell...." << endl << endl;
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
		    cout << execute(result, statementInput) << endl;
            delete result;
	    } else {
	        exit(EXIT_FAILURE);
	    } 
    } 

    return 0;
}
string execute(SQLParserResult* &result, string statementInput) {
    string resultStr = "";
    //handle invalid statement
    if(!result->isValid()) {
        return "Invalid SQL: " + statementInput;
    } 

    //handle sql statements accordingly
    SQLStatement* inputStatement = result->getMutableStatement(0);
    switch(inputStatement->type()) {
        case kStmtSelect:
            resultStr = "";
            break;
        case kStmtCreate:
            break;
        default:
            resultStr = "Invalid SQL: " + statementInput;
            break;
    }
    return resultStr;   
}

