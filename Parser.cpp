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

//Preconditions: the SqlParseResult parse tree MUST be processed 
//               beforehand by the SQLParser::parseSQLString function
//Process: for now, just returns the resulting statement result as a string
//Postconditions: none
string execute(hsql::SQLParserResult* result);

int main(int argc, char* argv[]) {
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
        return 1;
    }
	

    //read sql statements, parse & handle accordingly
    const string EXIT_RESPONSE = "quit";
    string statementInput = "";
    while(statementInput != EXIT_RESPONSE) {
        
        cout << "SQL> ";
        cin >> statementInput;
        hsql::SQLParserResult* result = nullptr;
        if(statementInput != EXIT_RESPONSE) {
	    result = parseSQLString(statementInput);
	    if(isValid(result))
	    {
		cout << execute(result);
	    }
	    //include logic here to handle
	}
	} else {
	    exit(EXIT_FAILURE);
	}
	delete result;
            
        }
    } 
    return 0;
}


string execute(hsql::SQLParserResult* result) {
	
    //handle logic etc...
    return "";
}
