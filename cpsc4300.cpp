//Ryan Silveira
// 4/16/2023
//CPSC 4300 - Milestone 1

#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <stack>
#include "db_cxx.h"
#include "SQLParser.h"
#include "ParseTreeToString.h"
#include "SQLExec.h"
using namespace std;
using namespace hsql;

//db environment variables
u_int32_t env_flags = DB_CREATE | DB_INIT_MPOOL; //If the environment does not exist, create it.  Initialize memory.
u_int32_t db_flags = DB_CREATE; //If the database does not exist, create it.
DbEnv *_DB_ENV;

int main(int argc, char **argv) {
   if(argc != 2){
        cerr << "Missing path." << endl;
        return -1;
    }
    string dbPath = argv[1];

    //init db environment locally and globally
    DbEnv environment(0U);
    try {
        environment.set_message_stream(&cout);
	    environment.set_error_stream(&cerr);
	    environment.open(dbPath.c_str(), env_flags, 0);
    } catch(DbException &E) {
        cout << "Error creating DB environment" << endl;
        exit(EXIT_FAILURE);
    }
    _DB_ENV = &environment;
    initialize_schema_tables();

    stack<string> transactions;

    //main parse loop
    while(true){
        string sqlcmd;
        cout << "SQL> ";
        getline(cin, sqlcmd);
        if(sqlcmd == "quit"){
            break;
        }
        if (sqlcmd.find("transaction") != string::npos) {
            cout << "transaction" << endl;
        }
        else {
            SQLParserResult* result = SQLParser::parseSQLString(sqlcmd);
            if(!result->isValid()){
                cout << "Invalid command: " << sqlcmd << endl;
            } else {
                for(uint i = 0; i < result->size(); ++i){
                    const SQLStatement* statement = result->getStatement(i);
                    try {
                        cout << ParseTreeToString::statement(statement) << endl;
                        QueryResult *q_result = SQLExec::execute(statement);
                        cout << *q_result << endl;
                        delete q_result;
                    }
                    catch (SQLExecError &e) {
                        cerr << e.what() << endl;
                    }
                }
            }
            delete result;
        }
    }

    //close db environment
    environment.close(0);
    return 0;
} 