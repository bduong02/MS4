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

void transactionMessage(stack<string> &transactions, string cmd);

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
        // string db01 = dbPath + "/__db.001";
        // int lockFile = open(db01.c_str(), O_RDWR, 0666);
        // if (lockFile == -1) {
        //     cout << "Could not open lock file" << endl;
        //     return -1;
        // }
        // // initialize lock
        // lock.l_type = F_WRLCK;
        // lock.l_whence = SEEK_SET;
        // lock.l_start = 0;
        // lock.l_len = 0;

        // if (fcntl(lockFile, F_SETLK, &lock) == -1) {
        //     cout << "Database is locked" << endl;
        //     return -1;
        // }
        // close(lockFile);
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
            // the parser doesn't consider these as real statements so i'm doing it myself
            transactionMessage(transactions, sqlcmd);
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
                        QueryResult *q_result = SQLExec::execute(statement, dbPath);
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

void transactionMessage(stack<string> &transactions, string cmd) {
    if (cmd.find("begin") != string::npos) {
        transactions.push(cmd);
        cout << "Opened transaction level " << transactions.size() << endl;
    }
    else if (cmd.find("commit") != string::npos) {
        if (transactions.size() == 0) {
            cout << "No open transactions, cannot commit" << endl;
            return;
        }
        transactions.pop();
        if (transactions.size() > 0)
            cout << "Committed transaction level " << transactions.size() + 1
                 << ", transaction level " << transactions.size() << " pending"
                 << endl;
        else
            cout << "Committed transaction level " << transactions.size() + 1
                 << ", no transactions pending" << endl;
    }
    else if (cmd.find("rollback") != string::npos) {
        if (transactions.size() == 0) {
            cout << "No open transactions, cannot rollback" << endl;
            return;
        }
        transactions.pop();
        if (transactions.size() > 0) 
            cout << "Transaction level " << transactions.size() + 1 
                 << " rolled back, transaction level " << transactions.size()
                 << " pending" << endl;
        else
            cout << "Transaction level " << transactions.size() + 1
                 << " rolled back, no transactions pending" << endl;
    }
    else {
        cout << "invalid transaction action" << endl;
    }
}