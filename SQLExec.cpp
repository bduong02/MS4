// I pulled this file from the solution repository; we will fill it out
// TODO: Update the Makefile to compile the new files that were added from the solution repo

/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#include "SQLExec.h"
#include "schema_tables.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row.at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    // FIXME
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // FIXME: initialize _tables table, if not yet present
    // initialize_schema_tables();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

/**
     * Pull out column name and attributes from AST's column definition clause
     * @param col                AST column definition
     * @param column_name        returned by reference
     * @param column_attributes  returned by reference
     */
// column attributes are data types
void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_attribute = ColumnAttribute((ColumnAttribute::DataType)col->type);
    column_name = Identifier(col->name);
    // throw SQLExecError("not implemented");  // FIXME
}

// // name constraints:
// and also that table_name and column_name are acceptable (
// consisting of one or more basic Latin letters, digits 0-9, dollar signs, underscores, but not all digits, 
// and not be an SQL reserved word). We won't allow quoted names in our pared down SQL.

QueryResult *SQLExec::create(const CreateStatement *statement) {
    cout << "In create"<<endl;
    Identifier columnName; // stores name of each column
    ColumnAttribute columnAttribute = ColumnAttribute(ColumnAttribute::DataType::INT); // data type of each column: use int as a placeholder since I need an argument for the constructor
    ColumnNames columnNames; // list of all column names
    ColumnAttributes columnAttributes; // list of all column data types
    bool allDigits = false; // checks if the column name is all digits
    bool acceptableColName = true;
    
    // check column data type and name
    for(ColumnDefinition* column : *(statement->columns)){
        column_definition(column, columnName, columnAttribute); // get name and data type

        // check if the column name is all digits
        for(char c : columnName){
            if(!isdigit(c))
                allDigits = true;
        }
        if(allDigits){
            cout << "The column name cannot be all digits" << endl;
            return new QueryResult("The column name cannot be all digits");
        }

        // checks if the column name is acceptable (all letters, digits, $, or _)
        for(char c : columnName){
            if(!isalnum(c) && c != '$' && c != '_')
                acceptableColName = false;
        }
        if(!acceptableColName){
            cout << "The column name has unacceptable characters" << endl;
            return new QueryResult("Unacceptable column name");
        }

        // TODO: check if it's an SQL keyword...

        // check if the data type is valid (int or text)
        if(columnAttribute.get_data_type() != ColumnAttribute::DataType::INT 
            && columnAttribute.get_data_type() != ColumnAttribute::DataType::TEXT){
            cout << "Invalid data type: " << endl;
            return new QueryResult("Invalid data type");
        }
        
        columnAttributes.push_back(columnAttribute);
        columnNames.push_back(columnName);
    }

    // add the table name to tables
    ValueDict tableRow = ValueDict();
    tableRow.insert({"table_name", Value(statement->tableName)});
    tables->insert(&tableRow);

    // add the column names and types to Columns
    ValueDict columnsRow = ValueDict(); // row to add to the Columns table
    columnsRow["table_name"] = Value(statement->tableName);

    // for each column, add a row to the Columns table
    for(int i=0; i < columnNames.size(); i++){
        columnsRow["column_name"] = Value((columnNames[i]));
        columnsRow["datatype"] = Value(columnAttributes[i].get_data_type());
        
        // I can't call the Columns constructor
        // Columns columnsTable;
        // columnsTable.insert(&columnsRow);
        // columnsRow.erase("column_name"); // get rid of the old column name to add a row with the next column name
        // columnsRow.erase("datatype"); // get rid of the old datatype to add a row with the next column name
    }

    ValueDicts* v = new ValueDicts(); // empty ValueDicts for the QueryResult
    return new QueryResult(&columnNames, &columnAttributes, v, "");
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_tables() {
    Handles* handles = tables->select(); // get handles to all the rows from "tables"
    ValueDict* rows; 
    for(Handle handle : *handles){
        // Each handle is a pair of (BlockID, RecordID)
        rows = tables->project(handle); // execute "SELECT *" to get the row from "tables" as a ValueDict
        // Assume "table_name" is the key
        cout << (*rows)["table_name"].n << endl;
        cout << (*rows)["table_name"].s << endl;
        
    }

    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

