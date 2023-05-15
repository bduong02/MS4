// I pulled this file from the solution repository; we will fill it out
// TODO: Update the Makefile to compile the new files that were added from the solution repo

/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#include "SQLExec.h"
#include "schema_tables.h"
#include "ParseTreeToString.h"

using namespace std;
using namespace hsql;

const string TABLE_NAME = "table_name";


// define static data
Tables *SQLExec::tables = nullptr;
Columns *SQLExec::columns = nullptr;
Indices *SQLExec::indices = nullptr;

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
    cout << "Init tables"<<endl;
    // FIXME: initialize _tables table, if not yet present
    initialize_schema_tables();

    if(tables == nullptr) // lines 63-64 were taken from Canvas
        tables = new Tables();
    if(columns == nullptr) // lines 63-64 were taken from Canvas
        columns = new Columns();

    cout << "done initializing tables"<<endl;

    try {
        switch (statement->type()) {
            case kStmtCreate:
                cout << "going to create"<<endl;
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
    // convert column definition data type to ColumnAttribute data type, since their enums are different
    if(col->type == ColumnDefinition::DataType::TEXT){
        column_attribute = ColumnAttribute(ColumnAttribute::DataType::TEXT);
    }else if(col->type == ColumnDefinition::DataType::INT){
        column_attribute = ColumnAttribute(ColumnAttribute::DataType::INT);
    }else{ // for now, we're only handling int and text
        throw SQLExecError("Invalid data type: needs to be int or text");
    }
    column_name = Identifier(col->name);
    // throw SQLExecError("not implemented");  // FIXME
}

// Creating a new index:
// •	Get the underlying table. 
// •	Check that all the index columns exist in the table.
// •	Insert a row for each column in index key into _indices. I recommend having a static reference to _indices in SQLExec, as we do for _tables.
// •	Call get_index to get a reference to the new index and then invoke the create method on it.
// Dropping an index:
// •	Call get_index to get a reference to the index and then invoke the drop method on it.
// •	Remove all the rows from _indices for this index.
// Showing index:
// •	Do a query (using the select method) on _indices for the given table name.
// Dropping a table:
// •	Before dropping the table, drop each index on the table.

QueryResult *SQLExec::create(const CreateStatement *statement) {
    if(statement->type == CreateStatement::CreateType::kIndex){
        cout << "in create index " << "Table name: " << statement->tableName << endl
             << "index name: " << statement->indexName
             << " index type: " << statement->indexType << " index columns: ";

        for(char* indexCol : *(statement->indexColumns))
            cout << indexCol << " ";
        
        cout << endl;

        // get the columns of the table to check if the index column exists
        ColumnAttributes columnAttributes;
        ColumnNames tableColumnNames;
        cout <<"calling getColumns from create"<<endl;
        Tables::get_columns(statement->tableName, tableColumnNames, columnAttributes);

        cout << "The columns in " << statement->tableName << ": ";
        for(int i=0; i < tableColumnNames.size(); i++)
            cout << tableColumnNames.at(i) << " : " << columnAttributes.at(i).get_data_type() << endl;
        
        bool found; // whether the index columns are columns in the table
        int index; // array index for columnNames and columnAttributes
        
        // make sure all the index columns are in the table: TODO: finish this later
        // for(char* indexCol : *(statement->indexColumns)){
        //     found = false;
        //     index = 0;

        //     while(!found && index < tableColumnNames.size()){
        //         if(tableColumnNames[index] == indexCol)
        //             found = true;
        //     }
        //     if(!found){
        //         cout << "Index column " << indexCol << " is not in the table" << endl;
        //         // return new QueryResult("Error: index column not in the table");
        //     }
        // }

        int seqInIndex = 1; // to track the order of the columns in a composite index
        ValueDict* indicesRow;

        // TODO: check if statement->tableName is null - will the parser do that?
        (*indicesRow)["table_name"] = Value(statement->tableName);
        (*indicesRow)["index_name"] = Value(statement->indexName);
        (*indicesRow)["index_type"] = Value(statement->indexType);

        if(statement->indexType == "BTREE") 
            (*indicesRow)["is_unique"] = false;
        else 
            (*indicesRow)["is_unique"] = true;

        cout << "added the first 4 cols to indicesRow"<<endl;
        
        // insert a row into _indices for each column in the index 
        for(char* indexCol : *(statement->indexColumns)){
            (*indicesRow)["column_name"] = Value(indexCol);
            (*indicesRow)["seq_in_index"] = Value(seqInIndex);
            cout << "about to insert into indices"<<endl;
            indices->insert(indicesRow);
            cout << "inserted into indices"<<endl;
            seqInIndex++;
        }
        
    }else if(statement->type == CreateStatement::CreateType::kTable){
        cout << endl << "In create table"<<endl;
        Identifier columnName; // stores name of each column
        ColumnAttribute columnAttribute = ColumnAttribute(ColumnAttribute::DataType::INT); // data type of each column: use int as a placeholder since I need an argument for the constructor
        ColumnNames columnNames; // list of all column names
        ColumnAttributes columnAttributes; // list of all column data types
        bool allDigits = false; // checks if the column name is all digits
        bool acceptableColName = true;

        // check if the table name is a reserved word
        if(ParseTreeToString::is_reserved_word(statement->tableName))
            return new QueryResult("table name is a reserved word");
        
        // check constraints
        for(ColumnDefinition* column : *(statement->columns)){
            column_definition(column, columnName, columnAttribute); // get name and data type

            // check if the column name is all digits
            for(char c : columnName){
                if(!isdigit(c))
                    allDigits = false;
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

            if(ParseTreeToString::is_reserved_word(columnName)){
                return new QueryResult("col name is a reserved word");
            }

            // check if the data type is valid (int or text)
            if(columnAttribute.get_data_type() != ColumnAttribute::DataType::INT 
                && columnAttribute.get_data_type() != ColumnAttribute::DataType::TEXT){
                cout << "Invalid data type: " << columnAttribute.get_data_type() << endl;
                return new QueryResult("Invalid data type");
            }
            
            columnAttributes.push_back(columnAttribute);
            columnNames.push_back(columnName);
        }

        // add the table name to tables
        ValueDict* tableRow = new ValueDict();

        (*tableRow)["table_name"] = Value(statement->tableName);
        cout << "printing table row: ";

        for(auto i : (*tableRow))
                cout << i.first << ", " << (i.second).s << endl;

        // tables->insert(tableRow); // seg fault here

        // add the column names and types to Columns
        ValueDict* columnsRow = new ValueDict(); // row to add to the Columns table
        (*columnsRow)["table_name"] = Value(statement->tableName);

        // for each column, add a row to the Columns table
        for(int i=0; i < columnNames.size(); i++){
            (*columnsRow)["column_name"] = Value(columnNames[i]);

            if(columnAttributes[i].get_data_type() == ColumnAttribute::DataType::INT)
                (*columnsRow)["data_type"] = Value("INT");
            else
                (*columnsRow)["data_type"] = Value("TEXT");
                
            for(auto pairr : (*columnsRow))
                cout << pairr.first << ", " << pairr.second.s << endl;

            columns->insert(columnsRow);

            cout << "inserted"<<endl;
            (*columnsRow).erase("column_name"); // get rid of the old column name to add a row with the next column name
            (*columnsRow).erase("data_type"); // get rid of the old datatype to add a row with the next column name
        }

        ValueDicts* v = new ValueDicts(); // empty ValueDicts for the QueryResult

        cout << "will return" << endl;
        return new QueryResult(&columnNames, &columnAttributes, v, "");
    }
    return new QueryResult();
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}


QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("index_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("column_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("seq_in_index");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back("index_type");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = Value(string(statement->tableName));
    Handles *handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

