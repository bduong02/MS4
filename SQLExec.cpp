// I pulled this file from the solution repository; we will fill it out
// TODO: Update the Makefile to compile the new files that were added from the solution repo

/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#include "SQLExec.h"
#include "schema_tables.h"
#include "schema_tables.cpp"
#include "ParseTreeToString.h"
#include "ParseTreeToString.cpp"


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

void SQLExec::initialize_schema_tables() {
    // if(tables == nullptr){
    //     tables = new Tables();
    //     tables->create_if_not_exists();
    //     tables->close();
    // }
    // if(columns == nullptr){
    //     columns = new Tables();
    //     columns->create_if_not_exists();
    //     columns->close();
    // }if(indices == nullptr){
    //     indices = new Tables();
    //     indices->create_if_not_exists();
    //     indices->close();
    // }
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    cout << "Init tables"<<endl;
    
    if(tables == nullptr){
        tables = new Tables();
        tables->create_if_not_exists();
        tables->close();
    }
    if(columns == nullptr){
        columns = new Columns();
        columns->create_if_not_exists();
        columns->close();
    }if(indices == nullptr){
        indices = new Indices();
        indices->create_if_not_exists();
        indices->close();
    }


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
// •	Get the underlying table. - I didn't
// •	Check that all the index columns exist in the table. - I think it's OK
// •	Insert a row for each column in index key into _indices. -seg fault with HeapFile.get()
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
        // TODO: check if the table exists
        // DbRelation table = Tables::get_table(statement->tableName);
        

        // get the columns of the table to check if the index column exists
        ColumnAttributes columnAttributes = ColumnAttributes();
        ColumnNames tableColumnNames = ColumnNames();
        cout <<"calling getColumns from create"<<endl;
        Tables::get_columns(statement->tableName, tableColumnNames, columnAttributes);

        cout << "tableColumnNames size: "<<tableColumnNames.size() << "columnAttrs size: "
            <<columnAttributes.size() << endl;

        // cout << "The columns in " << statement->tableName << ": ";
        // for(int i=0; i < tableColumnNames.size(); i++)
        //     cout << tableColumnNames.at(i) << " : " << columnAttributes.at(i).get_data_type() << endl;
        
        bool found; // whether the index columns are columns in the table
        int index; // array index for columnNames and columnAttributes
        
        // Check that all the index columns exist in the table. TODO: finish/test this later
        for(char* indexCol : *(statement->indexColumns)){
            found = false;
            index = 0;

            while(!found && index < tableColumnNames.size()){
                if(tableColumnNames[index] == indexCol)
                    found = true;
            }
            if(!found){
                cout << "Index column " << indexCol << " is not in the table" << endl;
                // return new QueryResult("Error: index column not in the table");
            }
        }

        // Insert a row for each column in index key into _indices
        int seqInIndex = 1; // to track the order of the columns in a composite index
        ValueDict* indicesRow = new ValueDict();

        // TODO: check if statement->tableName is null - will the parser do that?
        (*indicesRow)["table_name"] = Value(statement->tableName);
        (*indicesRow)["index_name"] = Value(statement->indexName);
        (*indicesRow)["index_type"] = Value(statement->indexType);

        cout << "added the first 3 cols to indicesRow" <<endl;

        if(statement->indexType == "BTREE") 
            (*indicesRow)["is_unique"] = false;
        else 
            (*indicesRow)["is_unique"] = true;

        cout << "added the first 4 cols to indicesRow"<<endl;
        
        // insert a row into _indices for each column in the index 
        for(char* indexCol : *(statement->indexColumns)){
            (*indicesRow)["column_name"] = Value(indexCol);
            (*indicesRow)["seq_in_index"] = Value(seqInIndex);

            cout << endl << endl << "------------------------ indicesRow: -------------------------" << endl;
            for(auto element : *indicesRow){
                // change this to account for bool
                cout << element.first << " ";
                if(element.second.data_type == ColumnAttribute::DataType::INT) cout << element.second.n << endl;
                else if(element.second.data_type == ColumnAttribute::DataType::TEXT) cout << element.second.s << endl;
                else cout << "The data type = bool"<<endl;
            }

            cout << "Is indices null? " << (indices==nullptr ? "Y":"N")<<endl;

            cout << "about to insert into indices"<<endl;
            indices->insert(indicesRow);
            cout << "inserted into indices"<<endl;
            seqInIndex++;
        }
        
        // Call get_index to get a reference to the new index and then invoke the create method on it.
        indices->get_index(Identifier(statement->tableName), Identifier(statement->indexName)).create();
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

            cout << "in sqlexec::create, about to call Columns::insert"<<endl;
            columns->insert(columnsRow);

            cout << "Returned from columns::insert, back in create"<<endl;
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
    return new QueryResult("not implemented"); // FIXME
}

// Return a table of table names, equivalent to:
// SELECT table_name FROM _tables WHERE table_name NOT IN ("_tables", "_columns");

QueryResult *SQLExec::show(const ShowStatement *statement) {
    // get all the columns in _tables
    ColumnNames columnNames;
    ColumnAttributes columnAttributes;
    tables->get_columns(TABLE_NAME, columnNames, columnAttributes);

    // remove "_tables" and "_columns" from the list of columns in _tables
    ColumnNames::iterator it = find(columnNames.begin(), columnNames.end(), TABLE_NAME);

    // if "_tables" isn't found in the table, there was an error
    if(it == columnNames.end())
        return new QueryResult("Error: " + TABLE_NAME + " not found in " + TABLE_NAME);

    columnNames.erase(it);

    it = find(columnNames.begin(), columnNames.end(), TABLE_NAME);

    // if "_tables" isn't found in the table, there was an error
    if(it == columnNames.end())
        return new QueryResult("Error: " + Columns::TABLE_NAME + " not found in " + TABLE_NAME);

    columnNames.erase(it);

    cout << "columnNames: ";
    for(Identifier colName : columnNames) cout << colName << ", " <<endl;


    // tables->project()

    // // I need a row handle to _tables for project()
    // tables->project()

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

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
     return new QueryResult("show index not implemented"); // FIXME
}

// Dropping an index:
// •	Call get_index to get a reference to the index and then invoke the drop method on it.
// •	Remove all the rows from _indices for this index.
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    if(statement->type != DropStatement::EntityType::kIndex){
        cout << "Error: attempted to call drop_index() on a non-index entity" << endl;
        return new QueryResult("Error: attempted to call drop_index() on a non-index entity");
    }

// •	Call get_index to get a reference to the index and then invoke the drop method on it.
    // Assume statement->name is the table name; I can't tell if it is.
    // auto indexTable = indices->get_index(statement->name, statement->indexName);
    // indexTable.drop();

    // query the indices table so we can know which table this index belongs to
    // ColumnNames colNamesForIndicesTable;
    // bool isHash, isUnique; 
    // ValueDict* where = new ValueDict();
    // (*where)["index_name"] = statement->indexName;
    // indices->select(where);

    // assume there is only one 

    // for(ColumnName col : colNamesForIndicesTable){

    // }
        return new QueryResult("not implemented");

}

