#include "SQLExec.h"
#include "schema_tables.h"
#include "schema_tables.cpp"
#include "ParseTreeToString.h"
#include "ParseTreeToString.cpp"


using namespace std;
using namespace hsql;

const string SUCCESS_MESSAGE = "success"; // message for a successful QueryResult


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
    delete column_names;
    column_names = nullptr;
    delete column_attributes;
    column_attributes = nullptr;
    delete rows;
    rows = nullptr;
}


void SQLExec::initialize_schema_tables() {
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
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // repeating this code because can't call initialize_schema_tables() with the "this" pointer
    // in a static method
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
void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    // convert column definition data type to ColumnAttribute data type, since their enums are different
    if(col->type == ColumnDefinition::DataType::TEXT){
        column_attribute = ColumnAttribute(ColumnAttribute::DataType::TEXT);
    }else if(col->type == ColumnDefinition::DataType::INT){
        column_attribute = ColumnAttribute(ColumnAttribute::DataType::INT);
    }else if(col->type == ColumnDefinition::DataType::UNKNOWN) // Assume the "UNKNOWN" type is always boolean, since there's no boolean in ColumnDefinition::DataType
        column_attribute = ColumnAttribute(ColumnAttribute::DataType::BOOLEAN);
    else{
        throw SQLExecError("Invalid data type: needs to be int, text, or boolean");
    }
    column_name = Identifier(col->name);
}

// returns a QueryResult for a table, including all of the table's rows, column names, column attributes,
// and a message indicating success
// tableName: the name of the table for which to get the information
QueryResult* SQLExec::getSuccessfulQueryResult(Identifier tableName){
    ColumnNames colNames;
    ColumnAttributes colAttributes;

    // get the HeapTable object for this table name
    DbRelation* table = &(tables->get_table(tableName));

    // get the column names and attributes of that table
    tables->get_columns(tableName, colNames, colAttributes);

    // get all rows in _tables for the QueryResult
    Handles* handles = table->select(); // get handles to each row (SELECT * FROM tableName)
    ValueDicts* rows;

    // get all the columns as a ValueDict for each row; add it to the list of ValueDicts
    for(Handle handle : *handles)
        rows->push_back(*(table->project(handle))); 

    delete handles;
    handles = nullptr;

    return new QueryResult(&colNames, &colAttributes, rows, SUCCESS_MESSAGE);
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    if(statement->type == CreateStatement::CreateType::kIndex){  
        // check if the table exists
        ValueDict where; // where condition: table_name == the name of the table in the create index statement
        where["table_name"] = Value(statement->tableName);  
        Handles* tableRowHandles = tables->select(&where); // get the handle to the row in _tables.

        if(tableRowHandles->empty()){
            cout << "Error: the table doesn't exist" << endl;
            return new QueryResult("Error: the table doesn't exist");
        }
             
        // get the columns of the table to check if the index column exists
        ColumnAttributes columnAttributes = ColumnAttributes();
        ColumnNames tableColumnNames = ColumnNames();
        bool found; // whether the index columns are columns in the table
        int index; // array index for columnNames and columnAttributes
        
        Tables::get_columns(statement->tableName, tableColumnNames, columnAttributes);

        // Check that all the index columns exist in the table
        for(char* indexCol : *(statement->indexColumns)){
            found = false;
            index = 0;

            while(!found && index < tableColumnNames.size()){
                if(tableColumnNames[index] == indexCol)
                    found = true;
                index++;
            }
            if(!found){
                cout << "Index column " << indexCol << " is not in the table" << endl;
                return new QueryResult("Error: index column not in the table");
            }
        }

        // Insert a row for each column in index key into _indices
        int seqInIndex = 1; // to track the order of the columns in a composite index
        ValueDict* indicesRow = new ValueDict(); // row(s) to insert into _indices

        (*indicesRow)["table_name"] = Value(statement->tableName);
        (*indicesRow)["index_name"] = Value(statement->indexName);
        (*indicesRow)["index_type"] = Value(statement->indexType);

        if(statement->indexType == "BTREE") 
            (*indicesRow)["is_unique"] = false;
        else 
            (*indicesRow)["is_unique"] = true;
        
        // insert a row into _indices for each column in the index 
        for(char* indexCol : *(statement->indexColumns)){
            (*indicesRow)["column_name"] = Value(indexCol);
            (*indicesRow)["seq_in_index"] = Value(seqInIndex);
            indices->insert(indicesRow);
            seqInIndex++;
        }

        delete indicesRow;
        indicesRow = nullptr;

        // Call get_index to get a reference to the new index and then invoke the create method on it.       
        DbIndex* newIndex = &indices->get_index(Identifier(statement->tableName), Identifier(statement->indexName));
        newIndex->create();
        delete newIndex;
        newIndex = nullptr;

        // the names and data types of the columns in _indices
         ColumnNames indicesColNames = {"table_name", "index_name", "seq_in_index", 
                                            "column_name", "index_type", "is_unique"};
         ColumnAttributes indicesDataTypes = {ColumnAttribute(ColumnAttribute::TEXT), ColumnAttribute(ColumnAttribute::TEXT), ColumnAttribute(ColumnAttribute::INT), 
                                                    ColumnAttribute(ColumnAttribute::TEXT), ColumnAttribute(ColumnAttribute::TEXT), ColumnAttribute(ColumnAttribute::BOOLEAN)};


        // since there are no rows yet in the new index, use an empty ValueDicts for the QueryResult
        return new QueryResult(&indicesColNames, &indicesDataTypes, new ValueDicts(), SUCCESS_MESSAGE);
    }else if(statement->type == CreateStatement::CreateType::kTable){
        // check if the table exists
        ValueDict where; // where condition: table_name == the name of the table in the create index statement
        where["table_name"] = Value(statement->tableName);  
        Handles* tableRowHandles = tables->select(&where); // get the handle to the row in _tables.

        if(!tableRowHandles->empty()){
            cout << "Error: table already exists" << endl;
            return new QueryResult("Error: table already exists");
        }

        // check if the table name is acceptable
        if(!is_acceptable_identifier(statement->tableName)){
            cout << "Error: Table name not an acceptable identifier" << endl << endl;
            return new QueryResult("Error: Table name not an acceptable identifier");
        }

        Identifier columnName; // stores name of each column
        ColumnAttribute columnAttribute = ColumnAttribute(ColumnAttribute::DataType::INT); // data type of each column: use int as a placeholder since I need an argument for the constructor 
        ColumnNames newTableColNames; // list of all column names for the new table
        ColumnAttributes newTableColAttributes; // list of all column attributes for the new table

        // check if the column names and data types are acceptable
        for(ColumnDefinition* column : *(statement->columns)){
            column_definition(column, columnName, columnAttribute); // get name and data type

            if(!is_acceptable_identifier(columnName)){
                cout << "Error: Column name " << columnName << " is not an acceptable identifier" << endl << endl;
                return new QueryResult("Error: Column name " + columnName + " not an acceptable identifier");
            }
            if(columnAttribute.get_data_type() != ColumnAttribute::TEXT &&
                columnAttribute.get_data_type() != ColumnAttribute::INT &&
                columnAttribute.get_data_type() != ColumnAttribute::BOOLEAN){
                cout << "Error: Invalid data type for column " << columnName << endl << endl;
                return new QueryResult("Error: Invalid data type for column " + columnName);
            }

            // if the column is valid, add the column name and attribute to the vectors
            newTableColAttributes.push_back(columnAttribute);
            newTableColNames.push_back(columnName);
        }
  
        // add the table name to tables
        ValueDict* tableRow = new ValueDict(); // row to add to _tables
        (*tableRow)["table_name"] = Value(statement->tableName);

        tables->insert(tableRow);

        // add the column names and types to Columns
        ValueDict* columnsRow = new ValueDict(); // row to add to _columns
        (*columnsRow)["table_name"] = Value(statement->tableName);

        // for each column, add a row to the Columns table
        for(int i=0; i < newTableColNames.size(); i++){
            (*columnsRow)["column_name"] = Value(newTableColNames[i]);

            if(newTableColAttributes[i].get_data_type() == ColumnAttribute::DataType::INT)
                (*columnsRow)["data_type"] = Value("INT");
            else if(newTableColAttributes[i].get_data_type() == ColumnAttribute::DataType::TEXT)
                (*columnsRow)["data_type"] = Value("TEXT");
            else if(newTableColAttributes[i].get_data_type() == ColumnAttribute::DataType::BOOLEAN)
                (*columnsRow)["data_type"] = Value("BOOLEAN");
                
            columns->insert(columnsRow);
        }

        // since there are no rows yet in the new table, use an empty ValueDicts for the QueryResult
        return new QueryResult(&newTableColNames, &newTableColAttributes, new ValueDicts(), SUCCESS_MESSAGE);
    }

    return new QueryResult("Invalid create statement type: must be create table or create index");
}

QueryResult *SQLExec::drop(const DropStatement *statement) {
    if(statement->type == DropStatement::EntityType::kIndex){
        return drop_index(statement);
    }else if(statement->type == DropStatement::EntityType::kTable){
        // drop all the indices on this table
        ValueDict where; // where condition: table_name == the name of the table in the drop statement
        where["table_name"] = Value(statement->name);  
        Handles* indexHandles = indices->select(&where); // get the handle to the row in _tables.
        ValueDict* indicesRow; // stores a row in _indices

        if(!indexHandles->empty()){ // if there are indices, drop them
            DropStatement* dropStmt = new DropStatement(DropStatement::EntityType::kIndex); // a drop statement for each index being dropped
            for(Handle indexHandle : *indexHandles){
                // get the name of each index, then drop it
                indicesRow = indices->project(indexHandle);
                dropStmt->indexName = (char*)((*indicesRow)["index_name"].s).c_str();
                delete indicesRow;
                drop_index(dropStmt);
            }
        }

        // drop the row from _tables        
        Handles* tablesHandles = tables->select(&where); // get the handle to the row in _tables.

        if(tablesHandles->empty()) // if there are no such rows, the table hasn't been created
            return new QueryResult("Error: the table being dropped doesn't exist");
        
        tables->del(tablesHandles->at(0)); // there should be only one handle returned since _tables rows are unique
        delete tablesHandles;

        // drop the rows from _columns that correspond to each column in the table
        Handles* columnsHandles = columns->select(&where); // again, need the rows where table_name is equal to the table

        // there can be more than one such row in _columns, so drop them all
        for(Handle columnsHandle : *columnsHandles)
            columns->del(columnsHandle);
        
        delete columnsHandles;
        columnsHandles = nullptr;

        return SQLExec().getSuccessfulQueryResult(Tables::TABLE_NAME);
    }

    return new QueryResult("Invalid drop statement type: must be drop table or drop index");
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
            rows->push_back(*row);
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
        rows->push_back(*row);
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
        rows->push_back(*row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

// done but need to test
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    if(statement->type != DropStatement::EntityType::kIndex){
        cout << "Error: attempted to call drop_index() on a non-index entity" << endl;
        return new QueryResult("Error: attempted to call drop_index() on a non-index entity");
    }

    // Call get_index to get a reference to the index and then call drop() on it. statement->name is the table name
    indices->get_index(statement->name, statement->indexName).drop();

    // find the rows in _indices for this index
    ValueDict* where; // where condition: index_name = the index being dropped
    (*where)["index_name"] = Value(statement->indexName);
    Handles* indexRowHandles = indices->select(where);

    delete where;
    where = nullptr;

    // delete each row from _indices for this index
    for(Handle handle : *indexRowHandles)
        indices->del(handle);

    delete indexRowHandles;
    indexRowHandles = nullptr;

    return SQLExec().getSuccessfulQueryResult(Indices::TABLE_NAME);
}
