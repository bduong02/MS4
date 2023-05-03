//Filename: heap_storage.cpp
//Author: Ishan Parikh, Bryan D.
//Purpose: implementation of SlottedPage, HeapFile, and HeapTable class


#include "heap_storage.h"
#include "db_cxx.h"
#include "storage_engine.h" 
#include <stdlib.h>
#include <algorithm>
#include <cstring>
#include <cmath>
#include <string>

//macros
#define TOMBSTONE 0

using namespace std;

//Slotted page----------------------------------------
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
  //init new block if specified new
  if(is_new) {
    this->num_records = 0;
    this->end_free = DbBlock::BLOCK_SZ -1;
    put_header();
  } else {
    //get the number of records and free memory pointer otherwise
    get_header(this->num_records, this->end_free);
  }
}

RecordID SlottedPage::add(const Dbt *data) {
  //don't add anything if there isn't room : (
  if(!has_room(data->get_size()))
    throw DbBlockNoRoomError("Room does not exist for new record");
  
  //increment record count and determine size of data block to put
  u_int16_t id = ++this->num_records;
  u_int16_t size = (u_int16_t) data->get_size();

  //decrement end_free to indicate a block is added stack-like from end
  this->end_free -= size;
  u_int16_t loc = this->end_free + 1;

  //update entry count and end of space pointer in slotted page as well
  put_header();

  //add new record entry to slotted page and write to slotted page 
  put_header(id, size, loc);
  memcpy(this->address(loc), data->get_data(), size);
  return id;
}

Dbt* SlottedPage::get(RecordID record_id) {
  //get the rcord entry header
  u_int16_t recordSize = 0, recordLocation = 0;
  get_header(recordSize, recordLocation, record_id);

  //0 indicates "tombstone"/no record
  if(recordLocation == TOMBSTONE) return nullptr;

  //return a new record (THIS IS HEAP MEMORY, must be deleted)
  return new Dbt(this->address(recordLocation), recordSize);
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {
  //get size and location of old record, and size of new data
  u_int16_t size = 0, location = 0, newSize = data.get_size();
  this->get_header(size, location, record_id);

  u_int16_t netSize = newSize - size, newLocation = location - netSize;

  if(newSize >= size) {
    //get net size and account for lack of room
    if(!this->has_room(netSize))
      throw DbBlockNoRoomError("No room to update indicated record size");
    
    //slide recordds left and copy new data
    this->slide(location, newLocation);
    memcpy(this->address(newLocation), data.get_data(), newSize);
  } else {
    //overwrite record and slide record data right
    memcpy(this->address(location), data.get_data(), newSize);
    this->slide(location + newSize, location + size);
  }

  //update header
  this->put_header(record_id, newSize, newLocation);
}

void SlottedPage::del(RecordID record_id) {
  //keeps record id same for everyone, just marks it as a tombstone and compacts data
  u_int16_t size = 0, location = 0;

  //get header information for shifting
  this->get_header(size, location, record_id);

  //clear header and data
  this->put_header(record_id, TOMBSTONE, TOMBSTONE);
  this->slide(location, location + size);
}

RecordIDs* SlottedPage::ids(void) {
  RecordIDs* idList = new RecordIDs;

  //add all ids to list and return
  for(u_int16_t id = 1; id < this->num_records + 1; id++) {
    u_int16_t size = 0, loc = 0;
    this->get_header(size, loc, id);

    //if active push it into the list! :DDDDDD
    if(loc != TOMBSTONE)
      idList->push_back(id);
  }
    
  return idList;
}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
  size = this->get_n(4 * id);
  loc = this->get_n(4 * id + 2);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }

    //header structure (in order): 
    //|2 bytes- data offset (size), 2 bytes- data location in block(loc)| 
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

// Get 2-byte integer at given offset in block.
u_int16_t SlottedPage::get_n(u_int16_t offset) {
    return *(u_int16_t*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u_int16_t offset, u_int16_t n) {
    *(u_int16_t*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u_int16_t offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

bool SlottedPage::has_room(u_int16_t size) {
  //size must be less than the room left
  //this->end_free gets the end of free space
  //(this->num_records + 2) * 4) represents the memory occupied by the
  //headers, including the initial header (each header occupies 4 bytes)
  //in the slotted page block
  return size <= (this->end_free - (this->num_records + 1) * 4); 
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
  int shift = start - end;
  u_int16_t size = 0, location = 0;

  if(shift == 0) return;
  
  memmove(this->address((this->end_free+1) + shift), 
         this->address((this->end_free+1)), 
         start - (this->end_free + 1));

  RecordIDs* idList = this->ids();

  //shift header information as well
  for(auto id : (*idList)) {
    this->get_header(size, location, id);

    //update location and put back into header
    if(location <= start) {
      location += shift;
      this->put_header(id, size, location);
    }
  }

  //shift end pointer and general header information
  this->end_free += shift;
  this->put_header();
  delete idList;  
}

//HeapFile-----------------------------------------------
void HeapFile::create() {
  //open db with CREATE and EXCL flags to force intialization
  db_open(DB_CREATE | DB_EXCL);

  //create a page to make it all exist
  SlottedPage* dummyPage = this->get_new();
  delete dummyPage;
}

void HeapFile::drop(void) {
  this->close();

  //reinit db
  Db reinit(_DB_ENV, 0);
  reinit.remove(this->dbfilename.c_str(), nullptr, 0);
}

void HeapFile::open(void) {
  this->db_open();
}

void HeapFile::close(void) {
  this->db.close(0);
  this->closed = true;
}

SlottedPage* HeapFile::get_new(void) {
  //create an empty char of block size as a data
  char dummyData[DbBlock::BLOCK_SZ];
  memset(dummyData, 0, sizeof(dummyData));
  Dbt blockToSlot(&dummyData, sizeof(dummyData));

  //get block id and create a key
  this->last++;
  BlockID blockID = ((this->last) + 0);
  Dbt key(&blockID, sizeof(blockID));

  //create page, create key-pair association in db and then return
  this->db.put(nullptr, &key, &blockToSlot, 0);
  return new SlottedPage(blockToSlot, blockID, true);
}

SlottedPage* HeapFile::get(BlockID blockID) {
  Dbt key(&blockID, sizeof(blockID)), data;

  //assign data from db using defined key and return 
  //in slotted page format
  this->db.get(nullptr, &key, &data, 0);
  return new SlottedPage(data, blockID, false);
}

void HeapFile::put(DbBlock* block) {
  //set key db object and use it as a key when 
  //putting it and the block data in the db
  BlockID id = block->get_block_id();
  Dbt key(&(id), sizeof(id));
  this->db.put(nullptr, &key, (Dbt*)(block->get_data()), 0);
}


BlockIDs* HeapFile::block_ids() {
  BlockIDs* idList = new BlockIDs();

  for(BlockID blockId = 1; blockId < this->last + 1; blockId++)
    idList->push_back(blockId);

  return idList;
}


void HeapFile::db_open(uint flags) {
  //handle closed state
  if(!this->closed) return;

  //set block size and open db
  this->db.set_re_len(DbBlock::BLOCK_SZ);
  this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0644);

  //intialize db statisitcs and set last block
  if(flags == 0) {
    DB_BTREE_STAT stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    this->last = stat.bt_ndata;
  } else this-> last = 0;

  //set closed to false to indicate db is open
  this->closed = false;
}
//-------------------------------------------------------

//HeapTable--------------------------------
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : 
DbRelation(table_name, column_names, column_attributes), file(table_name) {
  //quick intialize with super constructor and init file as such
  //to avoid using the assignment overload operator and causing issues
}
                     
void HeapTable::create() {
  this->file.create();
}

void HeapTable::create_if_not_exists() {
  //try to open, if exception (indicating lack of db relation), 
  //then initialize new heap table
  try {
    this->open();
  } catch(DbException& error){
    this->create();
  }
}

void HeapTable::drop() {
  this->file.drop();
}

void HeapTable::open() {
  this->file.open();
}

void HeapTable::close() {
  this->file.close();
}

Handle HeapTable::insert(const ValueDict *row) {
  //open relation define a row, insert it (deleting after) and return
  //h for future operations
  cout<<"in HeapTable::insert"<<endl;
  this->open();
  cout <<"opened"<<endl;
  ValueDict* fullRow = this->validate(row);
  Handle h = this->append(fullRow);
  delete fullRow;
  return h;
}
                     
// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
  char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
  uint offset = 0;
  uint col_num = 0;
  for (auto const& column_name: this->column_names) {
    ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    Value value = column->second;
    if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
      *(int32_t*) (bytes + offset) = value.n;
      offset += sizeof(int32_t);
    } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
      uint size = value.s.length();
      *(u_int16_t*) (bytes + offset) = size;
      offset += sizeof(u_int16_t);
      memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
      offset += size;
    } else {
      throw DbRelationError("Only know how to marshal INT and TEXT");
    }
  }
  char *right_size_bytes = new char[offset];
  memcpy(right_size_bytes, bytes, offset);
  delete[] bytes;
  Dbt *data = new Dbt(right_size_bytes, offset);
  return data;
}  

Handle HeapTable::append(const ValueDict *row) {
  //marshall row and attempt to insert in current block
  Dbt* data = marshal(row);
  SlottedPage* block = this->file.get(this->file.get_last_block_id());
  RecordID id;

  try {
    id = block->add(data);
  } catch (DbBlockNoRoomError &e) {
    //get a new block and add the record to it
    block = this->file.get_new();
    id = block->add(data);
  }

  //add block to file, and delete used memory
  this->file.put(block);
  delete [] (char *) data->get_data();
  delete data;
  delete block;
  return Handle(this->file.get_last_block_id(), id);
}

ValueDict* HeapTable::validate(const ValueDict *row) {
  ValueDict* completeRow = new ValueDict();
  for (auto &column : this->column_names) {

      Value val;
      ValueDict::const_iterator columnIterator = row->find(column);

      //if iterator reaches end, raise unimplemnted feature exception
      if (columnIterator == row->end())
        throw DbRelationError("Extra details not implemented yet");

      //if not assign the value appropriately to be appended
      else
          val = columnIterator->second;
      (*completeRow)[column] = val;
  }

  return completeRow;
}

//-----------------------------------------
