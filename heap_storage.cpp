//Filename: heap_storage.cpp
//Author: Ishan Parikh, Bryan D.
//Purpose 

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
    get_header(this->num_records, this->end_free);
  }
}

RecordID SlottedPage::add(const Dbt *data) {
  if(!has_room(data->get_size()))
    throw DbBlockNoRoomError("Room does not exist for new record");
  
  //increment record count and determine size of data block to put
  u_int16_t id = ++this->num_records;
  u_int16_t size = (u_int16_t) data->get_size();

  //decrement end_free to indicate a block is added stack-like from end
  this->end_free -= size;
  u_int16_t loc = this->end_free + 1;

  //update entry count and end of space pointer
  put_header();

  //add actual record entry and copy to 
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
  return size <= (this->end_free - (this->num_records + 2) * 4); 
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

//-------------------------------------------------------


//HeapFile-----------------------------------------------
void HeapFile::create() {
  //open db with CREATE and EXCL flags to force intialization
  db_open(DB_CREATE | DB_EXCL);

  //create a page to make it all exist
  SlottedPage* dummyPage = get_new();
  delete dummyPage;
}

void HeapFile::drop(void) {
  this->close();
  this->db.remove(this->dbfilename.c_str(), nullptr, 0);
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

  //get data from db using defined key
  this->db.get(nullptr, &key, &data, 0);
  return new SlottedPage(data, blockID, false);
}

void HeapFile::put(DbBlock* block) {
  BlockID blockId = block->get_block_id();
  Dbt key(&blockId, sizeof(blockId));
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

  //intialize db statisitcs
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



//-----------------------------------------
