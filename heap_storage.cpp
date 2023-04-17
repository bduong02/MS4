//Filename: heap_storage.cpp
//Author: Ishan Parikh, Bryan D.
//Purpose 

#include "heap_storage.h"
#include "db_cxx.h"
#include "storage_engine.h" 
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
    memcpy(this->address(newLocation), data.get_data(), size + netSize);
  } else {
    //overwrite record and slide record data right
    memcpy(this->address(newLocation), data.get_data(), newSize);
    this->slide(location, newLocation);
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
  for(u_int16_t id = 1; id <= this->num_records; id++) {
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
  //don't need to shift if no net difference
  

  //shift data left
  int shift = start - end;
  u_int16_t size = 0, location = 0;
  if(start == end) return;
  else if(start > end) {
    memcpy(this->address((this->end_free+1)-shift), 
           this->address((this->end_free+1)), 
           start - (this->end_free + 1));
  } else {
    char* destination  = (char*) (this->address(start));
    const char *source = (char*)((char*)(this->block.get_data()) + (start - shift));

    while (shift >= 0) {
      *destination-- = *source--;
      shift--;
    }
  }

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
