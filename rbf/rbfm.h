#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <set>
#include <climits>

#include "../rbf/pfm.h"

using namespace std;

// Record ID
typedef struct
{
  unsigned pageNum;    // page number
  unsigned slotNum;    // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar, TypeDefault = -1 } AttrType;

typedef unsigned AttrLength;
typedef unsigned AttrVersion;
typedef unsigned AttrPosition;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

struct AttributeWithVersion : Attribute {
	AttrVersion version;
	AttrPosition position;
};

struct AttributeWithData : AttributeWithVersion {
	int intData;
	float realData;
	string varcharData;
	bool isNull;
};

struct ridCompare {
	bool operator() (const RID &lhs, const RID &rhs) const {
		if (lhs.pageNum == rhs.pageNum) {
			return lhs.slotNum < rhs.pageNum;
		}
		return lhs.pageNum < rhs.pageNum;
	}
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// = 
           LT_OP,      // <
           LE_OP,      // <=
           GT_OP,      // >
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;

typedef unsigned short FreeSpaceOffset;
typedef unsigned short TotalSlots;
typedef unsigned short SlotOffset;
typedef unsigned short SlotLength;
typedef unsigned short RecordDirectoryLength;
typedef unsigned short RecordDirectorySlot;
typedef unsigned short RecordSize;
typedef unsigned DirectorySize;
typedef bool TombstoneBool;


/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
	RID previousRid;
	FileHandle fileHandle;
	CompOp compOp;
	const void *value;
	int valueSize;
	AttrType valueType;
	AttrVersion version;
	string conditionAttribute;
	vector<AttributeWithVersion> recordDescriptor;
	vector<string> attributeNames;
	vector<Attribute> requestedAttributes;
	set<RID, ridCompare> tombstoneRids;

  RBFM_ScanIterator();
  ~RBFM_ScanIterator();

  // Never keep the results in the memory. When getNextRecord() is called, 
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data);
  RC close();

  bool compareValues(const void *valueToCompare);
};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first, 
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
  // This method will be mainly used for debugging/testing. 
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<AttributeWithVersion> &recordDescriptor,
	  const AttrVersion version,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

public:
  FreeSpaceOffset getFreeSpaceOffset(const void *data);
  TotalSlots getTotalSlots(const void *data);
  SlotOffset getSlotOffset(const void* data, unsigned slotNumber);
  SlotLength getSlotLength(const void *data, unsigned slotNumber);
  RC readFormattedRecord(FileHandle &fileHandle, const RID &rid, void **formattedRecord);
  RC createDataRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const vector<Attribute> &attributes, const RID &rid, void *data, AttrVersion version);
  RC createDataRecord(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const vector<Attribute> &attributes, const RID &rid, void *data, AttrVersion version, void *page);
  RC getTableId(unsigned &tid, FileHandle &fileHandle);
  RC setTableId(const unsigned &tid, FileHandle &fileHandle);
  RC incrementTableId(FileHandle &fileHandle);
  RC readAttribute(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const RID &rid, const string &attributeName, void *data, RID &actualRid, AttrVersion version);
  RC readAttribute(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const RID &rid, const string &attributeName, void *data, RID &actualRid, AttrVersion version, void *page);
  RC insertRecord(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const void *data, RID &rid, AttrVersion version);
  RC readRecord(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const RID &rid, void *data, AttrVersion version);
  RC updateRecord(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const void *data, const RID &rid, AttrVersion version);
  void createAttributesWithVersion(const vector<Attribute> &attributes, vector<AttributeWithVersion> &attributesWithVersion, AttrVersion version);
  RC getRecordVersionWithLength(FileHandle &fileHandle, const RID &rid, AttrVersion &version, RecordDirectoryLength &length);
  RC getRecordVersionWithLength(FileHandle &fileHandle, const RID &rid, AttrVersion &version, RecordDirectoryLength &length, void *page);
  RC printRecord(const vector<AttributeWithVersion> &recordDescriptor, const void *data);
  RC readRecord(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const RID &rid, void *data, AttrVersion version, void *page);
  RC readFormattedRecord(FileHandle &fileHandle, const RID &rid, void **formattedRecord, void *page);

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
  //Added functions
  RC createSlotDirectory(void *data);
  RC createRecordDirectory(const void *data, const vector<AttributeWithVersion> &recordDescriptor, void *formattedRecord, AttrVersion version);
  RC convertFormattedRecordToData(const void *formattedRecord, const vector<AttributeWithVersion> &recordDescriptor, void *data, AttrVersion version);
  RC setFreeSpaceOffset(void *data, FreeSpaceOffset &value);
  RC increaseFreeSpaceOffset(void *data, FreeSpaceOffset &value);
  RC decreaseFreeSpaceOffset(void *data, FreeSpaceOffset &value);
  RC setTotalSlots(void *data, TotalSlots &value);
  RC incrementTotalSlots(void *data);
  RC setSlotOffset(void *data, unsigned slotNumber, SlotOffset &value);
  RC setSlotLength(void *data, unsigned slotNumber, SlotLength &value);
  //Calculates size of record
  RecordSize getRecordSize(const vector<AttributeWithVersion> &recordDescriptor, const void *data);
  RecordSize getFormattedRecordSize(const vector<AttributeWithVersion> &recordDescriptor, const void *data);
  RecordSize getFormattedRecordSizeFromData(const vector<AttributeWithVersion> &recordDescriptor, const void *data);
};

void setNullBit(unsigned char &byte, int position);
//Get bit from byte
bool getBit(const unsigned char byte, int position);
bool comparePositions(AttributeWithVersion attr1, AttributeWithVersion attr2);
#endif
