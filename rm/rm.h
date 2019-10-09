
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

typedef enum { TableTypeSystem = 0, TableTypeUser } TableType;


// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
	FileHandle fileHandle;
	RBFM_ScanIterator rbfm_ScanIterator;
  RM_ScanIterator();
  ~RM_ScanIterator();

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);


protected:
  RelationManager();
  ~RelationManager();

private:
  RecordBasedFileManager *_rbfm;
  RC createTable(const string &tableName, const vector<Attribute> &attrs, const TableType tableType);
  RC deleteTable(const string &tableName, const TableType tableType);
  RC insertTuple(const string &tableName, const void *data, RID &rid, const TableType tableType);
  RC insertTuple(const string &tableName, const vector<Attribute> &recordDescriptor, const void *data, RID &rid, const TableType tableType);
  RC readTuple(const string &tableName, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  RC deleteTuple(const string &tableName, const RID &rid, const TableType tableType);
  RC updateTuple(const string &tableName, const void *data, const RID &rid, const TableType tableType);
  RC dropAttribute(const string &tableName, const string &attributeName, const TableType tableType);
  RC addAttribute(const string &tableName, const Attribute &attr, const TableType tableType);
  void createTablesAttributes(vector<Attribute> &attributes);
  void createColumnsAttributes(vector<Attribute> &attributes);
  bool isSystemTable(const string &tableName);
  bool checkSystemTablePermission(const string &tableName, const TableType);
  RC getAllAttributes(const string &tableName, vector<AttributeWithVersion> &attrs, AttrVersion &version);
  RC getAttributes(const string &tableName, vector<AttributeWithVersion> &attrs, AttrVersion &version, RID &tableRid, int &tableId);
  RC getAllAttributes(const string &tableName, vector<AttributeWithVersion> &attrs, AttrVersion &version, RID &tableRid, int &tableId);
  void createTablesAttributes(vector<AttributeWithVersion> &attributes);
  void createColumnsAttributes(vector<AttributeWithVersion> &attributes);
  RC updateRecordVersion(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const RID &rid, const string &tableName, const RecordDirectoryLength &recordDirectoryLength, const AttrVersion &recordVersion, const AttrVersion &tableVersion);
};

#endif
