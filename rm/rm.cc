
#include "rm.h"
#include <cstring>
#include <cmath>
#include <iostream>
#include <algorithm>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	_rbfm = RecordBasedFileManager::instance();
	_im = IndexManager::instance();
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	vector<Attribute> tableAttributes;
	vector<Attribute> columnAttributes;
	createTablesAttributes(tableAttributes);
	createColumnsAttributes(columnAttributes);
	if (_rbfm->createFile("Tables")){
		return -1;
	}
	if (_rbfm->createFile("Columns")){
		return -1;
	}
	if (createTable("Tables", tableAttributes, TableTypeSystem) == -1){
		return -1;
	}

	if (createTable("Columns", columnAttributes, TableTypeSystem) == -1){
		return -1;
	}

    return 0;
}

RC RelationManager::deleteCatalog()
{
	if (_rbfm->destroyFile("Tables")){
		return -1;
	}
	if (_rbfm->destroyFile("Columns")){
		return -1;
	}
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	return createTable(tableName, attrs, TableTypeUser);
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs, const TableType tableType){
	if (!checkSystemTablePermission(tableName, tableType)){
		return -1;
	}
	if (tableType == TableTypeUser){
		if (_rbfm->createFile(tableName) == -1){
			return -1;
		}
	}

	vector<Attribute> tableAttributes;
	vector<Attribute> columnAttributes;
	createTablesAttributes(tableAttributes);
	createColumnsAttributes(columnAttributes);
	string fileName = tableName;

	RID rid;

	FileHandle fileHandle;
	_rbfm->openFile("Tables", fileHandle);
	unsigned currentTableId;
	_rbfm->getTableId(currentTableId, fileHandle);
    _rbfm->incrementTableId(fileHandle);
    _rbfm->closeFile(fileHandle);

	unsigned newTableId = currentTableId + 1;

	int tableNameLength = tableName.length();
	int fileNameLength = fileName.length();
	AttrVersion version = 1;
	AttrIndex index = 0;
	unsigned tableDataLength = sizeof(char) + sizeof(int) + sizeof(int) + tableNameLength + sizeof(int) + fileNameLength + sizeof(int) + sizeof(AttrVersion);
	void *tableData = malloc(tableDataLength);
    memset(tableData, 0, sizeof(char));
    memcpy(tableData + sizeof(char), &newTableId, sizeof(int));
    memcpy(tableData + sizeof(char) + sizeof(int), &tableNameLength, sizeof(int));
    memcpy(tableData + sizeof(char) + sizeof(int) + sizeof(int), tableName.c_str(), tableNameLength);
    memcpy(tableData + sizeof(char) + sizeof(int) + sizeof(int) + tableNameLength, &fileNameLength, sizeof(int));
    memcpy(tableData + sizeof(char) + sizeof(int) + sizeof(int) + tableNameLength + sizeof(int), fileName.c_str(), fileNameLength);
    memcpy(tableData + sizeof(char) + sizeof(int) + sizeof(int) + tableNameLength + sizeof(int) + fileNameLength, &tableType, sizeof(tableType));
    memcpy(tableData + sizeof(char) + sizeof(int) + sizeof(int) + tableNameLength + sizeof(int) + fileNameLength + sizeof(int), &version, sizeof(AttrVersion));

    if (insertTuple("Tables", tableAttributes, tableData, rid, TableTypeSystem) == -1){
    	free(tableData);
    	return -1;
    }
    free(tableData);

    for (unsigned i = 0; i < attrs.size(); ++i){
    	Attribute attribute = attrs.at(i);
    	unsigned columnPosition = i + 1;
    	void *columnData;
    	unsigned recordSize = sizeof(char);
    	recordSize += sizeof(int);
    	int attributeNameLength = attribute.name.length();
    	recordSize += sizeof(int);
    	recordSize += attributeNameLength;
    	recordSize += sizeof(AttrType);
    	recordSize += sizeof(AttrLength);
    	recordSize += sizeof(int);
    	recordSize += sizeof(AttrVersion);
    	recordSize += sizeof(AttrIndex);
    	columnData = malloc(recordSize);
    	memset(columnData, 0, sizeof(char));
    	memcpy(columnData + sizeof(char), &newTableId, sizeof(int));
    	memcpy(columnData + sizeof(char) + sizeof(int), &attributeNameLength, sizeof(int));
    	memcpy(columnData + sizeof(char) + sizeof(int) + sizeof(int), attribute.name.c_str(), attributeNameLength);
    	memcpy(columnData + sizeof(char) + sizeof(int) + sizeof(int) + attributeNameLength, &(attribute.type), sizeof(AttrType));
    	memcpy(columnData + sizeof(char) + sizeof(int) + sizeof(int) + attributeNameLength + sizeof(AttrType), &(attribute.length), sizeof(AttrLength));
    	memcpy(columnData + sizeof(char) + sizeof(int) + sizeof(int) + attributeNameLength + sizeof(AttrType) + sizeof(AttrLength), &columnPosition, sizeof(int));
    	memcpy(columnData + sizeof(char) + sizeof(int) + sizeof(int) + attributeNameLength + sizeof(AttrType) + sizeof(AttrLength) + sizeof(int), &version, sizeof(AttrVersion));
    	memcpy(columnData + sizeof(char) + sizeof(int) + sizeof(int) + attributeNameLength + sizeof(AttrType) + sizeof(AttrLength) + sizeof(int) + sizeof(AttrVersion), &index, sizeof(AttrIndex));
    	if(insertTuple("Columns", columnAttributes, columnData, rid, TableTypeSystem) == -1){
    		free(columnData);
    		return -1;
    	}
    	free(columnData);
    }
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	return deleteTable(tableName, TableTypeUser);
}

RC RelationManager::deleteTable(const string &tableName, const TableType tableType){
	if (!checkSystemTablePermission(tableName, tableType)){
		return -1;
	}

	if (tableType == TableTypeUser){
		if (_rbfm->destroyFile(tableName) == -1){
			return -1;
		}
	}
	RM_ScanIterator rmsi;
	vector<string> tableIdAttribute;
	tableIdAttribute.push_back("table-id");
	void *tableIdData = malloc(sizeof(char) + sizeof(unsigned));
	int tableNameLength = tableName.length();
	void *conditionString = malloc(sizeof(int) + tableNameLength);
	memcpy(conditionString, &tableNameLength, sizeof(int));
	memcpy(conditionString + sizeof(int), tableName.c_str(), tableNameLength);
	scan("Tables", "table-name", EQ_OP, conditionString, tableIdAttribute, rmsi);
	RID rid;
	if (rmsi.getNextTuple(rid, tableIdData) == RM_EOF){
		free(tableIdData);
		free(conditionString);
		return -1;
	}
	if (deleteTuple("Tables", rid, TableTypeSystem) == -1){
		free(tableIdData);
		free(conditionString);
		return -1;
	}

	int tableId;
	memcpy(&tableId, tableIdData + sizeof(char), sizeof(unsigned));
	free(tableIdData);
	free(conditionString);
	rmsi.close();
	vector<string> columnAttribute;
	scan("Columns", "table-id", EQ_OP, &tableId, columnAttribute, rmsi);
	while(rmsi.getNextTuple(rid, NULL) != RM_EOF){
		if (deleteTuple("Columns", rid, TableTypeSystem) == -1){
			return -1;
		}
	}
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    if(tableName == "Tables"){
        createTablesAttributes(attrs);
        return 0;
    }

    if(tableName == "Columns"){
        createColumnsAttributes(attrs);
        return 0;
    }
	RM_ScanIterator rmsi;
	vector<string> tableAttributes;
	tableAttributes.push_back("table-id");
	tableAttributes.push_back("table-version");
	void *tableData = malloc(sizeof(char) + sizeof(unsigned) + sizeof(AttrVersion));
	int tableNameLength = tableName.length();
	void *conditionString = malloc(sizeof(int) + tableNameLength);
	memcpy(conditionString, &tableNameLength, sizeof(int));
	memcpy(conditionString + sizeof(int), tableName.c_str(), tableNameLength);
	scan("Tables", "table-name", EQ_OP, conditionString, tableAttributes, rmsi);
	RID rid;
	if (rmsi.getNextTuple(rid, tableData) == RM_EOF){
		rmsi.close();
		free(conditionString);
		free(tableData);
		return -1;
	}
	int tableId;
	memcpy(&tableId, tableData + sizeof(char), sizeof(unsigned));
	AttrVersion tableVersion;
	memcpy(&tableVersion, tableData + sizeof(char) + sizeof(unsigned), sizeof(AttrVersion));
	rmsi.close();
	RM_ScanIterator rmsiColumns;
	vector<string> columnAttribute;
	columnAttribute.push_back("column-name");
	columnAttribute.push_back("column-type");
	columnAttribute.push_back("column-length");
	columnAttribute.push_back("column-position");
	columnAttribute.push_back("column-version");
	columnAttribute.push_back("column-index");
	unsigned columnDataSize = sizeof(char) + sizeof(int) + sizeof(int) + 50 + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion) + sizeof(AttrIndex);
	void *columnData = malloc(columnDataSize);
	scan("Columns", "table-id", EQ_OP, &tableId, columnAttribute, rmsi);
	set<int> columnPositions;
//	vector<Attribute> tempAttributes;
	while(rmsi.getNextTuple(rid, columnData) != RM_EOF){
		int attributeInfo[3];
		int columnNameLength;
		memcpy(&columnNameLength, columnData + sizeof(char), sizeof(int));
		char columnName[columnNameLength + 1] = {0};
		AttrVersion columnVersion;
		memcpy(&columnName, columnData + sizeof(char) + sizeof(int), columnNameLength);
		memcpy(&attributeInfo, columnData + sizeof(char) + sizeof(int) + columnNameLength, sizeof(int) + sizeof(int) + sizeof(int));
		memcpy(&columnVersion, columnData + sizeof(char) + sizeof(int) + columnNameLength + sizeof(int) + sizeof(int) + sizeof(int), sizeof(AttrVersion));
		if(columnVersion != tableVersion){
			continue;
		}
//		if(tempAttributes.size() < attributeInfo[2]){
//			tempAttributes.resize(attributeInfo[2]);
//		}
//		columnPositions.insert(attributeInfo[2] - 1);
		Attribute attribute;
		attribute.name = string(columnName);
		attribute.type = (AttrType) attributeInfo[0];
		attribute.length = attributeInfo[1];
//		attribute.position = attributeInfo[2];
//		attribute.version = columnVersion;
//		tempAttributes.at(attributeInfo[2] - 1) = attribute;
		attrs.push_back(attribute);
		memset(columnData, 0, columnDataSize);
	}
//	for (int i = 0; i < tempAttributes.size(); ++i){
//		if (columnPositions.count(i) == 1){
//			attrs.push_back(tempAttributes.at(i));
//		}
//	}
	rmsi.close();
	free(tableData);
	free(conditionString);
	free(columnData);
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<AttributeWithVersion> &attrs, AttrVersion &version, RID &tableRid, int &tableId){
    if(tableName == "Tables"){
        createTablesAttributes(attrs);
        return 0;
    }

    if(tableName == "Columns"){
        createColumnsAttributes(attrs);
        return 0;
    }
	RM_ScanIterator rmsi;
	vector<string> tableAttributes;
	tableAttributes.push_back("table-id");
	tableAttributes.push_back("table-version");
	void *tableData = malloc(sizeof(char) + sizeof(unsigned) + sizeof(AttrVersion));
	int tableNameLength = tableName.length();
	void *conditionString = malloc(sizeof(int) + tableNameLength);
	memcpy(conditionString, &tableNameLength, sizeof(int));
	memcpy(conditionString + sizeof(int), tableName.c_str(), tableNameLength);
	scan("Tables", "table-name", EQ_OP, conditionString, tableAttributes, rmsi);
	RID rid;
	if (rmsi.getNextTuple(rid, tableData) == RM_EOF){
		rmsi.close();
		free(conditionString);
		free(tableData);
		return -1;
	}
	tableRid = rid;
	memcpy(&tableId, tableData + sizeof(char), sizeof(unsigned));
	AttrVersion tableVersion;
	memcpy(&tableVersion, tableData + sizeof(char) + sizeof(unsigned), sizeof(AttrVersion));
	version = tableVersion;
	rmsi.close();
	RM_ScanIterator rmsiColumns;
	vector<string> columnAttribute;
	columnAttribute.push_back("column-name");
	columnAttribute.push_back("column-type");
	columnAttribute.push_back("column-length");
	columnAttribute.push_back("column-position");
	columnAttribute.push_back("column-version");
	columnAttribute.push_back("column-index");
	unsigned columnDataSize = sizeof(char) + sizeof(int) + sizeof(int) + 50 + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion) + sizeof(AttrIndex);
	void *columnData = malloc(columnDataSize);
	scan("Columns", "table-id", EQ_OP, &tableId, columnAttribute, rmsi);
	set<int> columnPositions;
	while(rmsi.getNextTuple(rid, columnData) != RM_EOF){
		int attributeInfo[3];
		int columnNameLength;
		memcpy(&columnNameLength, columnData + sizeof(char), sizeof(int));
		char columnName[columnNameLength + 1] = {0};
		AttrVersion columnVersion;
		memcpy(&columnName, columnData + sizeof(char) + sizeof(int), columnNameLength);
		memcpy(&attributeInfo, columnData + sizeof(char) + sizeof(int) + columnNameLength, sizeof(int) + sizeof(int) + sizeof(int));
		memcpy(&columnVersion, columnData + sizeof(char) + sizeof(int) + columnNameLength + sizeof(int) + sizeof(int) + sizeof(int), sizeof(AttrVersion));
		if(columnVersion != tableVersion){
			continue;
		}
		columnPositions.insert(attributeInfo[2] - 1);
		AttributeWithVersion attribute;
		attribute.name = string(columnName);
		attribute.type = (AttrType) attributeInfo[0];
		attribute.length = attributeInfo[1];
		attribute.position = attributeInfo[2];
		attribute.version = columnVersion;
		attrs.push_back(attribute);
		memset(columnData, 0, columnDataSize);

	}
	rmsi.close();
	free(tableData);
	free(conditionString);
	free(columnData);
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<AttributeWithIndex> &attrs)
{
    if(tableName == "Tables"){
        createTablesAttributes(attrs);
        return 0;
    }

    if(tableName == "Columns"){
        createColumnsAttributes(attrs);
        return 0;
    }
	RM_ScanIterator rmsi;
	vector<string> tableAttributes;
	tableAttributes.push_back("table-id");
	tableAttributes.push_back("table-version");
	void *tableData = malloc(sizeof(char) + sizeof(unsigned) + sizeof(AttrVersion));
	int tableNameLength = tableName.length();
	void *conditionString = malloc(sizeof(int) + tableNameLength);
	memcpy(conditionString, &tableNameLength, sizeof(int));
	memcpy(conditionString + sizeof(int), tableName.c_str(), tableNameLength);
	scan("Tables", "table-name", EQ_OP, conditionString, tableAttributes, rmsi);
	RID rid;
	if (rmsi.getNextTuple(rid, tableData) == RM_EOF){
		rmsi.close();
		free(conditionString);
		free(tableData);
		return -1;
	}
	int tableId;
	memcpy(&tableId, tableData + sizeof(char), sizeof(unsigned));
	AttrVersion tableVersion;
	memcpy(&tableVersion, tableData + sizeof(char) + sizeof(unsigned), sizeof(AttrVersion));
	rmsi.close();
	RM_ScanIterator rmsiColumns;
	vector<string> columnAttribute;
	columnAttribute.push_back("column-name");
	columnAttribute.push_back("column-type");
	columnAttribute.push_back("column-length");
	columnAttribute.push_back("column-position");
	columnAttribute.push_back("column-version");
	columnAttribute.push_back("column-index");
	unsigned columnDataSize = sizeof(char) + sizeof(int) + sizeof(int) + 50 + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion) + sizeof(AttrIndex);
	void *columnData = malloc(columnDataSize);
	scan("Columns", "table-id", EQ_OP, &tableId, columnAttribute, rmsi);
	set<int> columnPositions;
	while(rmsi.getNextTuple(rid, columnData) != RM_EOF){
		int attributeInfo[3];
		int columnNameLength;
		memcpy(&columnNameLength, columnData + sizeof(char), sizeof(int));
		char columnName[columnNameLength + 1] = {0};
		AttrVersion columnVersion;
		memcpy(&columnName, columnData + sizeof(char) + sizeof(int), columnNameLength);
		memcpy(&attributeInfo, columnData + sizeof(char) + sizeof(int) + columnNameLength, sizeof(int) + sizeof(int) + sizeof(int));
		memcpy(&columnVersion, columnData + sizeof(char) + sizeof(int) + columnNameLength + sizeof(int) + sizeof(int) + sizeof(int), sizeof(AttrVersion));
		if(columnVersion != tableVersion){
			continue;
		}
		AttrIndex columnIndex;
		memcpy(&columnIndex, columnData + sizeof(char) + sizeof(int) + columnNameLength + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion), sizeof(AttrIndex));
		AttributeWithIndex attribute;
		attribute.name = string(columnName);
		attribute.type = (AttrType) attributeInfo[0];
		attribute.length = attributeInfo[1];
		attribute.position = attributeInfo[2];
		attribute.version = columnVersion;
		attribute.index = columnIndex;
		attrs.push_back(attribute);
		memset(columnData, 0, columnDataSize);
	}
	rmsi.close();
	free(tableData);
	free(conditionString);
	free(columnData);
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<AttributeWithIndex> &attrs, AttrVersion &version, RID &tableRid, int &tableId)
{
    if(tableName == "Tables"){
        createTablesAttributes(attrs);
        return 0;
    }

    if(tableName == "Columns"){
        createColumnsAttributes(attrs);
        return 0;
    }
	RM_ScanIterator rmsi;
	vector<string> tableAttributes;
	tableAttributes.push_back("table-id");
	tableAttributes.push_back("table-version");
	void *tableData = malloc(sizeof(char) + sizeof(unsigned) + sizeof(AttrVersion));
	int tableNameLength = tableName.length();
	void *conditionString = malloc(sizeof(int) + tableNameLength);
	memcpy(conditionString, &tableNameLength, sizeof(int));
	memcpy(conditionString + sizeof(int), tableName.c_str(), tableNameLength);
	scan("Tables", "table-name", EQ_OP, conditionString, tableAttributes, rmsi);
	RID rid;
	if (rmsi.getNextTuple(rid, tableData) == RM_EOF){
		rmsi.close();
		free(conditionString);
		free(tableData);
		return -1;
	}
	tableRid = rid;
	memcpy(&tableId, tableData + sizeof(char), sizeof(unsigned));
	AttrVersion tableVersion;
	memcpy(&tableVersion, tableData + sizeof(char) + sizeof(unsigned), sizeof(AttrVersion));
	version = tableVersion;
	rmsi.close();
	RM_ScanIterator rmsiColumns;
	vector<string> columnAttribute;
	columnAttribute.push_back("column-name");
	columnAttribute.push_back("column-type");
	columnAttribute.push_back("column-length");
	columnAttribute.push_back("column-position");
	columnAttribute.push_back("column-version");
	columnAttribute.push_back("column-index");
	unsigned columnDataSize = sizeof(char) + sizeof(int) + sizeof(int) + 50 + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion) + sizeof(AttrIndex);
	void *columnData = malloc(columnDataSize);
	scan("Columns", "table-id", EQ_OP, &tableId, columnAttribute, rmsi);
	set<int> columnPositions;
	while(rmsi.getNextTuple(rid, columnData) != RM_EOF){
		int attributeInfo[3];
		int columnNameLength;
		memcpy(&columnNameLength, columnData + sizeof(char), sizeof(int));
		char columnName[columnNameLength + 1] = {0};
		AttrVersion columnVersion;
		memcpy(&columnName, columnData + sizeof(char) + sizeof(int), columnNameLength);
		memcpy(&attributeInfo, columnData + sizeof(char) + sizeof(int) + columnNameLength, sizeof(int) + sizeof(int) + sizeof(int));
		memcpy(&columnVersion, columnData + sizeof(char) + sizeof(int) + columnNameLength + sizeof(int) + sizeof(int) + sizeof(int), sizeof(AttrVersion));
		if(columnVersion != tableVersion){
			continue;
		}
		AttrIndex columnIndex;
		memcpy(&columnIndex, columnData + sizeof(char) + sizeof(int) + columnNameLength + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion), sizeof(AttrIndex));
		AttributeWithIndex attribute;
		attribute.name = string(columnName);
		attribute.type = (AttrType) attributeInfo[0];
		attribute.length = attributeInfo[1];
		attribute.position = attributeInfo[2];
		attribute.version = columnVersion;
		attribute.index = columnIndex;
		attrs.push_back(attribute);
		memset(columnData, 0, columnDataSize);
	}
	rmsi.close();
	free(tableData);
	free(conditionString);
	free(columnData);
    return 0;
}

RC RelationManager::getAllAttributes(const string &tableName, vector<AttributeWithVersion> &attrs, AttrVersion &version)
{
    if(tableName == "Tables"){
        createTablesAttributes(attrs);
        version = 1;
        return 0;
    }

    if(tableName == "Columns"){
        createColumnsAttributes(attrs);
        version = 1;
        return 0;
    }
	RM_ScanIterator rmsi;
	vector<string> tableAttributes;
	tableAttributes.push_back("table-id");
	tableAttributes.push_back("table-version");
	void *tableData = malloc(sizeof(char) + sizeof(unsigned) + sizeof(AttrVersion));
	int tableNameLength = tableName.length();
	void *conditionString = malloc(sizeof(int) + tableNameLength);
	memcpy(conditionString, &tableNameLength, sizeof(int));
	memcpy(conditionString + sizeof(int), tableName.c_str(), tableNameLength);
	scan("Tables", "table-name", EQ_OP, conditionString, tableAttributes, rmsi);
	RID rid;
	if (rmsi.getNextTuple(rid, tableData) == RM_EOF){
		rmsi.close();
		free(conditionString);
		free(tableData);
		return -1;
	}
	int tableId;
	memcpy(&tableId, tableData + sizeof(char), sizeof(unsigned));
	AttrVersion tableVersion;
	memcpy(&tableVersion, tableData + sizeof(char) + sizeof(unsigned), sizeof(AttrVersion));
	version = tableVersion;
	rmsi.close();
	RM_ScanIterator rmsiColumns;
	vector<string> columnAttribute;
	columnAttribute.push_back("column-name");
	columnAttribute.push_back("column-type");
	columnAttribute.push_back("column-length");
	columnAttribute.push_back("column-position");
	columnAttribute.push_back("column-version");
	columnAttribute.push_back("column-index");
	unsigned columnDataSize = sizeof(char) + sizeof(int) + sizeof(int) + 50 + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion) + sizeof(AttrIndex);
	void *columnData = malloc(columnDataSize);
	scan("Columns", "table-id", EQ_OP, &tableId, columnAttribute, rmsi);
	while(rmsi.getNextTuple(rid, columnData) != RM_EOF){
		int attributeInfo[3];
		int columnNameLength;
		memcpy(&columnNameLength, columnData + sizeof(char), sizeof(int));
		char columnName[columnNameLength + 1] = {0};
		AttrVersion columnVersion;
		memcpy(&columnName, columnData + sizeof(char) + sizeof(int), columnNameLength);
		memcpy(&attributeInfo, columnData + sizeof(char) + sizeof(int) + columnNameLength, sizeof(int) + sizeof(int) + sizeof(int));
		memcpy(&columnVersion, columnData + sizeof(char) + sizeof(int) + columnNameLength + sizeof(int) + sizeof(int) + sizeof(int), sizeof(AttrVersion));
		if(attrs.size() < attributeInfo[2]){
			attrs.resize(attributeInfo[2]);
		}
		AttributeWithVersion attribute;
		attribute.name = string(columnName);
		attribute.type = (AttrType) attributeInfo[0];
		attribute.length = attributeInfo[1];
		attribute.position = attributeInfo[2];
		attribute.version = columnVersion;
		attrs.at(attributeInfo[2] - 1) = attribute;
		memset(columnData, 0, columnDataSize);
	}

	rmsi.close();
	free(tableData);
	free(conditionString);
	free(columnData);
    return 0;
}

RC RelationManager::getAllAttributes(const string &tableName, vector<AttributeWithVersion> &attrs, AttrVersion &version, RID &tableRid, int &tableId)
{
    if(tableName == "Tables"){
        createTablesAttributes(attrs);
        version = 1;
        return 0;
    }

    if(tableName == "Columns"){
        createColumnsAttributes(attrs);
        version = 1;
        return 0;
    }
	RM_ScanIterator rmsi;
	vector<string> tableAttributes;
	tableAttributes.push_back("table-id");
	tableAttributes.push_back("table-version");
	void *tableData = malloc(sizeof(char) + sizeof(unsigned) + sizeof(AttrVersion));
	int tableNameLength = tableName.length();
	void *conditionString = malloc(sizeof(int) + tableNameLength);
	memcpy(conditionString, &tableNameLength, sizeof(int));
	memcpy(conditionString + sizeof(int), tableName.c_str(), tableNameLength);
	scan("Tables", "table-name", EQ_OP, conditionString, tableAttributes, rmsi);
	RID rid;
	if (rmsi.getNextTuple(rid, tableData) == RM_EOF){
		rmsi.close();
		free(conditionString);
		free(tableData);
		return -1;
	}
	tableRid = rid;
	memcpy(&tableId, tableData + sizeof(char), sizeof(unsigned));
	AttrVersion tableVersion;
	memcpy(&tableVersion, tableData + sizeof(char) + sizeof(unsigned), sizeof(AttrVersion));
	version = tableVersion;
	rmsi.close();
	RM_ScanIterator rmsiColumns;
	vector<string> columnAttribute;
	columnAttribute.push_back("column-name");
	columnAttribute.push_back("column-type");
	columnAttribute.push_back("column-length");
	columnAttribute.push_back("column-position");
	columnAttribute.push_back("column-version");
	columnAttribute.push_back("column-index");
	unsigned columnDataSize = sizeof(char) + sizeof(int) + sizeof(int) + 50 + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion) + sizeof(AttrIndex);
	void *columnData = malloc(columnDataSize);
	scan("Columns", "table-id", EQ_OP, &tableId, columnAttribute, rmsi);
	while(rmsi.getNextTuple(rid, columnData) != RM_EOF){
		int attributeInfo[3];
		int columnNameLength;
		memcpy(&columnNameLength, columnData + sizeof(char), sizeof(int));
		char columnName[columnNameLength + 1] = {0};
		AttrVersion columnVersion;
		memcpy(&columnName, columnData + sizeof(char) + sizeof(int), columnNameLength);
		memcpy(&attributeInfo, columnData + sizeof(char) + sizeof(int) + columnNameLength, sizeof(int) + sizeof(int) + sizeof(int));
		memcpy(&columnVersion, columnData + sizeof(char) + sizeof(int) + columnNameLength + sizeof(int) + sizeof(int) + sizeof(int), sizeof(AttrVersion));
		if(attrs.size() < attributeInfo[2]){
			attrs.resize(attributeInfo[2]);
		}
		AttributeWithVersion attribute;
		attribute.name = string(columnName);
		attribute.type = (AttrType) attributeInfo[0];
		attribute.length = attributeInfo[1];
		attribute.position = attributeInfo[2];
		attribute.version = columnVersion;
		attrs.at(attributeInfo[2] - 1) = attribute;
		memset(columnData, 0, columnDataSize);
	}

	rmsi.close();
	free(tableData);
	free(conditionString);
	free(columnData);
    return 0;
}

RC RelationManager::setIndexAttribute(const string &tableName, const string &attributeName, const AttrIndex &attributeIndex){
	RM_ScanIterator rmsi;
	vector<string> tableAttributes;
	tableAttributes.push_back("table-id");
	tableAttributes.push_back("table-version");
	void *tableData = malloc(sizeof(char) + sizeof(unsigned) + sizeof(AttrVersion));
	int tableNameLength = tableName.length();
	void *conditionString = malloc(sizeof(int) + tableNameLength);
	memcpy(conditionString, &tableNameLength, sizeof(int));
	memcpy(conditionString + sizeof(int), tableName.c_str(), tableNameLength);
	scan("Tables", "table-name", EQ_OP, conditionString, tableAttributes, rmsi);
	RID rid;
	if (rmsi.getNextTuple(rid, tableData) == RM_EOF){
		rmsi.close();
		free(conditionString);
		free(tableData);
		return -1;
	}
	int tableId;
	memcpy(&tableId, tableData + sizeof(char), sizeof(unsigned));
	AttrVersion tableVersion;
	memcpy(&tableVersion, tableData + sizeof(char) + sizeof(unsigned), sizeof(AttrVersion));
	rmsi.close();
	RM_ScanIterator rmsiColumns;
	vector<string> columnAttribute;
	columnAttribute.push_back("table-id");
	columnAttribute.push_back("column-name");
	columnAttribute.push_back("column-type");
	columnAttribute.push_back("column-length");
	columnAttribute.push_back("column-position");
	columnAttribute.push_back("column-version");
	columnAttribute.push_back("column-index");
	unsigned columnDataSize = sizeof(char) + sizeof(int) + sizeof(int) + 50 + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion) + sizeof(AttrIndex);
	void *columnData = malloc(columnDataSize);
	scan("Columns", "table-id", EQ_OP, &tableId, columnAttribute, rmsi);
	RC rc = -1;
	AttributeWithIndex attribute;
	AttrIndex columnIndex = -1;
	int columnNameLength;
	while(rmsi.getNextTuple(rid, columnData) != RM_EOF){
		int attributeInfo[3];
		memcpy(&columnNameLength, columnData + sizeof(char) + sizeof(int), sizeof(int));
		char columnName[columnNameLength + 1] = {0};
		AttrVersion columnVersion;
		memcpy(&columnName, columnData + sizeof(char) + sizeof(int) + sizeof(int), columnNameLength);
		memcpy(&attributeInfo, columnData + sizeof(char) + sizeof(int) + sizeof(int) + columnNameLength, sizeof(int) + sizeof(int) + sizeof(int));
		memcpy(&columnVersion, columnData + sizeof(char) + sizeof(int) + sizeof(int) + columnNameLength + sizeof(int) + sizeof(int) + sizeof(int), sizeof(AttrVersion));
		if(columnVersion != tableVersion || string(columnName) != attributeName){
			continue;
		}
		memcpy(&columnIndex, columnData + sizeof(char) + sizeof(int) + sizeof(int) + columnNameLength + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion), sizeof(AttrIndex));
		attribute.name = string(columnName);
		attribute.type = (AttrType) attributeInfo[0];
		attribute.length = attributeInfo[1];
		attribute.position = attributeInfo[2];
		attribute.version = columnVersion;
		attribute.index = columnIndex;
		rc = 0;
		break;
	}
	rmsi.close();
	free(tableData);
	free(conditionString);
	if (rc == -1 || columnIndex == attributeIndex){
		free(columnData);
		return -1;
	}
	unsigned updatedColumnDataSize = sizeof(char) + sizeof(int) + sizeof(int) + columnNameLength + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion) + sizeof(AttrIndex);
	void *updatedColumnData = malloc(updatedColumnDataSize);
//	memset(updatedColumnData, 0, updatedColumnDataSize);
//	int offset = sizeof(char);
//	memcpy(updatedColumnData + offset, &tableId, sizeof(int));
//	offset += sizeof(int);
//	memcpy(updatedColumnData + offset, &columnNameLength, sizeof(int));
//	offset += sizeof(int);
//	memcpy(updatedColumnData + offset, &attribute.name, columnNameLength);
//	offset += columnNameLength;
//	memcpy(updatedColumnData + offset, &attribute.type, sizeof(AttrType));
//	offset += sizeof(AttrType);
//	memcpy(updatedColumnData + offset, &attribute.length, sizeof(int));
//	offset += sizeof(int);
//	memcpy(updatedColumnData + offset, &attribute.position, sizeof(int));
//	offset += sizeof(int);
//	memcpy(updatedColumnData + offset, &attribute.version, sizeof(int));
//	offset += sizeof(AttrVersion);
	int offset = updatedColumnDataSize - sizeof(AttrIndex);
	memcpy(updatedColumnData, columnData, offset);
	memcpy(updatedColumnData + offset, &attributeIndex, sizeof(AttrIndex));
	if (updateTuple("Columns", updatedColumnData, rid, TableTypeSystem) == -1){
		free(columnData);
		free(updatedColumnData);
		return -1;
	}
	free(columnData);
	free(updatedColumnData);
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return insertTuple(tableName, data, rid, TableTypeUser);
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid, const TableType tableType){
	if (!checkSystemTablePermission(tableName, tableType)){
		return -1;
	}
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName, fileHandle) == -1){
		return -1;
	}

	vector<AttributeWithIndex> recordDescriptor;
	AttrVersion version;
	RID tableRid;
	int tableId;
	if (getAttributes(tableName, recordDescriptor, version, tableRid, tableId) == -1){
	    _rbfm->closeFile(fileHandle);
		return -1;
	}

	vector<AttributeWithVersion> insertedRecordDescriptor;
	for (AttributeWithIndex attribute : recordDescriptor){
		insertedRecordDescriptor.push_back(attribute);
	}
	if (_rbfm->insertRecord(fileHandle, insertedRecordDescriptor, data, rid, version) == -1){
	    _rbfm->closeFile(fileHandle);
		return -1;
	}

	if (_rbfm->closeFile(fileHandle) == -1){
		return -1;
	}

	for (AttributeWithIndex attribute : recordDescriptor){
		if (attribute.index == 1){
			string indexFileName = getIndexFileName(tableName, attribute.name);
			IXFileHandle ixfileHandle;
			if (_im->openFile(indexFileName, ixfileHandle) == -1){
				return -1;
			}
			void *indexKey = malloc(PAGE_SIZE);
			memset(indexKey, 0, PAGE_SIZE);
			char nullIndicator = {0};
			readAttribute(tableName, rid, attribute.name, indexKey);
			memcpy(&nullIndicator, indexKey, sizeof(char));
			if (getBit(nullIndicator, 0)){
				continue;
			}
			memmove(indexKey, indexKey + sizeof(char), PAGE_SIZE - sizeof(char));
			if (_im->insertEntry(ixfileHandle, attribute, indexKey, rid) == -1){
				_im->closeFile(ixfileHandle);
				free(indexKey);
				return -1;
			}
			free(indexKey);
			_im->closeFile(ixfileHandle);
		}
	}
	return 0;

}

RC RelationManager::insertTuple(const string &tableName, const vector<Attribute> &recordDescriptor, const void *data, RID &rid, const TableType tableType){
	if (!checkSystemTablePermission(tableName, tableType)){
		return -1;
	}
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName, fileHandle) == -1){
		return -1;
	}

	if (_rbfm->insertRecord(fileHandle, recordDescriptor, data, rid) == -1){
	    _rbfm->closeFile(fileHandle);
		return -1;
	}

//	for (AttributeWithIndex attribute : recordDescriptor){
//		if (attribute.index == 1){
//			string indexFileName = getIndexFileName(tableName, attribute.name);
//			IXFileHandle ixfileHandle;
//			if (_im->openFile(indexFileName, ixfileHandle) == -1){
//				return -1;
//			}
//			void *indexKey = malloc(PAGE_SIZE);
//			 readAttribute(tableName, rid, attribute.name, indexKey);
//			if (_im->insertEntry(ixfileHandle, attribute, indexKey, rid) == -1){
//				_im->closeFile(ixfileHandle);
//				return -1;
//			}
//			_im->closeFile(ixfileHandle);
//		}
//	}
    return _rbfm->closeFile(fileHandle);
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return deleteTuple(tableName, rid, TableTypeUser);
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid, const TableType tableType){
	if (!checkSystemTablePermission(tableName, tableType)){
		return -1;
	}
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName, fileHandle) == -1){
		return -1;
	}

	vector<AttributeWithIndex> recordDescriptor;
	if (getAttributes(tableName, recordDescriptor) == -1){
	    _rbfm->closeFile(fileHandle);
		return -1;
	}

	vector<Attribute> deletedRecordDescriptor;
	for (AttributeWithIndex attribute : recordDescriptor){
		deletedRecordDescriptor.push_back(attribute);
	}

	for (AttributeWithIndex attribute : recordDescriptor){
		if (attribute.index == 1){
			string indexFileName = getIndexFileName(tableName, attribute.name);
			IXFileHandle ixfileHandle;
			if (_im->openFile(indexFileName, ixfileHandle) == -1){
				return -1;
			}
			void *indexKey = malloc(PAGE_SIZE);
			memset(indexKey, 0, PAGE_SIZE);
			char nullIndicator = {0};
			readAttribute(tableName, rid, attribute.name, indexKey);
			memcpy(&nullIndicator, indexKey, sizeof(char));
			if (getBit(nullIndicator, 0)){
				continue;
			}
			memmove(indexKey, indexKey + sizeof(char), PAGE_SIZE - sizeof(char));
			if (_im->deleteEntry(ixfileHandle, attribute, indexKey, rid) == -1){
				_im->closeFile(ixfileHandle);
				free(indexKey);
				return -1;
			}
			free(indexKey);
			_im->closeFile(ixfileHandle);
		}
	}

	if (_rbfm->deleteRecord(fileHandle, deletedRecordDescriptor, rid) == -1){
	    _rbfm->closeFile(fileHandle);
		return -1;
	}

    return _rbfm->closeFile(fileHandle);
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return updateTuple(tableName, data, rid, TableTypeUser);
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid, const TableType tableType){
	if (!checkSystemTablePermission(tableName, tableType)){
		return -1;
	}
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName, fileHandle) == -1){
		return -1;
	}

	vector<AttributeWithIndex> recordDescriptor;
	AttrVersion version;
	RID tableRid;
	int tableId;
	if (getAttributes(tableName, recordDescriptor, version, tableRid, tableId) == -1){
		_rbfm->closeFile(fileHandle);
		return -1;
	}

	for (AttributeWithIndex attribute : recordDescriptor){
		if (attribute.index == 1){
			string indexFileName = getIndexFileName(tableName, attribute.name);
			IXFileHandle ixfileHandle;
			if (_im->openFile(indexFileName, ixfileHandle) == -1){
				return -1;
			}
			void *indexKey = malloc(PAGE_SIZE);
			memset(indexKey, 0, PAGE_SIZE);
			char nullIndicator = {0};
			readAttribute(tableName, rid, attribute.name, indexKey);
			memcpy(&nullIndicator, indexKey, sizeof(char));
			if (getBit(nullIndicator, 0)){
				continue;
			}
			memmove(indexKey, indexKey + sizeof(char), PAGE_SIZE - sizeof(char));
			if (_im->deleteEntry(ixfileHandle, attribute, indexKey, rid) == -1){
				_im->closeFile(ixfileHandle);
				free(indexKey);
				return -1;
			}
			free(indexKey);
			_im->closeFile(ixfileHandle);
		}
	}

	vector<AttributeWithVersion> updatedRecordDescriptor;
	for (AttributeWithIndex attribute : recordDescriptor){
		updatedRecordDescriptor.push_back(attribute);
	}

	if (_rbfm->updateRecord(fileHandle, updatedRecordDescriptor, data, rid, version) == -1){
	    _rbfm->closeFile(fileHandle);
		return -1;
	}

	if (_rbfm->closeFile(fileHandle) == -1){
		return -1;
	}

	for (AttributeWithIndex attribute : recordDescriptor){
		if (attribute.index == 1){
			string indexFileName = getIndexFileName(tableName, attribute.name);
			IXFileHandle ixfileHandle;
			if (_im->openFile(indexFileName, ixfileHandle) == -1){
				return -1;
			}
			void *indexKey = malloc(PAGE_SIZE);
			memset(indexKey, 0, PAGE_SIZE);
			char nullIndicator = {0};
			readAttribute(tableName, rid, attribute.name, indexKey);
			memcpy(&nullIndicator, indexKey, sizeof(char));
			if (getBit(nullIndicator, 0)){
				continue;
			}
			memmove(indexKey, indexKey + sizeof(char), PAGE_SIZE - sizeof(char));
			if (_im->insertEntry(ixfileHandle, attribute, indexKey, rid) == -1){
				_im->closeFile(ixfileHandle);
				free(indexKey);
				return -1;
			}
			free(indexKey);
			_im->closeFile(ixfileHandle);
		}
	}

    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName, fileHandle) == -1){
		return -1;
	}

	AttrVersion recordVersion;
	RecordDirectoryLength recordDirectoryLength;
	if (_rbfm->getRecordVersionWithLength(fileHandle, rid, recordVersion, recordDirectoryLength) == -1){
		_rbfm->closeFile(fileHandle);
		return -1;
	}
	vector<AttributeWithVersion> recordDescriptor;
	AttrVersion tableVersion;
	if (getAllAttributes(tableName, recordDescriptor, tableVersion) == -1){
		_rbfm->closeFile(fileHandle);
		return -1;
	}
	if (recordVersion != tableVersion){
		updateRecordVersion(fileHandle, recordDescriptor, rid, tableName, recordDirectoryLength, recordVersion, tableVersion);
	}

	for (int i = 0; i < recordDescriptor.size(); ++i){
		if (recordDescriptor.at(i).version != tableVersion){
			recordDescriptor.erase(recordDescriptor.begin() + i);
		}
	}

	if (_rbfm->readRecord(fileHandle, recordDescriptor, rid, data, tableVersion) == -1){
	    _rbfm->closeFile(fileHandle);
		return -1;
	}

    return _rbfm->closeFile(fileHandle);
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return _rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName, fileHandle) == -1){
		return -1;
	}

	AttrVersion recordVersion;
	RecordDirectoryLength recordDirectoryLength;
	if (_rbfm->getRecordVersionWithLength(fileHandle, rid, recordVersion, recordDirectoryLength) == -1){
		_rbfm->closeFile(fileHandle);
		return -1;
	}

	vector<AttributeWithVersion> recordDescriptor;
	AttrVersion tableVersion;
	if (getAllAttributes(tableName, recordDescriptor, tableVersion) == -1){
		_rbfm->closeFile(fileHandle);
		return -1;
	}

	vector<AttributeWithVersion> currentRecordDescriptor;
	for (AttributeWithVersion attribute : recordDescriptor){
		if (attribute.version == tableVersion){
			currentRecordDescriptor.push_back(attribute);
		}
	}

	if (recordVersion != tableVersion){
		updateRecordVersion(fileHandle, recordDescriptor, rid, tableName, recordDirectoryLength, recordVersion, tableVersion);
	}

	RID tempRid;
	if (_rbfm->readAttribute(fileHandle, currentRecordDescriptor, rid, attributeName, data, tempRid, tableVersion) == -1){
	    _rbfm->closeFile(fileHandle);
		return -1;
	}

    return _rbfm->closeFile(fileHandle);
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	if (_rbfm->openFile(tableName, rm_ScanIterator.fileHandle) == -1){
		return -1;
	}
	vector<AttributeWithVersion> recordDescriptorWithVersion;
	AttrVersion version;
	if (getAllAttributes(tableName, recordDescriptorWithVersion, version) == -1){
	    RecordBasedFileManager::instance()->closeFile(rm_ScanIterator.fileHandle);
		return -1;
	}

	vector<AttributeWithVersion> currentRecordDescriptor;
	for (AttributeWithVersion attribute : recordDescriptorWithVersion){
		if (attribute.version == version){
			currentRecordDescriptor.push_back(attribute);
		}
	}

	return _rbfm->scan(rm_ScanIterator.fileHandle, currentRecordDescriptor, version, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);
}

RM_ScanIterator::RM_ScanIterator(){
}

RM_ScanIterator::~RM_ScanIterator(){
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
	return rbfm_ScanIterator.getNextRecord(rid, data);
}

RC RM_ScanIterator::close(){
	if (rbfm_ScanIterator.fileHandle.file){
		RecordBasedFileManager::instance()->closeFile(rbfm_ScanIterator.fileHandle);
	}
	if (fileHandle.file){
		fileHandle.file = 0;
	}
	return rbfm_ScanIterator.close();
}

void RelationManager::createTablesAttributes(vector<Attribute> &attributes){
	Attribute attribute;
	attribute.name = "table-id";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attributes.push_back(attribute);

	attribute.name = "table-name";
	attribute.type = TypeVarChar;
	attribute.length = 50;
	attributes.push_back(attribute);

	attribute.name = "file-name";
	attribute.type = TypeVarChar;
	attribute.length = 50;
	attributes.push_back(attribute);

	attribute.name = "table-type";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attributes.push_back(attribute);

	attribute.name = "table-version";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attributes.push_back(attribute);
}

void RelationManager::createTablesAttributes(vector<AttributeWithVersion> &attributes){
	AttributeWithVersion attribute;
	attribute.name = "table-id";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 1;
	attribute.version = 1;
	attributes.push_back(attribute);

	attribute.name = "table-name";
	attribute.type = TypeVarChar;
	attribute.length = 50;
	attribute.position = 2;
	attribute.version = 1;
	attributes.push_back(attribute);

	attribute.name = "file-name";
	attribute.type = TypeVarChar;
	attribute.length = 50;
	attribute.position = 3;
	attribute.version = 1;
	attributes.push_back(attribute);

	attribute.name = "table-type";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 4;
	attribute.version = 1;
	attributes.push_back(attribute);

	attribute.name = "table-version";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 5;
	attribute.version = 1;
	attributes.push_back(attribute);
}

void RelationManager::createTablesAttributes(vector<AttributeWithIndex> &attributes){
	AttributeWithIndex attribute;
	attribute.name = "table-id";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 1;
	attribute.version = 1;
	attribute.index = 0;
	attributes.push_back(attribute);

	attribute.name = "table-name";
	attribute.type = TypeVarChar;
	attribute.length = 50;
	attribute.position = 2;
	attribute.version = 1;
	attribute.index = 0;
	attributes.push_back(attribute);

	attribute.name = "file-name";
	attribute.type = TypeVarChar;
	attribute.length = 50;
	attribute.position = 3;
	attribute.version = 1;
	attribute.index = 0;
	attributes.push_back(attribute);

	attribute.name = "table-type";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 4;
	attribute.version = 1;
	attribute.index = 0;
	attributes.push_back(attribute);

	attribute.name = "table-version";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 5;
	attribute.version = 1;
	attribute.index = 0;
	attributes.push_back(attribute);
}


void RelationManager::createColumnsAttributes(vector<Attribute> &attributes){
	Attribute attribute;
	attribute.name = "table-id";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attributes.push_back(attribute);

	attribute.name = "column-name";
	attribute.type = TypeVarChar;
	attribute.length = 50;
	attributes.push_back(attribute);

	attribute.name = "column-type";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attributes.push_back(attribute);

	attribute.name = "column-length";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attributes.push_back(attribute);

	attribute.name = "column-position";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attributes.push_back(attribute);

	attribute.name = "column-version";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attributes.push_back(attribute);

	attribute.name = "column-index";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attributes.push_back(attribute);
}

void RelationManager::createColumnsAttributes(vector<AttributeWithVersion> &attributes){
	AttributeWithVersion attribute;
	attribute.name = "table-id";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 1;
	attribute.version = 1;
	attributes.push_back(attribute);

	attribute.name = "column-name";
	attribute.type = TypeVarChar;
	attribute.length = 50;
	attribute.position = 2;
	attribute.version = 1;
	attributes.push_back(attribute);

	attribute.name = "column-type";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 3;
	attribute.version = 1;
	attributes.push_back(attribute);

	attribute.name = "column-length";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 4;
	attribute.version = 1;
	attributes.push_back(attribute);

	attribute.name = "column-position";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 5;
	attribute.version = 1;
	attributes.push_back(attribute);

	attribute.name = "column-version";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 6;
	attribute.version = 1;
	attributes.push_back(attribute);

	attribute.name = "column-index";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 7;
	attribute.version = 1;
	attributes.push_back(attribute);
}

void RelationManager::createColumnsAttributes(vector<AttributeWithIndex> &attributes){
	AttributeWithIndex attribute;
	attribute.name = "table-id";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 1;
	attribute.version = 1;
	attribute.index = 0;
	attributes.push_back(attribute);

	attribute.name = "column-name";
	attribute.type = TypeVarChar;
	attribute.length = 50;
	attribute.position = 2;
	attribute.version = 1;
	attribute.index = 0;
	attributes.push_back(attribute);

	attribute.name = "column-type";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 3;
	attribute.version = 1;
	attribute.index = 0;
	attributes.push_back(attribute);

	attribute.name = "column-length";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 4;
	attribute.version = 1;
	attribute.index = 0;
	attributes.push_back(attribute);

	attribute.name = "column-position";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 5;
	attribute.version = 1;
	attribute.index = 0;
	attributes.push_back(attribute);

	attribute.name = "column-version";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 6;
	attribute.version = 1;
	attribute.index = 0;
	attributes.push_back(attribute);

	attribute.name = "column-index";
	attribute.type = TypeInt;
	attribute.length = sizeof(int);
	attribute.position = 7;
	attribute.version = 1;
	attribute.index = 0;
	attributes.push_back(attribute);
}


bool RelationManager::isSystemTable(const string &tableName){
	return tableName == "Tables" || tableName == "Columns";
}

bool RelationManager::checkSystemTablePermission(const string &tableName, const TableType tableType){
	if (isSystemTable(tableName)){
		return tableType == TableTypeSystem;
	}
	return true;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return dropAttribute(tableName, attributeName, TableTypeUser);
}

RC RelationManager::dropAttribute(const string &tableName, const string &attributeName, const TableType tableType) {
	if (!checkSystemTablePermission(tableName, tableType)){
		return -1;
	}
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName, fileHandle) == -1){
		return -1;
	}

	vector<AttributeWithVersion> attributesWithVersion;
	AttrVersion tableVersion;
	RID tableRid;
	int tableId;
	if (getAllAttributes(tableName, attributesWithVersion, tableVersion, tableRid, tableId) == -1){
	    RecordBasedFileManager::instance()->closeFile(fileHandle);
		return -1;
	}

	int tableDataSize = sizeof(char) + sizeof(int) + sizeof(int) + 50 + sizeof(int) + 50 + sizeof(int) + sizeof(AttrVersion);
	void *data = malloc(tableDataSize);

	AttrVersion newTableVersion = tableVersion + 1;
	readTuple("Tables", tableRid, data);

	int offset = sizeof(char) + sizeof(int);
	int length;
	memcpy(&length, data + offset, sizeof(int));
	offset += sizeof(int);
	offset += length;
	memcpy(&length, data + offset, sizeof(int));
	offset += sizeof(int);
	offset += length;
	memcpy(data + offset, &tableType, sizeof(TableType));
	offset += sizeof(int);
	memcpy(data + offset, &newTableVersion, sizeof(AttrVersion));
	offset += sizeof(AttrVersion);
	void *updatedTableData = malloc(offset);
	memcpy(updatedTableData, data, offset);
	free(data);
	if(updateTuple("Tables", updatedTableData, tableRid, TableTypeSystem) == -1){
		free(updatedTableData);
		return -1;
	}
	free(updatedTableData);
	vector<AttributeWithVersion> newVersion;
	for (AttributeWithVersion attributeWithVersion : attributesWithVersion){
		if (attributeWithVersion.version == tableVersion){
			if (attributeWithVersion.name != attributeName){
				newVersion.push_back(attributeWithVersion);
			}
		}
	}
	for (AttributeWithVersion attribute : newVersion){
		offset = 1;
		int attributeNameLength = attribute.name.length();
		int columnSize = sizeof(char) + sizeof(int) + sizeof(int) + attributeNameLength + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion);
		void* columnData = malloc(columnSize);
		memset(columnData, 0, columnSize);
		memcpy(columnData + offset, &tableId, sizeof(int));
		offset += sizeof(int);
		memcpy(columnData + offset, &attributeNameLength, sizeof(int));
		offset += sizeof(int);
		memcpy(columnData + offset, attribute.name.c_str(), attributeNameLength);
		offset += attributeNameLength;
		memcpy(columnData + offset, &(attribute.type), sizeof(int));
		offset += sizeof(int);
		memcpy(columnData + offset, &(attribute.length), sizeof(int));
		offset += sizeof(int);
		memcpy(columnData + offset, &(attribute.position), sizeof(int));
		offset += sizeof(int);
		memcpy(columnData + offset, &newTableVersion, sizeof(AttrVersion));
		RID tempRid;
		if(insertTuple("Columns", columnData, tempRid, TableTypeSystem) == -1){
			free(columnData);
			return -1;
		}
		free(columnData);
	}

    return 0;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return addAttribute(tableName, attr, TableTypeUser);
}

RC RelationManager::addAttribute(const string &tableName, const Attribute &attr, const TableType tableType) {
	if (!checkSystemTablePermission(tableName, tableType)){
		return -1;
	}
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName, fileHandle) == -1){
		return -1;
	}

	vector<AttributeWithVersion> attributesWithVersion;
	AttrVersion tableVersion;
	RID tableRid;
	int tableId;
	if (getAllAttributes(tableName, attributesWithVersion, tableVersion, tableRid, tableId) == -1){
	    RecordBasedFileManager::instance()->closeFile(fileHandle);
		return -1;
	}

	int tableDataSize = sizeof(char) + sizeof(int) + sizeof(int) + 50 + sizeof(int) + 50 + sizeof(int) + sizeof(AttrVersion);
	void *data = malloc(tableDataSize);

	AttrVersion newTableVersion = tableVersion + 1;
	readTuple("Tables", tableRid, data);

	int offset = sizeof(char) + sizeof(int);
	int length;
	memcpy(&length, data + offset, sizeof(int));
	offset += sizeof(int);
	offset += length;
	memcpy(&length, data + offset, sizeof(int));
	offset += sizeof(int);
	offset += length;
	memcpy(data + offset, &tableType, sizeof(TableType));
	offset += sizeof(int);
	memcpy(data + offset, &newTableVersion, sizeof(AttrVersion));
	offset += sizeof(AttrVersion);
	void *updatedTableData = malloc(offset);
	memcpy(updatedTableData, data, offset);
	free(data);
	if(updateTuple("Tables", updatedTableData, tableRid, TableTypeSystem) == -1){
		free(updatedTableData);
		return -1;
	}
	free(updatedTableData);

	vector<AttributeWithVersion> newVersion;
	for (AttributeWithVersion attributeWithVersion : attributesWithVersion){
		if (attributeWithVersion.version == tableVersion){
			newVersion.push_back(attributeWithVersion);
		}
	}
	int nextPosition = attributesWithVersion.size() + 1;
	AttributeWithVersion newAttribute;
	newAttribute.name = attr.name;
	newAttribute.type = attr.type;
	newAttribute.length = attr.length;
	newAttribute.version = newTableVersion;
	newAttribute.position = nextPosition;
	newVersion.push_back(newAttribute);
	for (AttributeWithVersion attribute : newVersion){
		offset = 1;
		int attributeNameLength = attribute.name.length();
		int columnSize = sizeof(char) + sizeof(int) + sizeof(int) + attributeNameLength + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(AttrVersion);
		void* columnData = malloc(columnSize);
		memset(columnData, 0, columnSize);
		memcpy(columnData + offset, &tableId, sizeof(int));
		offset += sizeof(int);
		memcpy(columnData + offset, &attributeNameLength, sizeof(int));
		offset += sizeof(int);
		memcpy(columnData + offset, attribute.name.c_str(), attributeNameLength);
		offset += attributeNameLength;
		memcpy(columnData + offset, &(attribute.type), sizeof(int));
		offset += sizeof(int);
		memcpy(columnData + offset, &(attribute.length), sizeof(int));
		offset += sizeof(int);
		memcpy(columnData + offset, &(attribute.position), sizeof(int));
		offset += sizeof(int);
		memcpy(columnData + offset, &newTableVersion, sizeof(AttrVersion));
		RID tempRid;
		if(insertTuple("Columns", columnData, tempRid, TableTypeSystem) == -1){
			free(columnData);
			return -1;
		}
		free(columnData);
	}

	return 0;
}

RC RelationManager::updateRecordVersion(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const RID &rid, const string &tableName, const RecordDirectoryLength &recordDirectoryLength, const AttrVersion &recordVersion, const AttrVersion &tableVersion){
	vector<AttributeWithVersion> oldRecordDescriptor;
	int pos = 0;
	int oldRecordSize = 0;
	while (oldRecordDescriptor.size() != recordDirectoryLength){
		AttributeWithVersion attribute = recordDescriptor.at(pos);
		if (attribute.version >= recordVersion){
			oldRecordDescriptor.push_back(attribute);
			switch (attribute.type){
			case TypeInt:
				oldRecordSize += sizeof(int);
				break;
			case TypeReal:
				oldRecordSize += sizeof(float);
				break;
			case TypeVarChar:
				oldRecordSize += sizeof(int);
				oldRecordSize += attribute.length;
				break;
			case TypeDefault:
				return -1;
				break;
			default:
				return -1;
				break;
			}
		}
		++pos;
	}
	int oldNullFieldsIndicatorActualSize = (int)ceil(oldRecordDescriptor.size() / 8.0);
	oldRecordSize += oldNullFieldsIndicatorActualSize;
	void *oldData = malloc(oldRecordSize);
	if (_rbfm->readRecord(fileHandle, oldRecordDescriptor, rid, oldData, recordVersion) == -1){
		_rbfm->closeFile(fileHandle);
		return -1;
	}
	unsigned char oldNullsIndicator[oldNullFieldsIndicatorActualSize];
	memcpy(&oldNullsIndicator, oldData, oldNullFieldsIndicatorActualSize);
	int oldOffset = oldNullFieldsIndicatorActualSize;
	vector<AttributeWithData> attributesWithData;
	for (int i = 0, bytePosition = 0; i < recordDirectoryLength; ++i){
		AttributeWithVersion attribute = recordDescriptor.at(i);
		AttributeWithData attributeWithData;
		attributeWithData.name = attribute.name;
		attributeWithData.type = attribute.type;
		attributeWithData.length = attribute.length;
		attributeWithData.version = attribute.version;
		attributeWithData.position = attribute.position;
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
		if(getBit(oldNullsIndicator[bytePosition], i % 8)){
			attributeWithData.isNull = true;
			continue;
		}
		attributeWithData.isNull = false;
		switch(attribute.type){
		case TypeInt:{
			int intValue = 0;
			memcpy(&intValue, oldData + oldOffset, sizeof(int));
			oldOffset += sizeof(int);
			attributeWithData.intData = intValue;
			break;
		}
		case TypeReal:{
			float floatValue = 0;
			memcpy(&floatValue, oldData + oldOffset, sizeof(float));
			oldOffset += sizeof(float);
			attributeWithData.realData = floatValue;
			break;
		}
		case TypeVarChar:{
			int varcharLength = 0;
			memcpy(&varcharLength, oldData + oldOffset, sizeof(int));
			oldOffset += sizeof(int);
			char varchar[varcharLength + 1] = {0};
			memcpy(&varchar, oldData + oldOffset, varcharLength);
			oldOffset += varcharLength;
			attributeWithData.varcharData = varchar;
			break;
		}
		case TypeDefault:
			free(oldData);
			return -1;
			break;
		default:
			free(oldData);
			return -1;
			break;
		}
		attributesWithData.push_back(attributeWithData);
	}
	free(oldData);

	int recordSize = 0;
	vector<AttributeWithVersion> newRecordDescriptor;;
	for(AttributeWithVersion attribute: recordDescriptor){
		if (attribute.version == tableVersion){
			newRecordDescriptor.push_back(attribute);
			switch (attribute.type){
			case TypeInt:
				recordSize += sizeof(int);
				break;
			case TypeReal:
				recordSize += sizeof(float);
				break;
			case TypeVarChar:
				recordSize += sizeof(int);
				recordSize += attribute.length;
				break;
			case TypeDefault:
				return -1;
				break;
			default:
				return -1;
				break;
			}
		}
	}
	int nullFieldsIndicatorActualSize = (int)ceil(newRecordDescriptor.size() / 8.0);
	recordSize += nullFieldsIndicatorActualSize;
	void *newData = malloc(recordSize);
	unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
	int offset = nullFieldsIndicatorActualSize;
	for (int i = 0, bytePosition = 0; i < newRecordDescriptor.size(); ++i){
		AttributeWithVersion attribute = newRecordDescriptor.at(i);
		AttributeWithData oldAttribute;
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
		bool hit = false;
		for (AttributeWithData attributeWithData : attributesWithData){
			if (attribute.position == attributeWithData.position){
				oldAttribute = attributeWithData;
				hit = true;
				break;
			}
		}

		if (hit){
			if (oldAttribute.isNull){
				setNullBit(nullsIndicator[bytePosition], i % 8);
				continue;
			}

			switch(attribute.type){
			case TypeInt:
				memcpy(newData + offset, &(oldAttribute.intData), sizeof(int));
				offset += sizeof(int);
				break;
			case TypeReal:
				memcpy(newData + offset, &(oldAttribute.realData), sizeof(float));
				offset += sizeof(float);
				break;
			case TypeVarChar:{
				int varcharLength = oldAttribute.varcharData.length();
				memcpy(newData + offset, &varcharLength, sizeof(int));
				offset += sizeof(int);
				memcpy(newData + offset, oldAttribute.varcharData.c_str(), varcharLength);
				offset += varcharLength;
				break;
			}
			case TypeDefault:
				return -1;
				break;
			default:
				return -1;
				break;
			}
		}else{
			setNullBit(nullsIndicator[bytePosition], i % 8);
		}
	}
	memcpy(newData, nullsIndicator, nullFieldsIndicatorActualSize);
	if (updateTuple(tableName, newData, rid) == -1){
		free(newData);
		return -1;
	}
	free(newData);
	return 0;
}


RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	string indexFileName = getIndexFileName(tableName, attributeName);
	vector<AttributeWithVersion> recordDescriptor;
	Attribute indexAttribute;
	vector<string> indexAttributeName;
	AttrVersion version;
	RID rid;
	int tableId;
	getAttributes(tableName, recordDescriptor, version, rid, tableId);
	vector<AttributeWithIndex> test;
	getAttributes(tableName, test);
	for (Attribute attribute : recordDescriptor){
		if (attribute.name == attributeName){
			indexAttributeName.push_back(attribute.name);
			indexAttribute.name = attribute.name;
			indexAttribute.length = attribute.length;
			indexAttribute.type = attribute.type;
			break;
		}
	}
	if (indexAttributeName.size() != 1){
		return -1;
	}
	if (setIndexAttribute(tableName, attributeName, 1) == -1){
		return -1;
	}
	if (_im->createFile(indexFileName) == -1){
		setIndexAttribute(tableName, attributeName, 0);
		return -1;
	}
	IXFileHandle ixfileHandle;
	if (_im->openFile(indexFileName, ixfileHandle) == -1){
		setIndexAttribute(tableName, attributeName, 0);
		_im->destroyFile(indexFileName);
		return -1;
	}
	FileHandle fileHandle;
	if (_rbfm->openFile(tableName, fileHandle) == -1){
		setIndexAttribute(tableName, attributeName, 0);
		_im->closeFile(ixfileHandle);
		_im->destroyFile(indexFileName);
		return -1;
	}
	RBFM_ScanIterator rmsi;
	if (_rbfm->scan(fileHandle, recordDescriptor, version, "", NO_OP, NULL, indexAttributeName, rmsi) == -1){
		_rbfm->closeFile(fileHandle);
		setIndexAttribute(tableName, attributeName, 0);
		_im->closeFile(ixfileHandle);
		_im->destroyFile(indexFileName);
		return -1;
	}
	void *indexKey = malloc(PAGE_SIZE);
	memset(indexKey, 0, PAGE_SIZE);
	while (rmsi.getNextRecord(rid, indexKey) != RBFM_EOF){
		char nullIndicator = {0};
		memcpy(&nullIndicator, indexKey, sizeof(char));
		if (getBit(nullIndicator, 0)){
			continue;
		}
		memmove(indexKey, indexKey + sizeof(char), PAGE_SIZE - sizeof(char));
		if (_im->insertEntry(ixfileHandle, indexAttribute, indexKey, rid) == -1){
			free(indexKey);
			_rbfm->closeFile(fileHandle);
			rmsi.close();
			setIndexAttribute(tableName, attributeName, 0);
			_im->closeFile(ixfileHandle);
			_im->destroyFile(indexFileName);
			return -1;
		}
		memset(indexKey, 0, PAGE_SIZE);
	}
	free(indexKey);
	return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	string indexFileName = getIndexFileName(tableName, attributeName);
	if (_im->destroyFile(indexFileName) == -1){
		return -1;
	}
	if (setIndexAttribute(tableName, attributeName, 0) == -1){
		return -1;
	}
	return 0;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	string indexFileName = getIndexFileName(tableName, attributeName);
	if (_im->openFile(indexFileName, rm_IndexScanIterator.ixfileHandle) == -1){
		return -1;
	}
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	Attribute indexAttribute;
	RC rc = -1;
	for (Attribute attribute : recordDescriptor){
		if (attribute.name == attributeName){
			indexAttribute = attribute;
			rc = 0;
			break;
		}
	}
	if (rc == -1){
		_im->closeFile(rm_IndexScanIterator.ixfileHandle);
		return -1;
	}
	return _im->scan(rm_IndexScanIterator.ixfileHandle, indexAttribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_ScanIterator);
}

string RelationManager::getIndexFileName(const string &tableName, const string &attributeName){
	return tableName + "_" + attributeName + "_index";
}

RM_IndexScanIterator::RM_IndexScanIterator(){
}

RM_IndexScanIterator::~RM_IndexScanIterator(){
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
	return ix_ScanIterator.getNextEntry(rid, key);
}

RC RM_IndexScanIterator::close(){
	return ix_ScanIterator.close();
}
