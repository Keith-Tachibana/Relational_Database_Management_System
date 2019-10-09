#include "rbfm.h"
#include <iostream>
#include <iomanip>
#include <cmath>
#include <cstring>
#include <algorithm>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
	if(_rbf_manager){
		delete _rbf_manager;
	}
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance()->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	vector<AttributeWithVersion> recordDescriptorWithVersion;
	createAttributesWithVersion(recordDescriptor, recordDescriptorWithVersion, 1);
	return insertRecord(fileHandle, recordDescriptorWithVersion, data, rid, 1);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const void *data, RID &rid, AttrVersion version){
	if(recordDescriptor.empty() || data == NULL){
		return -1;
	}
	int numberOfPages = fileHandle.getNumberOfPages();
	RecordSize formattedRecordSize = getFormattedRecordSizeFromData(recordDescriptor, data);
	if (formattedRecordSize < sizeof(TombstoneBool) + sizeof(RID)){
		formattedRecordSize = sizeof(TombstoneBool) + sizeof(RID);
	}
	void *formattedRecord = malloc(formattedRecordSize);
	createRecordDirectory(data, recordDescriptor, formattedRecord, version);
	void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
	if (numberOfPages == 0){
		createSlotDirectory(page);
		memcpy(page, formattedRecord, formattedRecordSize);
		increaseFreeSpaceOffset(page, formattedRecordSize);
		incrementTotalSlots(page);
		SlotOffset slotOffset = 0;
		setSlotOffset(page, 0, slotOffset);
		setSlotLength(page, 0, formattedRecordSize);
		rid.slotNum = 0;
		rid.pageNum = 0;
		fileHandle.appendPage(page);
		free(page);
		free(formattedRecord);
		return 0;
	} else {
		int lastPage = numberOfPages - 1;
		fileHandle.readPage(lastPage, page);
		FreeSpaceOffset freeSpaceOffset = getFreeSpaceOffset(page);
		TotalSlots totalSlots = getTotalSlots(page);
		int freeSpace = PAGE_SIZE - freeSpaceOffset - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(SlotOffset) - sizeof(SlotLength) - sizeof(SlotOffset) * totalSlots - sizeof(SlotLength) * totalSlots;
		if (freeSpace < formattedRecordSize){
			for (int i = 0; i < numberOfPages - 1; ++i){
				fileHandle.readPage(i, page);
				freeSpaceOffset = getFreeSpaceOffset(page);
				totalSlots = getTotalSlots(page);
				freeSpace = PAGE_SIZE - freeSpaceOffset - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(SlotOffset) - sizeof(SlotLength) - sizeof(SlotOffset) * totalSlots - sizeof(SlotLength) * totalSlots;
				if (freeSpace  < formattedRecordSize){
					continue;
				}
				memcpy(page + freeSpaceOffset, formattedRecord, formattedRecordSize);
				increaseFreeSpaceOffset(page, formattedRecordSize);
				unsigned slotNumber = totalSlots;
				for (int i = 0; i < totalSlots; ++i){
					if (getSlotOffset(page, i) == 65535){
						slotNumber = i;
						break;
					}
				}
				if (slotNumber == totalSlots){
					incrementTotalSlots(page);
				}
				setSlotOffset(page, slotNumber, freeSpaceOffset);
				setSlotLength(page, slotNumber, formattedRecordSize);
				rid.pageNum = i;
				rid.slotNum = slotNumber;
				fileHandle.writePage(i, page);
				free(page);
				free(formattedRecord);
				return 0;
			}
		} else {
			memcpy(page + freeSpaceOffset, formattedRecord, formattedRecordSize);
			increaseFreeSpaceOffset(page, formattedRecordSize);
			unsigned slotNumber = totalSlots;
			for (int i = 0; i < totalSlots; ++i){
				if (getSlotOffset(page, i) == 65535){
					slotNumber = i;
					break;
				}
			}
			if (slotNumber == totalSlots){
				incrementTotalSlots(page);
			}
			setSlotOffset(page, slotNumber, freeSpaceOffset);
			setSlotLength(page, slotNumber, formattedRecordSize);
			rid.pageNum = lastPage;
			rid.slotNum = slotNumber;
			fileHandle.writePage(lastPage, page);
			free(page);
			free(formattedRecord);
			return 0;
		}
		memset(page, 0, PAGE_SIZE);
		createSlotDirectory(page);
		memcpy(page, formattedRecord, formattedRecordSize);
		increaseFreeSpaceOffset(page, formattedRecordSize);
		incrementTotalSlots(page);
		SlotOffset slotOffset = 0;
		setSlotOffset(page, 0, slotOffset);
		setSlotLength(page, 0, formattedRecordSize);
		rid.pageNum = numberOfPages;
		rid.slotNum = 0;
		fileHandle.appendPage(page);
		free(page);
		free(formattedRecord);
		return 0;
	}

}


RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	vector<AttributeWithVersion> recordDescriptorWithVersion;
	createAttributesWithVersion(recordDescriptor, recordDescriptorWithVersion, 1);
	return readRecord(fileHandle, recordDescriptorWithVersion, rid, data, 1);
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const RID &rid, void *data, AttrVersion version) {
	void *formattedRecord;
	if (readFormattedRecord(fileHandle, rid, &formattedRecord) == -1){
		return -1;
	}

	convertFormattedRecordToData(formattedRecord, recordDescriptor, data, version);
	free(formattedRecord);
    return 0;
}

RC RecordBasedFileManager::readFormattedRecord(FileHandle &fileHandle, const RID &rid, void **formattedRecord) {
	void *page = malloc(PAGE_SIZE);
	memset(page, 0, PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, page) == -1){
		free(page);
		return -1;
	}
	SlotOffset slotOffset = getSlotOffset(page, rid.slotNum);
	if (slotOffset == 65535){
		free(page);
		return -1;
	}
	SlotLength slotLength = getSlotLength(page, rid.slotNum);
	*formattedRecord = NULL;
	*formattedRecord = malloc(slotLength);
	memcpy(*formattedRecord, page + slotOffset, slotLength);
	TombstoneBool tombstone;
	memcpy(&tombstone, *formattedRecord, sizeof(TombstoneBool));
	if (tombstone){
		RID tombstoneRid;
		memcpy(&tombstoneRid, *formattedRecord + sizeof(TombstoneBool), sizeof(RID));
		free(page);
		free(*formattedRecord);
		return readFormattedRecord(fileHandle, tombstoneRid, formattedRecord);
	}
	free(page);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	if(recordDescriptor.empty() || data == NULL){
		return -1;
	}
	int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
	memcpy(nullsIndicator, data, nullFieldsIndicatorActualSize);
//	int currentVersion = 1;
//	for (Attribute attribute : recordDescriptor){
//		if (attribute.version > currentVersion){
//			currentVersion = attribute.version;
//		}
//	}
	for(int i = 0, bytePosition = 0, offset = nullFieldsIndicatorActualSize; i < recordDescriptor.size(); ++i){
		Attribute attribute = recordDescriptor.at(i);
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
//		if (attribute.version < currentVersion){
//			continue;
//		}

		std::cout << std::setw(12) << std::left << attribute.name + ":";
		if(getBit(nullsIndicator[bytePosition], i % 8)){
			std::cout << "NULL" << std::endl;
		} else {
			if(attribute.type == TypeInt){
				int value;
				memcpy(&value, data + offset, sizeof(int));
				std::cout << value << std::endl;
				offset += sizeof(int);
			} else if (attribute.type == TypeReal){
				float value;
				memcpy(&value, data + offset, sizeof(float));
				std::cout << value << std::endl;
				offset += sizeof(float);
			} else if (attribute.type == TypeVarChar){
				int varcharLength;
				memcpy(&varcharLength, data + offset, sizeof(int));
				offset += sizeof(int);
				char value[varcharLength + 1] = {0};
				memcpy(&value, data + offset, sizeof(char) * varcharLength);
				std::cout << value << std::endl;
				offset += sizeof(char) * varcharLength;
			} else {
				std::cout << "Unsupported Type" << std::endl;
			}
		}
	}
	std::cout << std::endl;
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
	void *page = malloc(PAGE_SIZE);
	memset(page, 0, PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, page) == -1){
		free(page);
		return -1;
	}
	TotalSlots totalSlots = getTotalSlots(page);
	SlotOffset slotOffset = getSlotOffset(page, rid.slotNum);
	SlotLength slotLength = getSlotLength(page, rid.slotNum);
	if (slotOffset == 65535){
		free(page);
		return -1;
	}
	void *deletedRecord = malloc(slotLength);
	memcpy(deletedRecord, page + slotOffset, slotLength);
	TombstoneBool tombstone;
	memcpy(&tombstone, deletedRecord, sizeof(TombstoneBool));
	if (tombstone){
		RID tombstoneRid;
		memcpy(&tombstoneRid, deletedRecord + sizeof(TombstoneBool), sizeof(RID));
		if (deleteRecord(fileHandle, recordDescriptor, tombstoneRid) == -1){
			free(page);
			free(deletedRecord);
			return -1;
		}
	}
	free(deletedRecord);
	SlotOffset newSlotOffset = 65535;
	setSlotOffset(page, rid.slotNum, newSlotOffset);
	SlotLength newSlotLength = 0;
	setSlotLength(page, rid.slotNum, newSlotLength);
	for (unsigned i = 1; i < totalSlots; ++i){
		SlotOffset currentSlotOffset = getSlotOffset(page, i);
		if (currentSlotOffset != 65535 && currentSlotOffset > slotOffset){
			SlotOffset updatedSlotOffset = currentSlotOffset - slotLength;
			setSlotOffset(page, i, updatedSlotOffset);
		}
	}
	FreeSpaceOffset freeSpaceOffset = getFreeSpaceOffset(page);
	RecordSize sizeOfRecordsWithDeletedRecord = freeSpaceOffset - slotOffset;
	void *recordsToMove = malloc(sizeOfRecordsWithDeletedRecord);
    memset(recordsToMove, 0, sizeOfRecordsWithDeletedRecord);
    memcpy(recordsToMove, page + slotOffset + slotLength, sizeOfRecordsWithDeletedRecord - slotLength);
	memcpy(page + slotOffset, recordsToMove, sizeOfRecordsWithDeletedRecord);
	free(recordsToMove);
	freeSpaceOffset -= slotLength;
	setFreeSpaceOffset(page, freeSpaceOffset);
	fileHandle.writePage(rid.pageNum, page);
	free(page);
	return 0;

}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
	vector<AttributeWithVersion> recordDescriptorWithVersion;
	createAttributesWithVersion(recordDescriptor, recordDescriptorWithVersion, 1);
	return updateRecord(fileHandle, recordDescriptorWithVersion, data, rid, 1);
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const void *data, const RID &rid, AttrVersion version){
	void *page = malloc(PAGE_SIZE);
	memset(page, 0, PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, page) == -1){
		free(page);
		return -1;
	}
	SlotOffset slotOffset = getSlotOffset(page, rid.slotNum);
	if (slotOffset == 65535){
		free(page);
		return -1;
	}
	TombstoneBool currentRecordTombstone;
	memcpy(&currentRecordTombstone, page + slotOffset, sizeof(TombstoneBool));
	if(currentRecordTombstone){
		RID tombstoneRid;
		memcpy(&tombstoneRid, page + slotOffset + sizeof(TombstoneBool), sizeof(RID));
		free(page);
		return updateRecord(fileHandle, recordDescriptor, data, tombstoneRid, version);
	}
	SlotLength slotLength = getSlotLength(page, rid.slotNum);
	RecordSize updatedFormattedRecordSize = getFormattedRecordSizeFromData(recordDescriptor, data);
	if (updatedFormattedRecordSize < sizeof(TombstoneBool) + sizeof(RID)){
		updatedFormattedRecordSize = sizeof(TombstoneBool) + sizeof(RID);
	}
	FreeSpaceOffset freeSpaceOffset = getFreeSpaceOffset(page);
	TotalSlots totalSlots = getTotalSlots(page);
	RecordSize sizeOfRecordsToMove = freeSpaceOffset - slotOffset - slotLength;
	if (slotLength > updatedFormattedRecordSize){
		int extraSpace = slotLength - updatedFormattedRecordSize;
		void *formattedRecord = malloc(updatedFormattedRecordSize);
		createRecordDirectory(data, recordDescriptor, formattedRecord, version);
		memcpy(page + slotOffset, formattedRecord, updatedFormattedRecordSize);
		free(formattedRecord);
		setSlotLength(page, rid.slotNum, updatedFormattedRecordSize);
		FreeSpaceOffset newFreeSpaceOffset = freeSpaceOffset - extraSpace;
		setFreeSpaceOffset(page, newFreeSpaceOffset);
		if (slotOffset + slotLength == freeSpaceOffset){
			memset(page + slotOffset + updatedFormattedRecordSize, 0, slotLength - extraSpace);
			fileHandle.writePage(rid.pageNum, page);
			free(page);
			return 0;
		}
		//compact page
		void *recordsToMove = malloc(sizeOfRecordsToMove);
	    memset(recordsToMove, 0, sizeOfRecordsToMove);
	    memcpy(recordsToMove, page + slotOffset + slotLength, sizeOfRecordsToMove);
		memcpy(page + slotOffset + updatedFormattedRecordSize, recordsToMove, sizeOfRecordsToMove);
		memset(page + slotOffset + updatedFormattedRecordSize + sizeOfRecordsToMove, 0, extraSpace);
		free(recordsToMove);
		for (int i = 0; i < totalSlots; ++i){
			SlotOffset currentSlotOffset = getSlotOffset(page, i);
			if (currentSlotOffset != 65535 && currentSlotOffset > slotOffset){
				SlotOffset updatedSlotOffset = currentSlotOffset - slotLength + updatedFormattedRecordSize;
				setSlotOffset(page, i, updatedSlotOffset);
			}
		}
		fileHandle.writePage(rid.pageNum, page);
		free(page);
		return 0;
	}
	int extraSpaceRequired = updatedFormattedRecordSize - slotLength;
	int freeSpace = PAGE_SIZE - freeSpaceOffset - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(SlotOffset) - sizeof(SlotLength) - sizeof(SlotOffset) * totalSlots - sizeof(SlotLength) * totalSlots;
	if (freeSpace < extraSpaceRequired){
		TombstoneBool tombstone = true;
		RecordSize tombstoneSize = sizeof(TombstoneBool) + sizeof(RID);
		int extraSpace = slotLength - tombstoneSize;
		void *tombstoneRecord = malloc(tombstoneSize);
		RID newRid;
		if (insertRecord(fileHandle, recordDescriptor, data, newRid, version) == -1){
			free(tombstoneRecord);
			free(page);
			return -1;
		}
		memcpy(tombstoneRecord, &tombstone, sizeof(TombstoneBool));
		memcpy(tombstoneRecord + sizeof(TombstoneBool), &newRid, sizeof(RID));
		memcpy(page + slotOffset, tombstoneRecord, tombstoneSize);
		free(tombstoneRecord);
		setSlotLength(page, rid.slotNum, tombstoneSize);
		FreeSpaceOffset newFreeSpaceOffset = freeSpaceOffset - extraSpace;
		setFreeSpaceOffset(page, newFreeSpaceOffset);
		//Add condition for if slot is last slot
		if (slotOffset + slotLength == freeSpaceOffset){
			memset(page + slotOffset + tombstoneSize, 0, slotLength - tombstoneSize);
			fileHandle.writePage(rid.pageNum, page);
			free(page);
			return 0;
		}
		void *recordsToMove = malloc(sizeOfRecordsToMove);
	    memcpy(recordsToMove, page + slotOffset + slotLength, sizeOfRecordsToMove);
		memcpy(page + slotOffset + tombstoneSize, recordsToMove, sizeOfRecordsToMove);
		memset(page + slotOffset + tombstoneSize + sizeOfRecordsToMove, 0, tombstoneSize);
		free(recordsToMove);
		//Add condition for if slot isn't last slot
		for (int i = 0; i < totalSlots; ++i){
			SlotOffset currentSlotOffset = getSlotOffset(page, i);
			if (currentSlotOffset != 65535 && currentSlotOffset > slotOffset){
				SlotOffset updatedSlotOffset = currentSlotOffset - slotLength + tombstoneSize;
				setSlotOffset(page, i, updatedSlotOffset);
			}
		}
		fileHandle.writePage(rid.pageNum, page);
		free(page);
		return 0;
	}
	//compact page and move record to back if needed
	FreeSpaceOffset updatedFreeSpaceOffset = freeSpaceOffset + extraSpaceRequired;
	setSlotLength(page, rid.slotNum, updatedFormattedRecordSize);
	setFreeSpaceOffset(page, updatedFreeSpaceOffset);
	void *updatedFormattedRecord = malloc(updatedFormattedRecordSize);
	memset(updatedFormattedRecord, 0, updatedFormattedRecordSize);
	createRecordDirectory(data, recordDescriptor, updatedFormattedRecord, version);
	if (slotOffset + slotLength == freeSpaceOffset){
		memcpy(page + slotOffset, updatedFormattedRecord, updatedFormattedRecordSize);
		fileHandle.writePage(rid.pageNum, page);
		free(updatedFormattedRecord);
		free(page);
		return 0;
	}
	void *recordsToMove = malloc(sizeOfRecordsToMove);
    memcpy(recordsToMove, page + slotOffset + slotLength, sizeOfRecordsToMove);
	memcpy(page + slotOffset, recordsToMove, sizeOfRecordsToMove);
	free(recordsToMove);
	for (int i = 0; i < totalSlots; ++i){
		SlotOffset currentSlotOffset = getSlotOffset(page, i);
		if (currentSlotOffset != 65535 && currentSlotOffset > slotOffset){
			SlotOffset updatedSlotOffset = currentSlotOffset - slotLength;
			setSlotOffset(page, i, updatedSlotOffset);
		}
	}
	RecordDirectorySlot updatedSlotOffset = updatedFreeSpaceOffset - updatedFormattedRecordSize;
	memcpy(page + updatedSlotOffset, updatedFormattedRecord, updatedFormattedRecordSize);
	free(updatedFormattedRecord);
	setSlotOffset(page, rid.slotNum, updatedSlotOffset);
	fileHandle.writePage(rid.pageNum, page);
	free(page);
	return 0;
}


RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
	RID actualRid;
	vector<AttributeWithVersion> recordDescriptorWithVersion;
	createAttributesWithVersion(recordDescriptor, recordDescriptorWithVersion, 1);
	return readAttribute(fileHandle, recordDescriptorWithVersion, rid,  attributeName, data, actualRid, 1);
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const RID &rid, const string &attributeName, void *data, RID &actualRid, AttrVersion version){
	void *page = malloc(PAGE_SIZE);
	memset(page, 0, PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, page) == -1){
		free(page);
		return -1;
	}

	if (readAttribute(fileHandle, recordDescriptor, rid, attributeName, data, actualRid, version, page) == -1){
		free(page);
		return -1;
	}
	free(page);
	return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const RID &rid, const string &attributeName, void *data, RID &actualRid, AttrVersion version, void *page){
	SlotOffset slotOffset = getSlotOffset(page, rid.slotNum);
	if (slotOffset == 65535){
		return -1;
	}
	actualRid = rid;
	SlotLength slotLength = getSlotLength(page, rid.slotNum);
	void *formattedRecord = malloc(slotLength);
	memcpy(formattedRecord, page + slotOffset, slotLength);
	TombstoneBool tombstone;
	memcpy(&tombstone, formattedRecord, sizeof(TombstoneBool));
	if (tombstone){
		RID tombstoneRid;
		memcpy(&tombstoneRid, formattedRecord + sizeof(TombstoneBool), sizeof(RID));
		actualRid = tombstoneRid;
		if (readAttribute(fileHandle, recordDescriptor, tombstoneRid, attributeName, data, actualRid, version) == -1){
			free(formattedRecord);
			return -1;
		}
		free(formattedRecord);
		return 0;
	}
	int directoryNumber = -1;
	AttributeWithVersion attribute;
	for (unsigned i = 0; i < recordDescriptor.size(); ++i){
		if (recordDescriptor.at(i).name == attributeName && recordDescriptor.at(i).version == version){
			attribute = recordDescriptor.at(i);
			directoryNumber = i;
			break;
		}
	}
	if (directoryNumber == -1){
		free(formattedRecord);
		return -1;
	}
	int offset = sizeof(TombstoneBool) + sizeof(AttrVersion) + directoryNumber * sizeof(RecordDirectorySlot);
	RecordDirectorySlot directoryLength;
	memcpy(&directoryLength, formattedRecord + sizeof(TombstoneBool) + sizeof(AttrVersion), sizeof(RecordDirectorySlot));
	RecordDirectorySlot directoryOffset;
	unsigned char nullIndicator;
	memset(&nullIndicator, 0, sizeof(char));
	if (directoryNumber == 0){
		directoryOffset = 0;
	} else {
		memcpy(&directoryOffset, formattedRecord + offset, sizeof(RecordDirectorySlot));
		//added check if directoryoffset is null
		if (directoryOffset == 65535){
			for (int tempOffset = offset - sizeof(RecordDirectorySlot);; tempOffset -= sizeof(RecordDirectorySlot)){
				if (tempOffset == sizeof(TombstoneBool) + sizeof(AttrVersion)){
					directoryOffset = 0;
					break;
				}
				memcpy(&directoryOffset, formattedRecord + tempOffset, sizeof(RecordDirectorySlot));
				if (directoryOffset != 65535){
					break;
				}
			}
		}
	}
	unsigned directorySize = (1 + directoryLength) * sizeof(RecordDirectorySlot);
	unsigned short recordDirectory[1 + directoryLength];
	memcpy(&recordDirectory, formattedRecord + sizeof(TombstoneBool) + sizeof(AttrVersion), directorySize);
	if (recordDirectory[directoryNumber + 1]== 65535){
		setNullBit(nullIndicator, 0);
		memcpy(data, &nullIndicator, sizeof(char));
		free(formattedRecord);
		return 0;
	} else {
		memcpy(data, &nullIndicator, sizeof(char));
		if (attribute.type == TypeInt){
			memcpy(data + sizeof(char), formattedRecord + sizeof(TombstoneBool) + sizeof(AttrVersion) + directorySize + directoryOffset, sizeof(int));
		} else if (attribute.type == TypeReal){
			memcpy(data + sizeof(char), formattedRecord + sizeof(TombstoneBool) + sizeof(AttrVersion) + directorySize + directoryOffset, sizeof(float));
		} else if (attribute.type == TypeVarChar){
			unsigned varcharLength;
			if(recordDescriptor.size() == 1){
				varcharLength = slotLength - directorySize - sizeof(TombstoneBool) - sizeof(AttrVersion);
			}
			if (directoryNumber == 0){
				varcharLength = recordDirectory[1];
			} else {
				bool restOfAttributesNull = true;
				for (int i = directoryNumber + 1; i < directorySize; ++i){
					if (recordDirectory[i] != 65535){
						varcharLength = recordDirectory[i] - recordDirectory[directoryNumber];
						restOfAttributesNull = false;
						break;
					}
				}
				if (restOfAttributesNull){
					varcharLength = slotLength - directoryOffset - directorySize - sizeof(TombstoneBool) - sizeof(AttrVersion);
				}
			}
			memcpy(data + sizeof(char), &varcharLength, sizeof(int));
			memcpy(data + sizeof(char) + sizeof(int), formattedRecord + sizeof(TombstoneBool) + sizeof(AttrVersion) + directorySize + directoryOffset, varcharLength);
		}
	}
	free(formattedRecord);
	return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
		const vector<AttributeWithVersion> &recordDescriptor,
		const AttrVersion version,
		const string &conditionAttribute,
		const CompOp compOp, const void *value,
		const vector<string> &attributeNames,
		RBFM_ScanIterator &rbfm_ScanIterator){
	rbfm_ScanIterator.fileHandle = fileHandle;
	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	rbfm_ScanIterator.version = version;
	rbfm_ScanIterator.conditionAttribute = conditionAttribute;
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.value = value;
	rbfm_ScanIterator.attributeNames = attributeNames;
	for (AttributeWithVersion attribute : recordDescriptor){
		if (conditionAttribute == attribute.name){
			rbfm_ScanIterator.valueType = attribute.type;
			if (value == NULL){
				rbfm_ScanIterator.valueSize = -1;
			} else {
				switch(rbfm_ScanIterator.valueType){
				case TypeInt:
					rbfm_ScanIterator.valueSize = sizeof(int);
					break;
				case TypeReal:
					rbfm_ScanIterator.valueSize = sizeof(float);
					break;
				case TypeVarChar:{
					int varcharLength;
					memcpy(&varcharLength, value, sizeof(int));
					rbfm_ScanIterator.valueSize = sizeof(int) + varcharLength;
					break;
				}
				default:
					return -1;
				}
			}
			break;
		}
	}
	for (unsigned i = 0; i < attributeNames.size(); ++i){
		string attributeName = attributeNames[i];
		for (AttributeWithVersion attribute : recordDescriptor){
			if (attributeName == attribute.name){
				rbfm_ScanIterator.requestedAttributes.push_back(attribute);
				break;
			}
		}
	}
	return 0;
}

RBFM_ScanIterator::RBFM_ScanIterator(){
	value = NULL;
	version = 1;
	compOp = NO_OP;
	valueSize = 0;
	previousRid.pageNum = 0;
	previousRid.slotNum = -1;
	valueType = TypeDefault;
	fileHandle.file = 0;
}

RBFM_ScanIterator::~RBFM_ScanIterator(){
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
	void *page = malloc(PAGE_SIZE);
	unsigned pageNum = previousRid.pageNum;
	unsigned slotNum = previousRid.slotNum + 1;
	unsigned numberOfPages = fileHandle.getNumberOfPages();

	for (unsigned i = pageNum; i < numberOfPages; ++i){
		if (fileHandle.readPage(i, page) == -1){
			free(page);
			return RBFM_EOF;
		}
		TotalSlots totalSlots = RecordBasedFileManager::instance()->getTotalSlots(page);
		for (unsigned j = slotNum; j < totalSlots; ++j){
			RecordDirectorySlot slotOffset = RecordBasedFileManager::instance()->getSlotOffset(page, j);
			RecordDirectorySlot slotLength = RecordBasedFileManager::instance()->getSlotLength(page, j);
			if (slotOffset == 65535){
				continue;
			}
			RID currentRid;
			currentRid.pageNum = i;
			currentRid.slotNum = j;
			if (conditionAttribute == ""){
				previousRid = currentRid;
				if (compOp == NO_OP){
					rid = currentRid;
					if (RecordBasedFileManager::instance()->createDataRecord(fileHandle, recordDescriptor, requestedAttributes, currentRid, data, version, page) == -1){
						free(page);
						return RBFM_EOF;
					}
					free(page);
					return 0;
				} else {
					continue;
				}
			}
			if (tombstoneRids.count(currentRid) == 1){
				continue;
			}
			if (slotLength < valueSize){
				continue;
			}
			RID actualRid;
			void *recordValue = malloc(slotLength);
			memset(recordValue, 0, slotLength);
			RecordBasedFileManager::instance()->readAttribute(fileHandle, recordDescriptor, currentRid, conditionAttribute, recordValue, actualRid, version, page);
			if (currentRid.pageNum != actualRid.pageNum && currentRid.slotNum != actualRid.slotNum){
				tombstoneRids.insert(actualRid);
			}
			if (compareValues(recordValue)){
				free(recordValue);
				rid = currentRid;
				previousRid = currentRid;
				if (data == NULL){
					free(page);
					return 0;
				}
				if (RecordBasedFileManager::instance()->createDataRecord(fileHandle, recordDescriptor, requestedAttributes, currentRid, data, version, page) == -1){
					free(page);
					return RBFM_EOF;
				}
				free(page);
				return 0;
			}
			free(recordValue);
		}
		slotNum = 0;
	}
	free(page);
	return RBFM_EOF;
}

RC RBFM_ScanIterator::close(){
//	if (fileHandle.file){
//		RecordBasedFileManager::instance()->closeFile(fileHandle);
//		fileHandle.file = 0;
//	}
	fileHandle.file = 0;
	value = NULL;
	valueSize = 0;
	valueType = TypeDefault;
	requestedAttributes.clear();
	tombstoneRids.clear();
	return 0;
}

bool RBFM_ScanIterator::compareValues(const void *valueToCompare){
	unsigned char nullIndicator;
	memset(&nullIndicator, 0, sizeof(char));
	memcpy(&nullIndicator, valueToCompare, sizeof(char));
	if(getBit(nullIndicator, 0)){
		switch(compOp){
		case EQ_OP:
			return value == NULL;
		case LT_OP:
			return false;
		case LE_OP:
			return value == NULL;
		case GT_OP:
			return false;
		case GE_OP:
			return value == NULL;
		case NE_OP:
			return value != NULL;
		case NO_OP:
			return true;
		default:
			return false;
		}
	}
	if (value == NULL){
		if(compOp == NO_OP){
			return true;
		} else {
			return false;
		}
	}
	switch(valueType){
	case TypeInt:{
		int recordIntValue;
		int conditionIntValue;
		memcpy(&conditionIntValue, value, sizeof(int));
		memcpy(&recordIntValue, valueToCompare + sizeof(char), sizeof(int));
		switch(compOp){
		case EQ_OP:
			return recordIntValue == conditionIntValue;
		case LT_OP:
			return recordIntValue < conditionIntValue;
		case LE_OP:
			return recordIntValue <= conditionIntValue;
		case GT_OP:
			return recordIntValue > conditionIntValue;
		case GE_OP:
			return recordIntValue >= conditionIntValue;
		case NE_OP:
			return recordIntValue != conditionIntValue;
		case NO_OP:
			return true;
		default:
			return false;
		}
		break;
	}
	case TypeReal:{
		float conditionRealValue;
		float recordRealValue;
		memcpy(&conditionRealValue, value, sizeof(float));
		memcpy(&recordRealValue, valueToCompare + sizeof(char), sizeof(float));
		switch(compOp){
		case EQ_OP:
			return recordRealValue == conditionRealValue;
		case LT_OP:
			return recordRealValue < conditionRealValue;
		case LE_OP:
			return recordRealValue <= conditionRealValue;
		case GT_OP:
			return recordRealValue > conditionRealValue;
		case GE_OP:
			return recordRealValue >= conditionRealValue;
		case NE_OP:
			return recordRealValue != conditionRealValue;
		case NO_OP:
			return true;
		default:
			return false;
		}
		break;
	}
	case TypeVarChar:{
		int varcharLength = 0;
		memcpy(&varcharLength, value, sizeof(int));
		unsigned recordVarcharLength = 0;
		memcpy(&recordVarcharLength, valueToCompare + sizeof(char), sizeof(int));
//		void *recordVarcharValue = malloc(valueSize);
//		void *conditionVarcharValue = malloc(valueSize);
//		memset(recordVarcharValue, 0, valueSize);
//		memset(conditionVarcharValue, 0, valueSize);
//		memcpy(recordVarcharValue, valueToCompare + sizeof(char) + sizeof(int), recordVarcharLength);
//		memcpy(conditionVarcharValue, value + sizeof(int), varcharLength);
//		int comparison = memcmp(recordVarcharValue, conditionVarcharValue, valueSize);
//		free(recordVarcharValue);
//		free(conditionVarcharValue);
		char recordVarchar[recordVarcharLength + 1] = {0};
		char conditionVarchar[varcharLength + 1] = {0};
		memcpy(recordVarchar, valueToCompare + sizeof(char) + sizeof(int), recordVarcharLength);
		memcpy(conditionVarchar, value + sizeof(int), varcharLength);
		int comparison = strcmp(recordVarchar, conditionVarchar);
//		int comparison = memcmp(value, valueToCompare + sizeof(char), valueSize);
		switch(compOp){
		case EQ_OP:
			return comparison == 0;
		case LT_OP:
			return comparison < 0;;
		case LE_OP:
			return comparison <= 0;
		case GT_OP:
			return comparison > 0;
		case GE_OP:
			return comparison >= 0;
		case NE_OP:
			return comparison != 0;
		case NO_OP:
			return true;
		default:{
			return false;
		}
		}
		break;
	}
	case TypeDefault:{
		switch(compOp){
		case EQ_OP:
			return false;
		case LT_OP:
			return false;
		case LE_OP:
			return false;
		case GT_OP:
			return false;
		case GE_OP:
			return false;
		case NE_OP:
			return false;
		case NO_OP:
			return true;
		default:
			return false;
		}
		break;
	}
	}
	return false;
}

RC RecordBasedFileManager::createDataRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const vector<Attribute> &attributes, const RID &rid, void *data, AttrVersion version){
	int nullFieldsIndicatorActualSize = (int)ceil(attributes.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
	unsigned offset = nullFieldsIndicatorActualSize;

	if (data == NULL){
		return -1;
	}

	for (unsigned i = 0, bytePosition = 0; i < attributes.size(); ++i){
		void *value;
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}

		Attribute attribute = attributes.at(i);
		unsigned char nullIndicator;
		switch(attribute.type){
		case TypeInt:
			value = malloc(sizeof(char) + sizeof(int));
			if(RecordBasedFileManager::instance()->readAttribute(fileHandle, recordDescriptor, rid, attribute.name, value) == -1){
				return -1;
			}
			memset(&nullIndicator, 0, sizeof(char));
			memcpy(&nullIndicator, value, sizeof(char));
			if (getBit(nullIndicator, 0)){
	    		setNullBit(nullsIndicator[bytePosition], i % 8);
			} else {
				memcpy(data + offset, value + sizeof(char), sizeof(int));
				offset += sizeof(int);
				free(value);
			}
			break;
		case TypeReal:
			value = malloc(sizeof(float) + sizeof(char));
			if(RecordBasedFileManager::instance()->readAttribute(fileHandle, recordDescriptor, rid, attribute.name, value) == -1){
				return -1;
			}
			memset(&nullIndicator, 0, sizeof(char));
			memcpy(&nullIndicator, value, sizeof(char));
			if (getBit(nullIndicator, 0)){
	    		setNullBit(nullsIndicator[bytePosition], i % 8);
			} else {
				memcpy(data + offset, value + sizeof(char), sizeof(float));
				offset += sizeof(float);
				free(value);
			}
			break;
		case TypeVarChar:
			value = malloc(sizeof(char) + sizeof(int) + attribute.length);
			memset(value, 0, sizeof(char) + sizeof(int) + attribute.length);
			if(RecordBasedFileManager::instance()->readAttribute(fileHandle, recordDescriptor, rid, attribute.name, value) == -1){
				return -1;
			}
			memset(&nullIndicator, 0, sizeof(char));
			memcpy(&nullIndicator, value, sizeof(char));
			int varcharLength;
			memcpy(&varcharLength, value + sizeof(char), sizeof(int));
			if (getBit(nullIndicator, 0)){
	    		setNullBit(nullsIndicator[bytePosition], i % 8);
			} else {
				char varchar[varcharLength];
				memcpy(&varchar, value + sizeof(char) + sizeof(int), varcharLength);
				unsigned varcharSize = sizeof(int) + varcharLength * sizeof(char);
				memcpy(data + offset, &varcharLength, sizeof(int));
				memcpy(data + offset + sizeof(int), &varchar, varcharLength);
				offset += varcharSize;
				free(value);
			}
			break;
		default:
			return -1;
		}
	}
	memcpy(data, &nullsIndicator, nullFieldsIndicatorActualSize);
	return 0;
}

RC RecordBasedFileManager::createDataRecord(FileHandle &fileHandle, const vector<AttributeWithVersion> &recordDescriptor, const vector<Attribute> &attributes, const RID &rid, void *data, AttrVersion version, void *page){
	int nullFieldsIndicatorActualSize = (int)ceil(attributes.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
	unsigned offset = nullFieldsIndicatorActualSize;

	if (data == NULL){
		return -1;
	}

	for (unsigned i = 0, bytePosition = 0; i < attributes.size(); ++i){
		void *value;
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}

		Attribute attribute = attributes.at(i);
		unsigned char nullIndicator;
		RID actualRid;
		switch(attribute.type){
		case TypeInt:
			value = malloc(sizeof(char) + sizeof(int));
			if(RecordBasedFileManager::instance()->readAttribute(fileHandle, recordDescriptor, rid, attribute.name, value, actualRid, version, page) == -1){
				free(value);
				return -1;
			}
			memset(&nullIndicator, 0, sizeof(char));
			memcpy(&nullIndicator, value, sizeof(char));
			if (getBit(nullIndicator, 0)){
	    		setNullBit(nullsIndicator[bytePosition], i % 8);
			} else {
				memcpy(data + offset, value + sizeof(char), sizeof(int));
				offset += sizeof(int);
			}
			free(value);
			break;
		case TypeReal:
			value = malloc(sizeof(float) + sizeof(char));
			if(RecordBasedFileManager::instance()->readAttribute(fileHandle, recordDescriptor, rid, attribute.name, value, actualRid, version, page) == -1){
				free(value);
				return -1;
			}
			memset(&nullIndicator, 0, sizeof(char));
			memcpy(&nullIndicator, value, sizeof(char));
			if (getBit(nullIndicator, 0)){
	    		setNullBit(nullsIndicator[bytePosition], i % 8);
			} else {
				memcpy(data + offset, value + sizeof(char), sizeof(float));
				offset += sizeof(float);
			}
			free(value);
			break;
		case TypeVarChar:
			value = malloc(sizeof(char) + sizeof(int) + attribute.length);
			memset(value, 0, sizeof(char) + sizeof(int) + attribute.length);
			if(RecordBasedFileManager::instance()->readAttribute(fileHandle, recordDescriptor, rid, attribute.name, value, actualRid, version, page) == -1){
				free(value);
				return -1;
			}
			memset(&nullIndicator, 0, sizeof(char));
			memcpy(&nullIndicator, value, sizeof(char));
			int varcharLength;
			memcpy(&varcharLength, value + sizeof(char), sizeof(int));
			if (getBit(nullIndicator, 0)){
	    		setNullBit(nullsIndicator[bytePosition], i % 8);
			} else {
				char varchar[varcharLength] = {0};
				memcpy(&varchar, value + sizeof(char) + sizeof(int), varcharLength);
				unsigned varcharSize = sizeof(int) + varcharLength * sizeof(char);
				memcpy(data + offset, &varcharLength, sizeof(int));
				memcpy(data + offset + sizeof(int), &varchar, varcharLength);
				offset += varcharSize;
			}
			free(value);
			break;
		default:
			return -1;
		}
	}
	memcpy(data, &nullsIndicator, nullFieldsIndicatorActualSize);
	return 0;
}

RC RecordBasedFileManager::createSlotDirectory(void *data){
	FreeSpaceOffset initialFreeSpace = 0;
	TotalSlots initialSlots = 0;
	setFreeSpaceOffset(data, initialFreeSpace);
	setTotalSlots(data, initialSlots);
	return 0;
}

RC RecordBasedFileManager::createRecordDirectory(const void *data, const vector<AttributeWithVersion> &recordDescriptor, void *formattedRecord, AttrVersion version){
	int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
	memcpy(&nullsIndicator, data, nullFieldsIndicatorActualSize);
	DirectorySize directorySize = sizeof(TombstoneBool) + sizeof(AttrVersion) + (recordDescriptor.size() + 1) * sizeof(RecordDirectorySlot);
	unsigned short recordDirectory[recordDescriptor.size() + 1];
	recordDirectory[0] = recordDescriptor.size();
	unsigned short offset = nullFieldsIndicatorActualSize;
	unsigned short formattedOffset = 0;
//	if (version == 1){
		for(int i = 0, bytePosition = 0; i < recordDescriptor.size(); ++i){
			AttributeWithVersion attribute = recordDescriptor.at(i);
			if (i != 0 && i % 8 == 0){
				++bytePosition;
			}
			if(getBit(nullsIndicator[bytePosition], i % 8)){
				recordDirectory[i + 1] = 65535;
			} else if (attribute.type == TypeInt){
				memcpy(formattedRecord + directorySize + formattedOffset, data + offset, sizeof(int));
				offset += sizeof(int);
				formattedOffset += sizeof(int);
				recordDirectory[i + 1] = formattedOffset;
			} else if (attribute.type == TypeReal){
				memcpy(formattedRecord + directorySize + formattedOffset, data + offset, sizeof(float));
				offset += sizeof(float);
				formattedOffset += sizeof(float);
				recordDirectory[i + 1] = formattedOffset;
			} else if (attribute.type == TypeVarChar){
				int varcharLength;
				memcpy(&varcharLength, data + offset, sizeof(int));
				offset += sizeof(int);
				memcpy(formattedRecord + directorySize + formattedOffset, data + offset, sizeof(char) * varcharLength);
				offset += sizeof(char) * varcharLength;
				formattedOffset += sizeof(char) * varcharLength;
				recordDirectory[i + 1] = formattedOffset;
			}

		}
////	} else {
//		vector<AttributeWithVersion> currentVersion;
//		for (AttributeWithVersion attribute : recordDescriptor){
//			if (attribute.version == version){
//				currentVersion.push_back(attribute);
//			}
//		}
//		std::sort(currentVersion.begin(), currentVersion.end(), comparePositions);
//		for(int i = 0, currentPosition = 0, bytePosition = 0; i < recordDescriptor.size(); ++i){
//			AttributeWithVersion attribute = recordDescriptor.at(currentPosition);
//			if (i != 0 && i % 8 == 0){
//				++bytePosition;
//			}
//			if (attribute.position - 1 != i){
//				recordDirectory[i] = 65535;
//				continue;
//			}
//			++currentPosition;
//			if(getBit(nullsIndicator[bytePosition], i % 8)){
//				recordDirectory[i + 1] = 65535;
//			} else if (attribute.type == TypeInt){
//				memcpy(formattedRecord + directorySize + formattedOffset, data + offset, sizeof(int));
//				offset += sizeof(int);
//				formattedOffset += sizeof(int);
//				recordDirectory[i + 1] = formattedOffset;
//			} else if (attribute.type == TypeReal){
//				memcpy(formattedRecord + directorySize + formattedOffset, data + offset, sizeof(float));
//				offset += sizeof(float);
//				formattedOffset += sizeof(float);
//				recordDirectory[i + 1] = formattedOffset;
//			} else if (attribute.type == TypeVarChar){
//				int varcharLength;
//				memcpy(&varcharLength, data + offset, sizeof(int));
//				offset += sizeof(int);
//				memcpy(formattedRecord + directorySize + formattedOffset, data + offset, sizeof(char) * varcharLength);
//				offset += sizeof(char) * varcharLength;
//				formattedOffset += sizeof(char) * varcharLength;
//				recordDirectory[i + 1] = formattedOffset;
//			}
//		}
//
//	}
	TombstoneBool tombstone = false;
	memcpy(formattedRecord, &tombstone, sizeof(TombstoneBool));
	//TODO ADD VERSIONING
	memcpy(formattedRecord + sizeof(TombstoneBool), &version, sizeof(AttrVersion));
	memcpy(formattedRecord + sizeof(TombstoneBool) + sizeof(AttrVersion), &recordDirectory, directorySize - sizeof(TombstoneBool) - sizeof(AttrVersion));

	return 0;
}

RC RecordBasedFileManager::convertFormattedRecordToData(const void *formattedRecord, const vector<AttributeWithVersion> &recordDescriptor, void *data, AttrVersion version){
	int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    DirectorySize directorySize = sizeof(TombstoneBool) + sizeof(AttrVersion) + (recordDescriptor.size() + 1) * sizeof(RecordDirectorySlot);
    unsigned short recordDirectory[recordDescriptor.size() + 1];
    memcpy(&recordDirectory, formattedRecord + sizeof(TombstoneBool) + sizeof(AttrVersion), directorySize - sizeof(TombstoneBool) - sizeof(AttrVersion));
    RecordSize formattedRecordSize = getFormattedRecordSize(recordDescriptor, formattedRecord);
    unsigned short previousDirectory = 65535;
    for (int i = 0, bytePosition = 0, offset = nullFieldsIndicatorActualSize; i < recordDescriptor.size(); ++i){
    	AttributeWithVersion attribute = recordDescriptor.at(i);
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
    	if (recordDirectory[i + 1] == 65535){
    		setNullBit(nullsIndicator[bytePosition], i % 8);
    		if (i == 0){
				previousDirectory = 0;
    		}
    	} else {
    		if(i == 0){
				if (attribute.type == TypeInt){
					memcpy(data + offset, formattedRecord + directorySize, sizeof(int));
					offset += sizeof(int);
				} else if (attribute.type == TypeReal){
					memcpy(data + offset, formattedRecord + directorySize, sizeof(float));
					offset += sizeof(float);
				} else if (attribute.type == TypeVarChar){
					int varcharLength;
					if(recordDescriptor.size() == 1){
						varcharLength = formattedRecordSize - directorySize;
					} else {
						bool restOfAttributesNull = true;
						for (int j = 1; i < recordDescriptor.size(); ++j){
							if (recordDirectory[j] != 65535){
								varcharLength = recordDirectory[j];
								restOfAttributesNull = false;
								break;
							}
						}
						if (restOfAttributesNull){
							varcharLength = formattedRecordSize - directorySize;
						}
					}
					memcpy(data + offset, &varcharLength, sizeof(int));
					offset += sizeof(int);
					memcpy(data + offset, formattedRecord + directorySize, sizeof(char) * varcharLength);
					offset += sizeof(char) * varcharLength;
					if (recordDirectory[i + 1] != 65535){
						previousDirectory = recordDirectory[i + 1];
					}
				}
    		} else {
    			unsigned short formattedOffset = recordDirectory[i];
    			if (formattedOffset == 65535){
    				if (previousDirectory == 65535){
    					formattedOffset = directorySize;
    				} else {
    					formattedOffset = previousDirectory;
    				}
    			}
				if (attribute.type == TypeInt){
					memcpy(data + offset, formattedRecord + directorySize + formattedOffset, sizeof(int));
					offset += sizeof(int);
				} else if (attribute.type == TypeReal){
					memcpy(data + offset, formattedRecord + directorySize + formattedOffset, sizeof(float));
					offset += sizeof(float);
				} else if (attribute.type == TypeVarChar){
					int varcharLength = 0;
					if (i == recordDirectory[0]){
						varcharLength = formattedRecordSize - formattedOffset - directorySize;
					} else {
						bool restOfAttributesNull = true;
						for (int j = i + 1; j < recordDescriptor.size(); ++j){
							if (recordDirectory[j] != 65535){
								varcharLength = recordDirectory[j] - formattedOffset;
								restOfAttributesNull = false;
								break;
							}
						}
						if (restOfAttributesNull){
							varcharLength = formattedRecordSize - formattedOffset - directorySize;
						}
					}
					memcpy(data + offset, &varcharLength, sizeof(int));
					offset += sizeof(int);
					memcpy(data + offset, formattedRecord + directorySize + formattedOffset, sizeof(char) * varcharLength);
					offset += sizeof(char) * (varcharLength);
				}
				if (recordDirectory[i + 1] != 65535){
					previousDirectory = recordDirectory[i + 1];
				}
    		}
    	}
    }
    memcpy(data, nullsIndicator, nullFieldsIndicatorActualSize);
    return 0;
}

RC RecordBasedFileManager::setFreeSpaceOffset(void *data, FreeSpaceOffset &value){
	FreeSpaceOffset offset = PAGE_SIZE - sizeof(FreeSpaceOffset);
	memcpy(data + offset, &value, sizeof(FreeSpaceOffset));
	return 0;
}

FreeSpaceOffset RecordBasedFileManager::getFreeSpaceOffset(const void *data){
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset);
	FreeSpaceOffset freeSpaceOffset;
	memcpy(&freeSpaceOffset, data + offset, sizeof(FreeSpaceOffset));
	return freeSpaceOffset;
}

RC RecordBasedFileManager::increaseFreeSpaceOffset(void *data, FreeSpaceOffset &value){
	FreeSpaceOffset freeSpace = getFreeSpaceOffset(data);
	freeSpace += value;
	setFreeSpaceOffset(data, freeSpace);
	return 0;
}

RC RecordBasedFileManager::decreaseFreeSpaceOffset(void *data, FreeSpaceOffset &value){
	FreeSpaceOffset freeSpace = getFreeSpaceOffset(data);
	freeSpace -= value;
	setFreeSpaceOffset(data, freeSpace);
	return 0;

}

RC RecordBasedFileManager::setTotalSlots(void *data, TotalSlots &value){
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots);
	memcpy(data + offset, &value, sizeof(TotalSlots));
	return 0;
}

TotalSlots RecordBasedFileManager::getTotalSlots(const void *data){
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots);
	TotalSlots totalSlots;
	memcpy(&totalSlots, data + offset, sizeof(TotalSlots));
	return totalSlots;
}

RC RecordBasedFileManager::incrementTotalSlots(void *data){
	TotalSlots totalSlots = getTotalSlots(data);
	totalSlots += 1;
	setTotalSlots(data, totalSlots);
	return 0;
}

RC RecordBasedFileManager::setSlotOffset(void *data, unsigned slotNumber, SlotOffset &value){
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(SlotOffset) - sizeof(SlotOffset) * slotNumber - sizeof(SlotLength) * slotNumber;
	memcpy(data + offset, &value, sizeof(SlotOffset));
	return 0;
}

SlotOffset RecordBasedFileManager::getSlotOffset(const void* data, unsigned slotNumber){
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(SlotOffset) - sizeof(SlotOffset) * slotNumber - sizeof(SlotLength) * slotNumber;
	SlotOffset slotOffset;
	memcpy(&slotOffset, data + offset, sizeof(SlotOffset));
	return slotOffset;
}

RC RecordBasedFileManager::setSlotLength(void *data, unsigned slotNumber, SlotLength &value){
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(SlotOffset) - sizeof(SlotLength) - sizeof(SlotOffset) * slotNumber - sizeof(SlotLength) * slotNumber;
	memcpy(data + offset, &value, sizeof(SlotLength));
	return 0;
}

SlotLength RecordBasedFileManager::getSlotLength(const void *data, unsigned slotNumber){
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(SlotOffset) - sizeof(SlotLength) - sizeof(SlotOffset) * slotNumber - sizeof(SlotLength) * slotNumber;
	SlotLength slotLength;
	memcpy(&slotLength, data + offset, sizeof(SlotLength));
	return slotLength;
}

RecordSize RecordBasedFileManager::getRecordSize(const vector<AttributeWithVersion> &recordDescriptor, const void *data){
	if (recordDescriptor.empty() || data == NULL){
		return -1;
	}
	int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
	memcpy(nullsIndicator, data, nullFieldsIndicatorActualSize);
	RecordSize offset = nullFieldsIndicatorActualSize;
	for (int i = 0, bytePosition = 0; i < recordDescriptor.size(); ++i){
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
		if(getBit(nullsIndicator[bytePosition], i % 8)){
			continue;
		}
		AttributeWithVersion attribute = recordDescriptor.at(i);
		if (attribute.type == TypeInt){
			offset += sizeof(int);
		} else if (attribute.type == TypeReal) {
			offset += sizeof(float);
		} else if (attribute.type == TypeVarChar){
			int varcharLength;
			memcpy(&varcharLength, data + offset, sizeof(int));
			offset += sizeof(int);
			offset += sizeof(char) * varcharLength;
		} else {
			return -1;
		}
	}
	return offset;
}

RecordSize RecordBasedFileManager::getFormattedRecordSize(const vector<AttributeWithVersion> &recordDescriptor, const void *data){
	if (recordDescriptor.empty() || data == NULL){
		return -1;
	}
	RecordSize recordSize = 0;
	DirectorySize directorySize = sizeof(TombstoneBool) + sizeof(AttrVersion) + (recordDescriptor.size() + 1) * sizeof(RecordDirectorySlot);
	recordSize += directorySize;
	RecordSize recordDataSize;
	memcpy(&recordDataSize, data + directorySize - sizeof(RecordDirectorySlot), sizeof(RecordDirectorySlot));
	recordSize += recordDataSize;
	return recordSize;
}

RecordSize RecordBasedFileManager::getFormattedRecordSizeFromData(const vector<AttributeWithVersion> &recordDescriptor, const void *data){
	int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
	memcpy(nullsIndicator, data, nullFieldsIndicatorActualSize);
	RecordSize recordSize = getRecordSize(recordDescriptor, data);
	recordSize -= nullFieldsIndicatorActualSize;
	for (int i = 0, bytePosition = 0; i < recordDescriptor.size(); ++i){
		AttributeWithVersion attribute = recordDescriptor.at(i);
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
		if (!getBit(nullsIndicator[bytePosition], i % 8) && attribute.type == TypeVarChar){
			recordSize -= sizeof(int);
		}
	}
	DirectorySize directorySize = sizeof(TombstoneBool) + sizeof(AttrVersion) + (recordDescriptor.size() + 1) * sizeof(RecordDirectorySlot);
	recordSize += directorySize;
	return recordSize;
}

RC RecordBasedFileManager::getTableId(unsigned int &tid, FileHandle &fileHandle){
	return fileHandle.getTableId(tid);
}

RC RecordBasedFileManager::setTableId(const unsigned int &tid, FileHandle &fileHandle){
	return fileHandle.setTableId(tid);
}

RC RecordBasedFileManager::incrementTableId(FileHandle &fileHandle){
	return fileHandle.incrementTableId();
}

void RecordBasedFileManager::createAttributesWithVersion(const vector<Attribute> &attributes, vector<AttributeWithVersion> &attributesWithVersion, AttrVersion version){
	for (Attribute attribute : attributes){
		AttributeWithVersion attributeWithVersion;
		attributeWithVersion.name = attribute.name;
		attributeWithVersion.type = attribute.type;
		attributeWithVersion.length = attribute.length;
		attributeWithVersion.version = version;
		attributesWithVersion.push_back(attributeWithVersion);
	}
}

RC RecordBasedFileManager::getRecordVersionWithLength(FileHandle &fileHandle, const RID &rid, AttrVersion &version, RecordDirectoryLength &length){
	void *page = malloc(PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, page) == -1){
		free(page);
		return -1;
	}
	SlotOffset slotOffset = getSlotOffset(page, rid.slotNum);
	if (slotOffset == 65535){
		free(page);
		return -1;
	}
	TombstoneBool tombstone;
	memcpy(&tombstone, page + slotOffset, sizeof(TombstoneBool));
	if (tombstone){
		RID tombstoneRid;
		memcpy(&tombstoneRid, page + slotOffset + sizeof(TombstoneBool), sizeof(RID));
		if (getRecordVersionWithLength(fileHandle, tombstoneRid, version, length) == -1){
			free(page);
			return -1;
		}
		free(page);
		return 0;
	}

	memcpy(&version, page + slotOffset + sizeof(TombstoneBool), sizeof(AttrVersion));
	memcpy(&length, page + slotOffset + sizeof(TombstoneBool) + sizeof(AttrVersion), sizeof(RecordDirectoryLength));
	free(page);
	return 0;
}

void setNullBit(unsigned char &byte, int position){
	const unsigned char option0 = 0x1; // hex for 0000 0001
	const unsigned char option1 = 0x2; // hex for 0000 0010
	const unsigned char option2 = 0x4; // hex for 0000 0100
	const unsigned char option3 = 0x8; // hex for 0000 1000
	const unsigned char option4 = 0x10; // hex for 0001 0000
	const unsigned char option5 = 0x20; // hex for 0010 0000
	const unsigned char option6 = 0x40; // hex for 0100 0000
	const unsigned char option7 = 0x80; // hex for 1000 0000
	position = abs(position - 7);
	if (position == 0){
		byte |= option0;
	} else if (position == 1){
		byte |= option1;
	} else if (position == 2){
		byte |= option2;
	} else if (position == 3){
		byte |= option3;
	} else if (position == 4){
		byte |= option4;
	} else if (position == 5){
		byte |= option5;
	} else if (position == 6){
		byte |= option6;
	} else if (position == 7){
		byte |= option7;
	}
}

bool getBit(const unsigned char byte, int position) {
	// position in range 0-7
	//reads bits from left to right
	position = abs(position - 7);
    return (byte >> position) & 0x1;
}

bool comparePositions(AttributeWithVersion attr1, AttributeWithVersion attr2) {
    return (attr1.position < attr2.position);
}
