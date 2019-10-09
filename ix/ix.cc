
#include "ix.h"
#include <cstring>
#include <cmath>
#include <iostream>
#include <limits>
IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
	if(_index_manager){
		delete _index_manager;
	}
}

void printKeyRid(const KeyRidPair &keyRid){
	char key[keyRid.keySize + 1] = {0};
	memcpy(&key, keyRid.key, keyRid.keySize);
	cout << "Key Size: " << keyRid.keySize << endl;
	cout << "Key: " << key << endl;
	cout << "RID: (" << keyRid.rid.pageNum << "," << keyRid.rid.slotNum << ")" << endl;
}

void printNodeKeyRid(const NodeKeyRidPair &keyRid){
	char key[keyRid.keySize + 1] = {0};
	memcpy(&key, keyRid.key, keyRid.keySize);
	cout << "Left Page: " << keyRid.leftPage << endl;
	cout << "Key Size: " << keyRid.keySize << endl;
	cout << "Key: " << key << endl;
	cout << "RID: (" << keyRid.rid.pageNum << "," << keyRid.rid.slotNum << ")" << endl;
	cout << "Right Page: " << keyRid.rightPage << endl;
}

RC IndexManager::createFile(const string &fileName)
{
	return PagedFileManager::instance()->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
	return PagedFileManager::instance()->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	return PagedFileManager::instance()->openFile(fileName, ixfileHandle.fileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	return PagedFileManager::instance()->closeFile(ixfileHandle.fileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	KeyRidPair keyRidPair;
	keyRidPair.keySize = 0;
	keyRidPair.rid = rid;
	switch(attribute.type){
	case TypeInt:
		keyRidPair.keySize = sizeof(int);
		keyRidPair.key = malloc(keyRidPair.keySize);
		memcpy(keyRidPair.key, key, keyRidPair.keySize);
		break;
	case TypeReal:
		keyRidPair.keySize = sizeof(float);
		keyRidPair.key = malloc(keyRidPair.keySize);
		memcpy(keyRidPair.key, key, keyRidPair.keySize);
		break;
	case TypeVarChar:
		memcpy(&keyRidPair.keySize, key, sizeof(int));
		keyRidPair.key = malloc(keyRidPair.keySize);
		memcpy(keyRidPair.key, key + sizeof(int), keyRidPair.keySize);
		break;
	case TypeDefault:
		return -1;
	default:
		return -1;
	}
	if (keyRidPair.keySize == 0){
		return -1;
	}
	unsigned short entrySize = getEntrySize(keyRidPair.keySize);
	void *entry = malloc(entrySize);
	createEntry(entry, keyRidPair);
	if (ixfileHandle.fileHandle.getNumberOfPages() == 0){
		void *page = malloc(PAGE_SIZE);
		initializeIndexPageDirectory(page, true);
		memcpy(page, entry, entrySize);
		increaseFreeSpaceOffset(page, entrySize);
		incrementTotalSlots(page);
		ixfileHandle.fileHandle.appendPage(page);
		ixfileHandle.fileHandle.setRootPageNumber(0);
		free(keyRidPair.key);
		free(page);
		free(entry);
		return 0;
	} else {
		if (insertEntryIntoTree(ixfileHandle, attribute, entry, entrySize) == -1){
			free(keyRidPair.key);
			free(entry);
			return -1;
		}
	}
	free(entry);
	free(keyRidPair.key);
    return 0;
}

RC IndexManager::insertEntryIntoTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *entry, const unsigned short &entrySize){
	stack<TraversedPage> traversedPages;
	KeyRidPair keyRid;
	keyRid.key = NULL;
	createKeyRidPair(keyRid, entry);
	if (navigateTree(ixfileHandle, keyRid, attribute, traversedPages) != 0 || traversedPages.empty()){
		if (keyRid.key){
			free(keyRid.key);
		}
		return -1;
	}
	NodeKeyRidPair pushedKeyRidPair;
	pushedKeyRidPair.key = NULL;
	RC rc = insertEntryIntoLeaf(ixfileHandle, traversedPages.top(), entry, entrySize, attribute, pushedKeyRidPair);
	if(rc == 0 || rc == -1){
		freeTraversedPageStack(traversedPages);
		if (keyRid.key){
			free(keyRid.key);
			keyRid.key = NULL;
		}
		if (pushedKeyRidPair.key){
			free(pushedKeyRidPair.key);
			pushedKeyRidPair.key = NULL;
		}
		return rc;
	} else {
		free(traversedPages.top().page);
		traversedPages.pop();
		unsigned short nodeEntrySize = getNodeEntrySize(pushedKeyRidPair.keySize);
		void *nodeEntry = malloc(nodeEntrySize);
		createNodeEntry(nodeEntry, pushedKeyRidPair);
		if (traversedPages.empty()){
			void *newRootPage = malloc(PAGE_SIZE);
			initializeIndexPageDirectory(newRootPage, false);
			memcpy(newRootPage, nodeEntry, nodeEntrySize);
			increaseFreeSpaceOffset(newRootPage, nodeEntrySize);
			incrementTotalSlots(newRootPage);
			PageNumber newRootPageNumber = ixfileHandle.fileHandle.getNumberOfPages();
			ixfileHandle.fileHandle.setRootPageNumber(newRootPageNumber);
			ixfileHandle.fileHandle.appendPage(newRootPage);
			free(nodeEntry);
			free(newRootPage);
			if (pushedKeyRidPair.key){
				free(pushedKeyRidPair.key);
				pushedKeyRidPair.key = NULL;
			}
			if (keyRid.key){
				free(keyRid.key);
				keyRid.key = NULL;
			}
			return 0;
		}
		while(rc == 1 && !traversedPages.empty()){
			rc = insertKeyIntoNode(ixfileHandle, traversedPages.top(), nodeEntry, nodeEntrySize, attribute, pushedKeyRidPair);
			free(traversedPages.top().page);
			traversedPages.pop();
			nodeEntrySize = getNodeEntrySize(pushedKeyRidPair.keySize);
			createNodeEntry(nodeEntry, pushedKeyRidPair);
		}
		if(rc == 1 && traversedPages.empty()){
			void *newRootPage = malloc(PAGE_SIZE);
			initializeIndexPageDirectory(newRootPage, false);
			memcpy(newRootPage, nodeEntry, nodeEntrySize);
			increaseFreeSpaceOffset(newRootPage, nodeEntrySize);
			incrementTotalSlots(newRootPage);
			PageNumber newRootPageNumber = ixfileHandle.fileHandle.getNumberOfPages();
			ixfileHandle.fileHandle.setRootPageNumber(newRootPageNumber);
			ixfileHandle.fileHandle.appendPage(newRootPage);
			free(newRootPage);
		}
		free(nodeEntry);
		freeTraversedPageStack(traversedPages);
	}
	if (keyRid.key){
		free(keyRid.key);
		keyRid.key = NULL;
	}
	if (pushedKeyRidPair.key){
		free(pushedKeyRidPair.key);
		pushedKeyRidPair.key = NULL;
	}
	return 0;
}


RC IndexManager::insertEntryIntoLeaf(IXFileHandle &ixfileHandle, TraversedPage traversedPage, const void *entry, const unsigned short &entrySize, const Attribute &attribute, NodeKeyRidPair &nodeKeyRid){
	if(!getIsLeafPage(traversedPage.page)){
		return -1;
	}
	if (getIndexPageFreeSpace(traversedPage.page) >= entrySize){
		TotalSlots totalSlots = getTotalSlots(traversedPage.page);
		FreeSpaceOffset freeSpaceOffset = getFreeSpaceOffset(traversedPage.page);
		if (totalSlots == 0){
			memcpy(traversedPage.page + freeSpaceOffset, entry, entrySize);
			increaseFreeSpaceOffset(traversedPage.page, entrySize);
			incrementTotalSlots(traversedPage.page);
			ixfileHandle.fileHandle.writePage(traversedPage.pageNumber, traversedPage.page);
			return 0;
		}
		KeyRidPair insertedKeyRid;
		createKeyRidPair(insertedKeyRid, entry);
		unsigned previousOffset = 0;
		KeySize keySize;
		void *compareEntry = malloc(PAGE_SIZE);
		memset(compareEntry, 0, PAGE_SIZE);
		unsigned nextOffset = 0;
		for (int i = 0; i < totalSlots; ++i){
			memcpy(&keySize, traversedPage.page + nextOffset, sizeof(KeySize));
			unsigned short slotEntrySize = getEntrySize(keySize);
			memcpy(compareEntry, traversedPage.page + nextOffset, slotEntrySize);
			nextOffset += slotEntrySize;
			KeyRidPair compareKeyRid;
			createKeyRidPair(compareKeyRid, compareEntry);
			int comparison = compareKeyRidPair(insertedKeyRid, compareKeyRid, attribute);
			if (comparison < 0){
				free(compareKeyRid.key);
				break;
			}
			if (comparison == 0){
				free(compareEntry);
				free(compareKeyRid.key);
				free(insertedKeyRid.key);
				return -1;
			}
			previousOffset +=  slotEntrySize;
			free(compareKeyRid.key);
		}
		free(compareEntry);
		shiftChunk(traversedPage.page, previousOffset, entrySize);
		memcpy(traversedPage.page + previousOffset, entry, entrySize);
		increaseFreeSpaceOffset(traversedPage.page, entrySize);
		incrementTotalSlots(traversedPage.page);
		ixfileHandle.fileHandle.writePage(traversedPage.pageNumber, traversedPage.page);
		free(insertedKeyRid.key);
		return 0;
	} else {
		void *rightPage = malloc(PAGE_SIZE);
		splitLeaf(traversedPage.page, rightPage, entry, entrySize, attribute, nodeKeyRid);
		RightPageNumber previousRightPageNumber = getRightPageNumber(traversedPage.page);
		RightPageNumber rightPageNumber = ixfileHandle.fileHandle.getNumberOfPages();
		setRightPageNumber(rightPage, previousRightPageNumber);
		setRightPageNumber(traversedPage.page, rightPageNumber);
		setLeftPageNumber(rightPage, traversedPage.pageNumber);
		nodeKeyRid.leftPage = traversedPage.pageNumber;
		nodeKeyRid.rightPage = rightPageNumber;
		ixfileHandle.fileHandle.writePage(nodeKeyRid.leftPage, traversedPage.page);
		ixfileHandle.fileHandle.appendPage(rightPage);
//		printBtreeDetails(ixfileHandle, true);
		free(rightPage);
		return 1;
	}
}

RC IndexManager::insertKeyIntoNode(IXFileHandle &ixfileHandle, TraversedPage traversedPage, const void *entry, const unsigned short &entrySize, const Attribute &attribute, NodeKeyRidPair &nodeKeyRid){
	if(getIsLeafPage(traversedPage.page)){
		return -1;
	}
	if (getIndexPageFreeSpace(traversedPage.page) >= entrySize){
		TotalSlots totalSlots = getTotalSlots(traversedPage.page);
		FreeSpaceOffset freeSpaceOffset = getFreeSpaceOffset(traversedPage.page);
		if (totalSlots == 0){
			memcpy(traversedPage.page + freeSpaceOffset, entry, entrySize);
			increaseFreeSpaceOffset(traversedPage.page, entrySize);
			incrementTotalSlots(traversedPage.page);
			ixfileHandle.fileHandle.writePage(traversedPage.pageNumber, traversedPage.page);
			return 0;
		}
		NodeKeyRidPair insertedNodeKeyRid;
		insertedNodeKeyRid.key = NULL;
		createNodeKeyRidPair(insertedNodeKeyRid, entry);
		unsigned previousOffset = 0;
		KeySize keySize;
		void *compareEntry = malloc(PAGE_SIZE);
		memset(compareEntry, 0, PAGE_SIZE);
		unsigned nextOffset = 0;
		for (int i = 0; i < totalSlots; ++i){
			memcpy(&keySize, traversedPage.page + nextOffset + sizeof(LeftPageNumber), sizeof(KeySize));
			unsigned short slotEntrySize = getNodeEntrySize(keySize);
			memcpy(compareEntry, traversedPage.page + nextOffset, slotEntrySize);
			nextOffset += slotEntrySize;
			NodeKeyRidPair compareNodeKeyRid;
			compareNodeKeyRid.key = NULL;
			createNodeKeyRidPair(compareNodeKeyRid, compareEntry);
			int comparison = compareKeyRidPair(insertedNodeKeyRid, compareNodeKeyRid, attribute);
			if (comparison == 0){
				free(compareEntry);
				if (compareNodeKeyRid.key){
					free(compareNodeKeyRid.key);
					compareNodeKeyRid.key = NULL;
				}
				if (insertedNodeKeyRid.key){
					free(insertedNodeKeyRid.key);
					insertedNodeKeyRid.key = NULL;
				}
				return -1;
			} else if (comparison < 0){
				if (compareNodeKeyRid.key){
					free(compareNodeKeyRid.key);
					compareNodeKeyRid.key = NULL;
				}
				break;
			}
			if (compareNodeKeyRid.key){
				free(compareNodeKeyRid.key);
				compareNodeKeyRid.key = NULL;
			}
			previousOffset +=  slotEntrySize;
		}
		shiftChunk(traversedPage.page, previousOffset, entrySize);
		memcpy(traversedPage.page + previousOffset, entry, entrySize);
		if (previousOffset != 0){
			memcpy(traversedPage.page + previousOffset - sizeof(RightPageNumber), &insertedNodeKeyRid.leftPage, sizeof(RightPageNumber));
		}
		if (previousOffset != freeSpaceOffset){
			memcpy(traversedPage.page + previousOffset + entrySize, &insertedNodeKeyRid.rightPage, sizeof(LeftPageNumber));
		}
		increaseFreeSpaceOffset(traversedPage.page, entrySize);
		incrementTotalSlots(traversedPage.page);
		ixfileHandle.fileHandle.writePage(traversedPage.pageNumber, traversedPage.page);
		free(compareEntry);
		if (insertedNodeKeyRid.key){
			free(insertedNodeKeyRid.key);
			insertedNodeKeyRid.key = NULL;
		}
		return 0;
	} else {
		void *rightPage = malloc(PAGE_SIZE);
		splitNode(traversedPage.page, rightPage, entry, entrySize, attribute, nodeKeyRid);
		RightPageNumber previousRightPageNumber = getRightPageNumber(traversedPage.page);
		RightPageNumber rightPageNumber = ixfileHandle.fileHandle.getNumberOfPages();
		setRightPageNumber(rightPage, previousRightPageNumber);
		setRightPageNumber(traversedPage.page, rightPageNumber);
		setLeftPageNumber(rightPage, traversedPage.pageNumber);
		nodeKeyRid.leftPage = traversedPage.pageNumber;
		nodeKeyRid.rightPage = rightPageNumber;
		ixfileHandle.fileHandle.writePage(nodeKeyRid.leftPage, traversedPage.page);
		ixfileHandle.fileHandle.appendPage(rightPage);
		free(rightPage);
		return 1;
	}
}

RC IndexManager::shiftChunk(void *page, const unsigned short &startOffset, const short &offset){
	//Shift chunk left if negative offset, right if positive offset
	FreeSpaceOffset freeSpaceOffset = getFreeSpaceOffset(page);
	return shiftChunk(page, startOffset, offset, freeSpaceOffset);
}

RC IndexManager::shiftChunk(void *page, const unsigned short &startOffset, const short &offset, const FreeSpaceOffset &freeSpaceOffset){
	unsigned short chunkSize = freeSpaceOffset - startOffset;
	if (chunkSize != 0){
		void *chunk = malloc(chunkSize);
		memcpy(chunk, page + startOffset, chunkSize);
		unsigned short newOffset = startOffset + offset;
		memcpy(page + newOffset, chunk, chunkSize);
		if(newOffset < startOffset){
			memset(page + newOffset + chunkSize, 0, -1 * offset);
		}
		free(chunk);
	}
	return 0;
}

RC IndexManager::splitLeaf(void *leftPage, void *rightPage, const void *insertedEntry, const unsigned short &entrySize, const Attribute &attribute, NodeKeyRidPair &nodeKeyRid){
	TotalSlots totalSlots = getTotalSlots(leftPage);
	FreeSpaceOffset freeSpaceOffset = getFreeSpaceOffset(leftPage);
	unsigned short totalSize = freeSpaceOffset + entrySize;
	void *sortedOverflowLeaf = malloc(totalSize);
	memcpy(sortedOverflowLeaf, leftPage, freeSpaceOffset);
	KeyRidPair insertedKeyRid;
	insertedKeyRid.key = NULL;
	createKeyRidPair(insertedKeyRid, insertedEntry);
	unsigned previousOffset = 0;
	KeySize keySize;
	void *compareEntry = malloc(PAGE_SIZE);
	memset(compareEntry, 0, PAGE_SIZE);
	unsigned nextOffset = 0;
	for (int i = 0; i < totalSlots; ++i){
		memcpy(&keySize, sortedOverflowLeaf + nextOffset, sizeof(KeySize));
		unsigned short slotEntrySize = getEntrySize(keySize);
		memcpy(compareEntry, sortedOverflowLeaf + nextOffset, slotEntrySize);
		nextOffset += slotEntrySize;
		KeyRidPair compareKeyRid;
		compareKeyRid.key = NULL;
		createKeyRidPair(compareKeyRid, compareEntry);
		int comparison = compareKeyRidPair(insertedKeyRid, compareKeyRid, attribute);
		if (comparison < 0){
			if (compareKeyRid.key){
				free(compareKeyRid.key);
			}
			break;
		}
		previousOffset +=  slotEntrySize;
		if (compareKeyRid.key){
			free(compareKeyRid.key);
		}
	}
	free(compareEntry);
	shiftChunk(sortedOverflowLeaf, previousOffset, entrySize, freeSpaceOffset);
	memcpy(sortedOverflowLeaf + previousOffset, insertedEntry, entrySize);
	++totalSlots;
	TotalSlots leftPageTotalSlots = 0;
	PageNumber leftPageLeft = getLeftPageNumber(leftPage);
	PageNumber leftPageRight = getRightPageNumber(leftPage);
	initializeIndexPageDirectory(leftPage, true);
	setLeftPageNumber(leftPage, leftPageLeft);
	setRightPageNumber(leftPage, leftPageRight);
	initializeIndexPageDirectory(rightPage, true);
	unsigned short leftPageFreeSpaceOffset = 0;
	unsigned short slotEntrySize = getEntrySize(keySize);
	while(leftPageFreeSpaceOffset < ceil(getIndexPageFreeSpace(leftPage) / 2)){
		memcpy(&keySize, sortedOverflowLeaf + leftPageFreeSpaceOffset, sizeof(KeySize));
		slotEntrySize = getEntrySize(keySize);
		leftPageFreeSpaceOffset += slotEntrySize;
		++leftPageTotalSlots;
	}
	unsigned short rightPageFreeSpaceOffset = totalSize - leftPageFreeSpaceOffset;
	TotalSlots rightPageTotalSlots = totalSlots - leftPageTotalSlots;
	if (rightPageFreeSpaceOffset < ceil(getIndexPageFreeSpace(leftPage) / 2)){
		leftPageFreeSpaceOffset -= slotEntrySize;
		rightPageFreeSpaceOffset += slotEntrySize;
		leftPageTotalSlots -= 1;
		rightPageTotalSlots += 1;
	}
	if (leftPageFreeSpaceOffset + rightPageFreeSpaceOffset != totalSize){
		cout << "Left Page + Right Page != Total Size" << endl;
		if (insertedKeyRid.key){
			free(insertedKeyRid.key);
		}
		free(sortedOverflowLeaf);
		return -1;
	}

	memcpy(leftPage, sortedOverflowLeaf, leftPageFreeSpaceOffset);
	increaseFreeSpaceOffset(leftPage, leftPageFreeSpaceOffset);
	setTotalSlots(leftPage, leftPageTotalSlots);
	memcpy(rightPage, sortedOverflowLeaf + leftPageFreeSpaceOffset, rightPageFreeSpaceOffset);
	increaseFreeSpaceOffset(rightPage, rightPageFreeSpaceOffset);
	setTotalSlots(rightPage, rightPageTotalSlots);
	KeySize copyKeySize;
	memcpy(&copyKeySize, rightPage, sizeof(KeySize));
	unsigned short copyEntrySize = getEntrySize(copyKeySize);
	void *copyEntry = malloc(copyEntrySize);
	memcpy(copyEntry, rightPage, copyEntrySize);
	if (nodeKeyRid.key){
		free(nodeKeyRid.key);
		nodeKeyRid.key = NULL;
	}
	createKeyRidPair(nodeKeyRid, copyEntry);
	free(copyEntry);
	free(sortedOverflowLeaf);
	if (insertedKeyRid.key){
		free(insertedKeyRid.key);
	}
	return 0;
}

RC IndexManager::splitNode(void *leftPage, void *rightPage, const void *insertedEntry, const unsigned short &entrySize, const Attribute &attribute, NodeKeyRidPair &nodeKeyRid){
	TotalSlots totalSlots = getTotalSlots(leftPage);
	FreeSpaceOffset freeSpaceOffset = getFreeSpaceOffset(leftPage);
	unsigned short totalSize = freeSpaceOffset + entrySize;
	void *sortedOverflowNode = malloc(totalSize);
	memcpy(sortedOverflowNode, leftPage, freeSpaceOffset);
	NodeKeyRidPair insertedNodeKeyRid;
	insertedNodeKeyRid.key = NULL;
	createNodeKeyRidPair(insertedNodeKeyRid, insertedEntry);
//	printNodeKeyRid(insertedNodeKeyRid);
	unsigned previousOffset = 0;
	KeySize keySize;
	void *compareEntry = malloc(PAGE_SIZE);
	memset(compareEntry, 0, PAGE_SIZE);
	unsigned nextOffset = 0;
	for (int i = 0; i < totalSlots; ++i){
		memcpy(&keySize, sortedOverflowNode + nextOffset + sizeof(LeftPageNumber), sizeof(KeySize));
		unsigned short slotEntrySize = getNodeEntrySize(keySize);
		memcpy(compareEntry, sortedOverflowNode + nextOffset, slotEntrySize);
		nextOffset += slotEntrySize;
		NodeKeyRidPair compareNodeKeyRid;
		compareNodeKeyRid.key = NULL;
		createNodeKeyRidPair(compareNodeKeyRid, compareEntry);
//		printNodeKeyRid(compareNodeKeyRid);
		int comparison = compareKeyRidPair(insertedNodeKeyRid, compareNodeKeyRid, attribute);
		if (comparison < 0){
			if(compareNodeKeyRid.key){
				free(compareNodeKeyRid.key);
				compareNodeKeyRid.key = NULL;
			}
			break;
		}
		previousOffset +=  slotEntrySize;
		if(compareNodeKeyRid.key){
			free(compareNodeKeyRid.key);
			compareNodeKeyRid.key = NULL;
		}
	}
	free(compareEntry);
	shiftChunk(sortedOverflowNode, previousOffset, entrySize, freeSpaceOffset);
	memcpy(sortedOverflowNode + previousOffset, insertedEntry, entrySize);
	if (previousOffset != 0){
		memcpy(sortedOverflowNode + previousOffset - sizeof(RightPageNumber), &insertedNodeKeyRid.leftPage, sizeof(RightPageNumber));
	}
	if (previousOffset != freeSpaceOffset){
		memcpy(sortedOverflowNode + previousOffset + entrySize, &insertedNodeKeyRid.rightPage, sizeof(LeftPageNumber));
	}
	TotalSlots leftPageTotalSlots = 0;
	initializeIndexPageDirectory(leftPage, false);
	initializeIndexPageDirectory(rightPage, false);
	unsigned short leftPageFreeSpaceOffset = 0;
	unsigned short slotEntrySize;
	while(leftPageFreeSpaceOffset < ceil(getIndexPageFreeSpace(leftPage) / 2)){
		memcpy(&keySize, sortedOverflowNode + leftPageFreeSpaceOffset + sizeof(LeftPageNumber), sizeof(KeySize));
		slotEntrySize = getNodeEntrySize(keySize);
		leftPageFreeSpaceOffset += slotEntrySize;
		++leftPageTotalSlots;
	}
	unsigned short rightPageFreeSpaceOffset = totalSize - leftPageFreeSpaceOffset;
	TotalSlots rightPageTotalSlots = totalSlots - leftPageTotalSlots;
	if (rightPageFreeSpaceOffset < ceil(getIndexPageFreeSpace(leftPage) / 2)){
		leftPageFreeSpaceOffset -= slotEntrySize;
		rightPageFreeSpaceOffset += slotEntrySize;
		leftPageTotalSlots -= 1;
		rightPageTotalSlots += 1;
	}
	memcpy(leftPage, sortedOverflowNode, leftPageFreeSpaceOffset);
	increaseFreeSpaceOffset(leftPage, leftPageFreeSpaceOffset);
	setTotalSlots(leftPage, leftPageTotalSlots);
	KeySize pushKeySize;
	memcpy(&pushKeySize, sortedOverflowNode + leftPageFreeSpaceOffset + sizeof(LeftPageNumber), sizeof(KeySize));
	unsigned short pushEntrySize = getNodeEntrySize(pushKeySize);
	void *pushEntry = malloc(pushEntrySize);
	rightPageFreeSpaceOffset -= pushEntrySize;
	memcpy(rightPage, sortedOverflowNode + leftPageFreeSpaceOffset + pushEntrySize, rightPageFreeSpaceOffset);
	increaseFreeSpaceOffset(rightPage, rightPageFreeSpaceOffset);
	setTotalSlots(rightPage, rightPageTotalSlots);
	memcpy(pushEntry, sortedOverflowNode + leftPageFreeSpaceOffset, pushEntrySize);
	if (nodeKeyRid.key){
		free(nodeKeyRid.key);
		nodeKeyRid.key = NULL;
	}
	createNodeKeyRidPair(nodeKeyRid, pushEntry);
//	printNodeKeyRid(nodeKeyRid);
	if (insertedNodeKeyRid.key){
		free(insertedNodeKeyRid.key);
		insertedNodeKeyRid.key = NULL;
	}
	free(pushEntry);
	free(sortedOverflowNode);
	return 0;
}

RC IndexManager::navigateTree(IXFileHandle &ixfileHandle, const KeyRidPair &keyRid, const Attribute &attribute, stack<TraversedPage> &traversedPages){
	TraversedPage traversedPage;
	traversedPage.page = malloc(PAGE_SIZE);
	unsigned rootPageNumber = 0;
	ixfileHandle.fileHandle.getRootPageNumber(rootPageNumber);
	ixfileHandle.fileHandle.readPage(rootPageNumber, traversedPage.page);
	traversedPage.pageNumber = rootPageNumber;
	traversedPages.push(traversedPage);
	while(!getIsLeafPage(traversedPage.page)){
		PageNumber previousPageNumber = traversedPage.pageNumber;
		TotalSlots totalSlots = getTotalSlots(traversedPages.top().page);
		unsigned offset = 0;
		NodeKeyRidPair nodeKeyRid;
		nodeKeyRid.key = NULL;
		for (int i = 0; i < totalSlots; ++i){
			memcpy(&nodeKeyRid.leftPage, traversedPage.page + offset, sizeof(LeftPageNumber));
			offset += sizeof(LeftPageNumber);
			memcpy(&nodeKeyRid.keySize, traversedPage.page + offset, sizeof(KeySize));
			offset += sizeof(KeySize);
			nodeKeyRid.key = malloc(nodeKeyRid.keySize);
			memcpy(nodeKeyRid.key, traversedPage.page + offset, nodeKeyRid.keySize);
			offset += nodeKeyRid.keySize;
			memcpy(&nodeKeyRid.rid, traversedPage.page + offset, sizeof(RID));
			offset += sizeof(RID);
			memcpy(&nodeKeyRid.rightPage, traversedPage.page + offset, sizeof(RightPageNumber));
			offset += sizeof(RightPageNumber);
			int comparison = compareKeyRidPair(keyRid, nodeKeyRid, attribute);
			if (totalSlots == 1 || i == totalSlots - 1){
				if (comparison >= 0){
					traversedPage.pageNumber = nodeKeyRid.rightPage;
					traversedPage.page = malloc(PAGE_SIZE);
					ixfileHandle.fileHandle.readPage(traversedPage.pageNumber, traversedPage.page);
					traversedPages.push(traversedPage);
					if (nodeKeyRid.key){
						free(nodeKeyRid.key);
						nodeKeyRid.key = NULL;
					}
					break;
				} else if (comparison < 0){
					traversedPage.pageNumber = nodeKeyRid.leftPage;
					traversedPage.page = malloc(PAGE_SIZE);
					ixfileHandle.fileHandle.readPage(traversedPage.pageNumber, traversedPage.page);
					traversedPages.push(traversedPage);
					if (nodeKeyRid.key){
						free(nodeKeyRid.key);
						nodeKeyRid.key = NULL;
					}
					break;
				}
			}
			if (comparison == 0){
				traversedPage.pageNumber = nodeKeyRid.rightPage;
				traversedPage.page = malloc(PAGE_SIZE);
				ixfileHandle.fileHandle.readPage(traversedPage.pageNumber, traversedPage.page);
				traversedPages.push(traversedPage);
				if (nodeKeyRid.key){
					free(nodeKeyRid.key);
					nodeKeyRid.key = NULL;
				}
				break;
			} else if (comparison < 0){
				traversedPage.pageNumber = nodeKeyRid.leftPage;
				traversedPage.page = malloc(PAGE_SIZE);
				ixfileHandle.fileHandle.readPage(traversedPage.pageNumber, traversedPage.page);
				traversedPages.push(traversedPage);
				if (nodeKeyRid.key){
					free(nodeKeyRid.key);
					nodeKeyRid.key = NULL;
				}
				break;
			}
			if (nodeKeyRid.key){
				free(nodeKeyRid.key);
				nodeKeyRid.key = NULL;
			}
		}
		if (traversedPages.top().pageNumber == previousPageNumber){
			if(nodeKeyRid.key){
				free(nodeKeyRid.key);
				nodeKeyRid.key = NULL;
			}
			break;
		}
	}
	if(!getIsLeafPage(traversedPage.page)){
		return -1;
	}
	return 0;
}

RC IndexManager::navigateTree(IXFileHandle &ixfileHandle, const KeyRidPair &keyRid, const Attribute &attribute, TraversedPage &traversedPage){
	traversedPage.page = malloc(PAGE_SIZE);
	unsigned rootPageNumber = 0;
	ixfileHandle.fileHandle.getRootPageNumber(rootPageNumber);
	ixfileHandle.fileHandle.readPage(rootPageNumber, traversedPage.page);
	traversedPage.pageNumber = rootPageNumber;
	while(!getIsLeafPage(traversedPage.page)){
		PageNumber previousPageNumber = traversedPage.pageNumber;
		TotalSlots totalSlots = getTotalSlots(traversedPage.page);
		unsigned offset = 0;
		NodeKeyRidPair nodeKeyRid;
		nodeKeyRid.key = NULL;
		for (int i = 0; i < totalSlots; ++i){
			memcpy(&nodeKeyRid.leftPage, traversedPage.page + offset, sizeof(LeftPageNumber));
			offset += sizeof(LeftPageNumber);
			memcpy(&nodeKeyRid.keySize, traversedPage.page + offset, sizeof(KeySize));
			offset += sizeof(KeySize);
			nodeKeyRid.key = malloc(nodeKeyRid.keySize);
			memcpy(nodeKeyRid.key, traversedPage.page + offset, nodeKeyRid.keySize);
			offset += nodeKeyRid.keySize;
			memcpy(&nodeKeyRid.rid, traversedPage.page + offset, sizeof(RID));
			offset += sizeof(RID);
			memcpy(&nodeKeyRid.rightPage, traversedPage.page + offset, sizeof(RightPageNumber));
			offset += sizeof(RightPageNumber);
			int comparison = compareKeyRidPair(keyRid, nodeKeyRid, attribute);
			if (totalSlots == 1 || i == totalSlots - 1){
				if (comparison >= 0){
					traversedPage.pageNumber = nodeKeyRid.rightPage;
					ixfileHandle.fileHandle.readPage(traversedPage.pageNumber, traversedPage.page);
					if(nodeKeyRid.key){
						free(nodeKeyRid.key);
						nodeKeyRid.key = NULL;
					}
					break;
				} else if (comparison < 0){
					traversedPage.pageNumber = nodeKeyRid.leftPage;
					ixfileHandle.fileHandle.readPage(traversedPage.pageNumber, traversedPage.page);
					if(nodeKeyRid.key){
						free(nodeKeyRid.key);
						nodeKeyRid.key = NULL;
					}
					break;
				}
			}
			if (comparison == 0){
				traversedPage.pageNumber = nodeKeyRid.rightPage;
				ixfileHandle.fileHandle.readPage(traversedPage.pageNumber, traversedPage.page);
				if(nodeKeyRid.key){
					free(nodeKeyRid.key);
					nodeKeyRid.key = NULL;
				}
				break;
			} else if (comparison < 0){
				traversedPage.pageNumber = nodeKeyRid.leftPage;
				ixfileHandle.fileHandle.readPage(traversedPage.pageNumber, traversedPage.page);
				if(nodeKeyRid.key){
					free(nodeKeyRid.key);
					nodeKeyRid.key = NULL;
				}
				break;
			}
			if(nodeKeyRid.key){
				free(nodeKeyRid.key);
				nodeKeyRid.key = NULL;
			}
		}
		if (traversedPage.pageNumber == previousPageNumber){
			if(nodeKeyRid.key){
				free(nodeKeyRid.key);
				nodeKeyRid.key = NULL;
			}
			break;
		}
	}
	if(!getIsLeafPage(traversedPage.page)){
		return -1;
	}
	return 0;
}

RC IndexManager::navigateTreeLeft(IXFileHandle &ixfileHandle, TraversedPage &traversedPage){
	traversedPage.page = malloc(PAGE_SIZE);
	unsigned rootPageNumber = 0;
	ixfileHandle.fileHandle.getRootPageNumber(rootPageNumber);
	ixfileHandle.fileHandle.readPage(rootPageNumber, traversedPage.page);
	traversedPage.pageNumber = rootPageNumber;
	while(!getIsLeafPage(traversedPage.page)){
		TotalSlots totalSlots = getTotalSlots(traversedPage.page);
		if (totalSlots == 0){
			return -1;
		}
		memcpy(&traversedPage.pageNumber, traversedPage.page, sizeof(LeftPageNumber));
		ixfileHandle.fileHandle.readPage(traversedPage.pageNumber, traversedPage.page);
	}
	if(!getIsLeafPage(traversedPage.page)){
		return -1;
	}
	return 0;
}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	if(ixfileHandle.fileHandle.getNumberOfPages() == 0){
		return -1;
	}

	KeyRidPair keyRidPair;
	keyRidPair.keySize = 0;
	keyRidPair.rid = rid;
	keyRidPair.key = NULL;
	switch(attribute.type){
	case TypeInt:
		keyRidPair.keySize = sizeof(int);
		keyRidPair.key = malloc(keyRidPair.keySize);
		memcpy(keyRidPair.key, key, keyRidPair.keySize);
		break;
	case TypeReal:
		keyRidPair.keySize = sizeof(float);
		keyRidPair.key = malloc(keyRidPair.keySize);
		memcpy(keyRidPair.key, key, keyRidPair.keySize);
		break;
	case TypeVarChar:
		memcpy(&keyRidPair.keySize, key, sizeof(int));
		keyRidPair.key = malloc(keyRidPair.keySize);
		memcpy(keyRidPair.key, key + sizeof(int), keyRidPair.keySize);
		break;
	case TypeDefault:
		return -1;
	default:
		return -1;
	}

	unsigned short entrySize = getEntrySize(keyRidPair.keySize);
	TraversedPage traversedPage;
	traversedPage.page = NULL;
	if(navigateTree(ixfileHandle, keyRidPair, attribute, traversedPage) == -1){
		if(traversedPage.page){
			free(traversedPage.page);
			traversedPage.page = NULL;
		}
		if (keyRidPair.key){
			free(keyRidPair.key);
			keyRidPair.key = NULL;
		}
		return -1;
	}
	TotalSlots totalSlots = getTotalSlots(traversedPage.page);
	FreeSpaceOffset freeSpaceOffset = getFreeSpaceOffset(traversedPage.page);
	if (totalSlots == 0){
		if(traversedPage.page){
			free(traversedPage.page);
			traversedPage.page = NULL;
		}
		if (keyRidPair.key){
			free(keyRidPair.key);
			keyRidPair.key = NULL;
		}
		return -1;
	}
	KeySize keySize;
	void *compareEntry = malloc(PAGE_SIZE);
	memset(compareEntry, 0, PAGE_SIZE);
	unsigned offset = 0;
	bool found = false;
	for (int i = 0; i < totalSlots; ++i){
		memcpy(&keySize, traversedPage.page + offset, sizeof(KeySize));
		unsigned short slotEntrySize = getEntrySize(keySize);
		memcpy(compareEntry, traversedPage.page + offset, slotEntrySize);
		KeyRidPair compareKeyRid;
		compareKeyRid.key = NULL;
		createKeyRidPair(compareKeyRid, compareEntry);
		int comparison = compareKeyRidPair(keyRidPair, compareKeyRid, attribute);
		if (comparison == 0){
			found = true;
			if(compareKeyRid.key){
				free(compareKeyRid.key);
				compareKeyRid.key = NULL;
			}
			break;
		}
		offset +=  slotEntrySize;
		if(compareKeyRid.key){
			free(compareKeyRid.key);
			compareKeyRid.key = NULL;
		}
	}
	free(compareEntry);
	if(!found){
		if (keyRidPair.key){
			free(keyRidPair.key);
			keyRidPair.key = NULL;
		}
		if(traversedPage.page){
			free(traversedPage.page);
			traversedPage.page = NULL;
		}
		return -1;
	}
	if(offset + entrySize == freeSpaceOffset){
		memset(traversedPage.page + offset, 0, entrySize);
	} else {
		shiftChunk(traversedPage.page, offset + entrySize, -1 * entrySize);
	}
	decreaseFreeSpaceOffset(traversedPage.page, entrySize);
	decrementTotalSlots(traversedPage.page);
	ixfileHandle.fileHandle.writePage(traversedPage.pageNumber, traversedPage.page);
	if(traversedPage.page){
		free(traversedPage.page);
		traversedPage.page = NULL;
	}
	if (keyRidPair.key){
		free(keyRidPair.key);
		keyRidPair.key = NULL;
	}
	return 0;
}

RC IndexManager::createEntry(void *entry, const KeyRidPair &keyRid){
	unsigned offset = 0;
	memcpy(entry + offset, &keyRid.keySize, sizeof(KeySize));
	offset += sizeof(KeySize);
	memcpy(entry + offset, keyRid.key, keyRid.keySize);
	offset += keyRid.keySize;
	memcpy(entry + offset, &keyRid.rid, sizeof(RID));
	return 0;
}

RC IndexManager::createKeyRidPair(KeyRidPair &keyRid, const void *entry) const{
	unsigned offset = 0;
	memcpy(&keyRid.keySize, entry + offset, sizeof(KeySize));
	offset += sizeof(KeySize);
	keyRid.key = malloc(keyRid.keySize);
	memcpy(keyRid.key, entry + offset, keyRid.keySize);
	offset += keyRid.keySize;
	memcpy(&keyRid.rid, entry + offset, sizeof(RID));
	return 0;
}

RC IndexManager::createNodeEntry(void *entry, const NodeKeyRidPair &nodeKeyRid){
	unsigned offset = 0;
	memcpy(entry + offset, &nodeKeyRid.leftPage, sizeof(LeftPageNumber));
	offset += sizeof(LeftPageNumber);
	memcpy(entry + offset, &nodeKeyRid.keySize, sizeof(KeySize));
	offset += sizeof(KeySize);
	memcpy(entry + offset, nodeKeyRid.key, nodeKeyRid.keySize);
	offset += nodeKeyRid.keySize;
	memcpy(entry + offset, &nodeKeyRid.rid, sizeof(RID));
	offset += sizeof(RID);
	memcpy(entry + offset, &nodeKeyRid.rightPage, sizeof(RightPageNumber));
	return 0;
}

RC IndexManager::createNodeKeyRidPair(NodeKeyRidPair &nodeKeyRid, const void *entry) const{
	unsigned offset = 0;
	memcpy(&nodeKeyRid.leftPage, entry + offset, sizeof(LeftPageNumber));
	offset += sizeof(LeftPageNumber);
	memcpy(&nodeKeyRid.keySize, entry + offset, sizeof(KeySize));
	offset += sizeof(KeySize);
	nodeKeyRid.key = malloc(nodeKeyRid.keySize);
	memcpy(nodeKeyRid.key, entry + offset, nodeKeyRid.keySize);
	offset += nodeKeyRid.keySize;
	memcpy(&nodeKeyRid.rid, entry + offset, sizeof(RID));
	offset += sizeof(RID);
	memcpy(&nodeKeyRid.rightPage, entry + offset, sizeof(RightPageNumber));
	return 0;
}

RC IndexManager::initializeIndexPageDirectory(void *data, IsLeafPage isLeafPage){
	memset(data, 0, PAGE_SIZE);
	FreeSpaceOffset initialFreeSpace = 0;
	TotalSlots initialSlots = 0;
	LeftPageNumber leftPageNumber = USHRT_MAX;
	RightPageNumber rightPageNumber = USHRT_MAX;
	setFreeSpaceOffset(data, initialFreeSpace);
	setTotalSlots(data, initialSlots);
	setLeftPageNumber(data, leftPageNumber);
	setRightPageNumber(data, rightPageNumber);
	setIsLeafPage(data, isLeafPage);
	return 0;
}

RC IndexManager::setFreeSpaceOffset(void *data, FreeSpaceOffset &value){
	FreeSpaceOffset offset = PAGE_SIZE - sizeof(FreeSpaceOffset);
	memcpy(data + offset, &value, sizeof(FreeSpaceOffset));
	return 0;
}

FreeSpaceOffset IndexManager::getFreeSpaceOffset(const void *data) const{
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset);
	FreeSpaceOffset freeSpaceOffset;
	memcpy(&freeSpaceOffset, data + offset, sizeof(FreeSpaceOffset));
	return freeSpaceOffset;
}

RC IndexManager::increaseFreeSpaceOffset(void *data, const FreeSpaceOffset &value){
	FreeSpaceOffset freeSpace = getFreeSpaceOffset(data);
	freeSpace += value;
	setFreeSpaceOffset(data, freeSpace);
	return 0;
}

RC IndexManager::decreaseFreeSpaceOffset(void *data, const FreeSpaceOffset &value){
	FreeSpaceOffset freeSpace = getFreeSpaceOffset(data);
	freeSpace -= value;
	setFreeSpaceOffset(data, freeSpace);
	return 0;
}

RC IndexManager::setTotalSlots(void *data, TotalSlots &value){
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots);
	memcpy(data + offset, &value, sizeof(TotalSlots));
	return 0;
}

TotalSlots IndexManager::getTotalSlots(const void *data) const{
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots);
	TotalSlots totalSlots;
	memcpy(&totalSlots, data + offset, sizeof(TotalSlots));
	return totalSlots;
}

RC IndexManager::incrementTotalSlots(void *data){
	TotalSlots totalSlots = getTotalSlots(data);
	totalSlots += 1;
	setTotalSlots(data, totalSlots);
	return 0;
}

RC IndexManager::decrementTotalSlots(void *data){
	TotalSlots totalSlots = getTotalSlots(data);
	totalSlots -= 1;
	setTotalSlots(data, totalSlots);
	return 0;
}

RC IndexManager::setLeftPageNumber(void *data, LeftPageNumber &value){
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(RightPageNumber) - sizeof(LeftPageNumber);
	memcpy(data + offset, &value, sizeof(LeftPageNumber));
	return 0;
}

RC IndexManager::setRightPageNumber(void *data, RightPageNumber &value){
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(RightPageNumber);
	memcpy(data + offset, &value, sizeof(RightPageNumber));
	return 0;
}

RC IndexManager::setIsLeafPage(void *data, IsLeafPage &value){
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(RightPageNumber) - sizeof(LeftPageNumber) - sizeof(IsLeafPage);
	memcpy(data + offset, &value, sizeof(IsLeafPage));
	return 0;
}

LeftPageNumber IndexManager::getLeftPageNumber(const void *data) const{
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(RightPageNumber) - sizeof(LeftPageNumber);
	LeftPageNumber leftPageNumber;
	memcpy(&leftPageNumber, data + offset, sizeof(LeftPageNumber));
	return leftPageNumber;
}

RightPageNumber IndexManager::getRightPageNumber(const void *data) const{
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(RightPageNumber);
	RightPageNumber rightPageNumber;
	memcpy(&rightPageNumber, data + offset, sizeof(RightPageNumber));
	return rightPageNumber;
}

IsLeafPage IndexManager::getIsLeafPage(const void *data) const{
	int offset = PAGE_SIZE - sizeof(FreeSpaceOffset) - sizeof(TotalSlots) - sizeof(RightPageNumber) - sizeof(LeftPageNumber) - sizeof(IsLeafPage);
	IsLeafPage isLeafPage;
	memcpy(&isLeafPage, data + offset, sizeof(IsLeafPage));
	return isLeafPage;
}

unsigned short IndexManager::getIndexPageDirectorySize() const{
	return sizeof(FreeSpaceOffset) + sizeof(TotalSlots) + sizeof(RightPageNumber) + sizeof(LeftPageNumber) + sizeof(IsLeafPage);
}

unsigned short IndexManager::getEntrySize(const KeySize &keySize) const{
	return sizeof(KeySize) + keySize + sizeof(RID);
}

unsigned short IndexManager::getNodeEntrySize(const KeySize &keySize) const{
	return sizeof(LeftPageNumber) + sizeof(KeySize) + keySize + sizeof(RID) + sizeof(RightPageNumber);
}

unsigned short IndexManager::getIndexPageFreeSpace(const void *data) const{
	unsigned indexDirectorySize = getIndexPageDirectorySize();
	unsigned freeSpaceOffset = getFreeSpaceOffset(data);
	return PAGE_SIZE - freeSpaceOffset - indexDirectorySize;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
	if (!ixfileHandle.fileHandle.file){
		return -1;
	}
	ix_ScanIterator.ixfileHandle = ixfileHandle;
	ix_ScanIterator.attribute = attribute;
	if (lowKey){
		switch(attribute.type){
		case TypeInt:
			ix_ScanIterator.lowKey = malloc(sizeof(int));
			memcpy(ix_ScanIterator.lowKey, lowKey, sizeof(int));
			break;
		case TypeReal:
			ix_ScanIterator.lowKey = malloc(sizeof(float));
			memcpy(ix_ScanIterator.lowKey, lowKey, sizeof(float));
			break;
		case TypeVarChar:{
			unsigned keySize = 0;
			memcpy(&keySize, lowKey, sizeof(unsigned));
			ix_ScanIterator.lowKey = malloc(keySize);
			memcpy(ix_ScanIterator.lowKey, lowKey + sizeof(unsigned), keySize);
			break;
		}
		case TypeDefault:
			return -1;
		default:
			return -1;
		}
	}
	if (highKey){
		switch(attribute.type){
		case TypeInt:
			ix_ScanIterator.highKey = malloc(sizeof(int));
			memcpy(ix_ScanIterator.highKey, highKey, sizeof(int));
			break;
		case TypeReal:
			ix_ScanIterator.highKey = malloc(sizeof(float));
			memcpy(ix_ScanIterator.highKey, highKey, sizeof(float));
			break;
		case TypeVarChar:{
			unsigned keySize = 0;
			memcpy(&keySize, highKey, sizeof(unsigned));
			ix_ScanIterator.highKey = malloc(keySize);
			memcpy(ix_ScanIterator.highKey, highKey + sizeof(unsigned), keySize);
			break;
		}
		case TypeDefault:
			return -1;
		default:
			return -1;
		}
	}
	ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
	ix_ScanIterator.highKeyInclusive = highKeyInclusive;
	RID lowRid;
	lowRid.pageNum = 0;
	lowRid.slotNum = 0;
	ix_ScanIterator.lowKeyRid.rid = lowRid;
	if (lowKey){
		ix_ScanIterator.lowKeyRid.keySize = 0;
		switch(attribute.type){
		case TypeInt:
			ix_ScanIterator.lowKeyRid.keySize = sizeof(int);
			ix_ScanIterator.lowKeyRid.key = malloc(ix_ScanIterator.lowKeyRid.keySize);
			memcpy(ix_ScanIterator.lowKeyRid.key, lowKey, ix_ScanIterator.lowKeyRid.keySize);
			break;
		case TypeReal:
			ix_ScanIterator.lowKeyRid.keySize = sizeof(float);
			ix_ScanIterator.lowKeyRid.key = malloc(ix_ScanIterator.lowKeyRid.keySize);
			memcpy(ix_ScanIterator.lowKeyRid.key, lowKey, ix_ScanIterator.lowKeyRid.keySize);
			break;
		case TypeVarChar:
			memcpy(&ix_ScanIterator.lowKeyRid.keySize, lowKey, sizeof(int));
			ix_ScanIterator.lowKeyRid.key = malloc(ix_ScanIterator.lowKeyRid.keySize);
			memcpy(ix_ScanIterator.lowKeyRid.key, lowKey + sizeof(int), ix_ScanIterator.lowKeyRid.keySize);
			break;
		case TypeDefault:
			return -1;
		default:
			return -1;
		}
		if (ix_ScanIterator.lowKeyRid.keySize == 0){
			return -1;
		}
	} else {
		ix_ScanIterator.lowKeyRid.keySize = 0;
	}
	RID highRid;
	highRid.pageNum = UINT_MAX;
	highRid.slotNum = UINT_MAX;
	ix_ScanIterator.highKeyRid.rid = highRid;

	if (highKey){
		ix_ScanIterator.highKeyRid.keySize = 0;
		switch(attribute.type){
		case TypeInt:
			ix_ScanIterator.highKeyRid.keySize = sizeof(int);
			ix_ScanIterator.highKeyRid.key = malloc(ix_ScanIterator.highKeyRid.keySize);
			memcpy(ix_ScanIterator.highKeyRid.key, highKey, ix_ScanIterator.highKeyRid.keySize);
			break;
		case TypeReal:
			ix_ScanIterator.highKeyRid.keySize = sizeof(float);
			ix_ScanIterator.highKeyRid.key = malloc(ix_ScanIterator.highKeyRid.keySize);
			memcpy(ix_ScanIterator.highKeyRid.key, highKey, ix_ScanIterator.highKeyRid.keySize);
			break;
		case TypeVarChar:
			memcpy(&ix_ScanIterator.highKeyRid.keySize, lowKey, sizeof(int));
			ix_ScanIterator.highKeyRid.key = malloc(ix_ScanIterator.highKeyRid.keySize);
			memcpy(ix_ScanIterator.highKeyRid.key, highKey + sizeof(int), ix_ScanIterator.highKeyRid.keySize);
			break;
		case TypeDefault:
			return -1;
		default:
			return -1;
		}
		if (ix_ScanIterator.highKeyRid.keySize == 0){
			return -1;
		}
	} else {
		ix_ScanIterator.highKeyRid.keySize = 0;
	}
	return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
	if(ixfileHandle.fileHandle.getNumberOfPages() == 0){
		cout << "{}" << endl;
		return;
	}
	unsigned rootPageNumber;
	ixfileHandle.fileHandle.getRootPageNumber(rootPageNumber);
	PageNumber pageNumber = (PageNumber) rootPageNumber;
	printBtreeHelper(ixfileHandle, attribute, pageNumber, true);
}

void IndexManager::printBtreeHelper(IXFileHandle &ixfileHandle, const Attribute &attribute, const PageNumber &pageNumber, const bool &root) const{
	void *page = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(pageNumber, page);
	unsigned pn = ixfileHandle.fileHandle.getNumberOfPages();
	TotalSlots totalSlots = getTotalSlots(page);
	if (totalSlots == 0){
		free(page);
		return;
	}
	if(getIsLeafPage(page)){
		cout << "{\"keys\": [";
		unsigned offset = 0;
		KeyRidPair keyToCompare;
		keyToCompare.key = NULL;
		for (unsigned i = 0; i < totalSlots; ++i){
			KeySize keySize;
			memcpy(&keySize, page + offset, sizeof(KeySize));
			unsigned short entrySize = getEntrySize(keySize);
			void *entry = malloc(entrySize);
			memcpy(entry, page + offset, entrySize);
			KeyRidPair entryKeyRid;
			entryKeyRid.key = NULL;
			createKeyRidPair(entryKeyRid, entry);
			if (i == 0){
				keyToCompare.key = malloc(keySize);
				keyToCompare.keySize = keySize;
				memcpy(keyToCompare.key, page + offset + sizeof(KeySize), keySize);
				if (attribute.type == TypeInt){
					int key;
					memcpy(&key, entryKeyRid.key, sizeof(int));
					cout << "\"" << key << ":[";
				} else if (attribute.type == TypeReal){
					float key;
					memcpy(&key, entryKeyRid.key, sizeof(float));
					cout << "\"" << key << ":[";
				} else if (attribute.type == TypeVarChar){
					char key[entryKeyRid.keySize + 1] = {0};
					memcpy(&key, entryKeyRid.key, entryKeyRid.keySize);
					cout << "\"" << key << ":[";
				}
				cout << "(" << entryKeyRid.rid.pageNum << "," << entryKeyRid.rid.slotNum << ")";
				offset += entrySize;
				free(entry);
				free(entryKeyRid.key);
				continue;
			}
			int comparison = compareKey(keyToCompare, entryKeyRid, attribute);
			if (comparison == 0){
				cout << ",(" << entryKeyRid.rid.pageNum << "," << entryKeyRid.rid.slotNum << ")";
			} else {
				cout << "]\",";
				if (keyToCompare.key){
					free(keyToCompare.key);
				}
				keyToCompare.key = malloc(keySize);
				keyToCompare.keySize = keySize;
				memcpy(keyToCompare.key, page + offset + sizeof(KeySize), keySize);
				if (attribute.type == TypeInt){
					int key;
					memcpy(&key, entryKeyRid.key, sizeof(int));
					cout << "\"" << key << ":[";
				} else if (attribute.type == TypeReal){
					float key;
					memcpy(&key, entryKeyRid.key, sizeof(float));
					cout << "\"" << key << ":[";
				} else if (attribute.type == TypeVarChar){
					char key[entryKeyRid.keySize + 1] = {0};
					memcpy(&key, entryKeyRid.key, entryKeyRid.keySize);
					cout << "\"" << key << ":[";
				}
				cout << "(" << entryKeyRid.rid.pageNum << "," << entryKeyRid.rid.slotNum << ")";
			}
			offset += entrySize;
			free(entry);
			free(entryKeyRid.key);
		}
		if (keyToCompare.key){
			free(keyToCompare.key);
			keyToCompare.key = NULL;
		}
		cout << "\"]}" << endl;
	} else {
		cout << "{\"keys\": [";
		unsigned offset = 0;
		for (unsigned i = 0; i < totalSlots; ++i){
			KeySize keySize;
			memcpy(&keySize, page + offset + sizeof(LeftPageNumber), sizeof(KeySize));
			unsigned short nodeEntrySize = getNodeEntrySize(keySize);
			void *nodeEntry = malloc(nodeEntrySize);
			memcpy(nodeEntry, page + offset, nodeEntrySize);
			NodeKeyRidPair nodeKeyRid;
			nodeKeyRid.key = NULL;
			createNodeKeyRidPair(nodeKeyRid, nodeEntry);
			if (attribute.type == TypeInt){
				int key;
				memcpy(&key, nodeKeyRid.key, sizeof(int));
				cout << key;
			} else if (attribute.type == TypeReal){
				float key;
				memcpy(&key, nodeKeyRid.key, sizeof(float));
				cout << key;
			} else if (attribute.type == TypeVarChar){
				char key[nodeKeyRid.keySize + 1] = {0};
				memcpy(&key, nodeKeyRid.key, nodeKeyRid.keySize);
				cout << "\"" << key << "\"";
			}
			if (i != totalSlots - 1){
				cout << ",";
			}
			offset += nodeEntrySize;
			free(nodeEntry);
			if (nodeKeyRid.key){
				free(nodeKeyRid.key);
			}
		}
		cout << "]," << endl << "\"children\": [";
		offset = 0;
		for (unsigned i = 0; i < totalSlots; ++i){
			KeySize keySize;
			memcpy(&keySize, page + offset + sizeof(LeftPageNumber), sizeof(KeySize));
			unsigned short nodeEntrySize = getNodeEntrySize(keySize);
			void *nodeEntry = malloc(nodeEntrySize);
			memcpy(nodeEntry, page + offset, nodeEntrySize);
			NodeKeyRidPair nodeKeyRid;
			nodeKeyRid.key = NULL;
			createNodeKeyRidPair(nodeKeyRid, nodeEntry);
			if (i == 0){
				printBtreeHelper(ixfileHandle, attribute, nodeKeyRid.leftPage, false);
				cout << ",";
			}
			printBtreeHelper(ixfileHandle, attribute, nodeKeyRid.rightPage, false);
			offset += nodeEntrySize;
			if (i != totalSlots - 1){
				cout << ",";
			}
			free(nodeEntry);
			if (nodeKeyRid.key){
				free(nodeKeyRid.key);
			}
		}
		cout << "]}";
	}
	if (root){
		cout << endl << endl;
	}
	free(page);
}

void IndexManager::printBtreeDetails(IXFileHandle &ixfileHandle, bool detailed){
	unsigned numberOfPages = ixfileHandle.fileHandle.getNumberOfPages();
	vector<unsigned> leafPages;
	vector<unsigned> nodePages;
	cout << "Number of pages: " << numberOfPages << endl;
	for (unsigned i = 0; i < numberOfPages; ++i){
		void *page = malloc(PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(i, page);
		if (getIsLeafPage(page)){
			leafPages.push_back(i);
		} else {
			nodePages.push_back(i);
		}
		free(page);;
	}
	cout << "Number of Leaf Pages: " << leafPages.size() << endl;

	if (detailed){
		for (unsigned pageNumber : leafPages){
			void *page = malloc(PAGE_SIZE);
			ixfileHandle.fileHandle.readPage(pageNumber, page);
			cout << pageNumber << ": (" << getLeftPageNumber(page) << "," << getRightPageNumber(page) << ") ";
			free(page);
		}
		cout << endl;
	} else {
		for (unsigned pageNumber : leafPages){
			cout << pageNumber << " ";
		}
		cout << endl;
	}
	cout << "Number of Node Pages: " << nodePages.size() << endl;

	if (detailed){
		for (unsigned pageNumber : nodePages){
			void *page = malloc(PAGE_SIZE);
			ixfileHandle.fileHandle.readPage(pageNumber, page);
			cout << pageNumber << ": (" << getLeftPageNumber(page) << "," << getRightPageNumber(page) << ") ";
			free(page);
		}
		cout << endl;
	} else {
		for (unsigned pageNumber : nodePages){
			cout << pageNumber << " ";
		}
		cout << endl;
	}
	stack<PageNumber> leafPageLink;
	TraversedPage traversedPage;
	navigateTreeLeft(ixfileHandle, traversedPage);
	while (traversedPage.pageNumber != USHRT_MAX){
		leafPageLink.push(traversedPage.pageNumber);
		traversedPage.pageNumber = getRightPageNumber(traversedPage.page);
		if (traversedPage.pageNumber != USHRT_MAX){
			ixfileHandle.fileHandle.readPage(traversedPage.pageNumber, traversedPage.page);
		}
	}
	cout << "Number of Leaf Page Links: " << leafPageLink.size() << endl;
	while (!leafPageLink.empty()){
		cout << leafPageLink.top() << " ";
		leafPageLink.pop();
	}
	cout << endl << endl;
}

int compareKeyRidPair(const KeyRidPair &lhs, const KeyRidPair &rhs, const Attribute &attribute){
	int comparison;
	if (attribute.type == TypeInt){
		int lhsInt;
		int rhsInt;
		memcpy(&lhsInt, lhs.key, sizeof(int));
		memcpy(&rhsInt, rhs.key, sizeof(int));
		if (lhsInt == rhsInt){
			comparison = 0;
		} else if (lhsInt < rhsInt){
			comparison = -1;
		} else if (lhsInt > rhsInt){
			comparison = 1;
		}
	} else if (attribute.type == TypeReal){
		float lhsFloat;
		float rhsFloat;
		memcpy(&lhsFloat, lhs.key, sizeof(float));
		memcpy(&rhsFloat, rhs.key, sizeof(float));
		if (lhsFloat == rhsFloat){
			comparison = 0;
		} else if (lhsFloat < rhsFloat){
			comparison = -1;
		} else if (lhsFloat > rhsFloat){
			comparison = 1;
		}
	} else if (attribute.type == TypeVarChar){
		unsigned smallerKeySize = (lhs.keySize < rhs.keySize) ? lhs.keySize : rhs.keySize;
		comparison = memcmp(lhs.key, rhs.key, smallerKeySize);
		if (comparison == 0){
			if (lhs.keySize < rhs.keySize){
				comparison = -1;
			} else if (lhs.keySize > rhs.keySize){
				comparison = 1;
			}
		}
	}
	if (comparison == 0){
		RID lhsRid = lhs.rid;
		RID rhsRid = rhs.rid;
		comparison = compareRid(lhsRid, rhsRid);
	}
	return comparison;
}

int compareKey(const KeyRidPair &lhs, const KeyRidPair &rhs, const Attribute &attribute){
	int comparison;
	if (attribute.type == TypeInt){
		int lhsInt;
		int rhsInt;
		memcpy(&lhsInt, lhs.key, sizeof(int));
		memcpy(&rhsInt, rhs.key, sizeof(int));
		if (lhsInt == rhsInt){
			comparison = 0;
		} else if (lhsInt < rhsInt){
			comparison = -1;
		} else if (lhsInt > rhsInt){
			comparison = 1;
		}
	} else if (attribute.type == TypeReal){
		float lhsFloat;
		float rhsFloat;
		memcpy(&lhsFloat, lhs.key, sizeof(float));
		memcpy(&rhsFloat, rhs.key, sizeof(float));
		if (lhsFloat == rhsFloat){
			comparison = 0;
		} else if (lhsFloat < rhsFloat){
			comparison = -1;
		} else if (lhsFloat > rhsFloat){
			comparison = 1;
		}
	} else if (attribute.type == TypeVarChar){
		unsigned smallerKeySize = (lhs.keySize < rhs.keySize) ? lhs.keySize : rhs.keySize;
		comparison = memcmp(lhs.key, rhs.key, smallerKeySize);
		if (comparison == 0){
			if (lhs.keySize < rhs.keySize){
				comparison = -1;
			} else if (lhs.keySize > rhs.keySize){
				comparison = 1;
			}
		}
	}
	return comparison;
}

int compareRid(const RID &lhs, const RID &rhs){
	int comparison;
	if (lhs.pageNum == rhs.pageNum){
		if (lhs.slotNum == rhs.slotNum){
			comparison = 0;
		}
		if (lhs.slotNum > rhs.slotNum){
			comparison = 1;
		}
		if (lhs.slotNum < rhs.slotNum){
			comparison = -1;
		}
	} else if (lhs.pageNum > rhs.pageNum){
		comparison = 1;
	} else {
		comparison = -1;
	}
	return comparison;
}


IX_ScanIterator::IX_ScanIterator()
{
	lowKey = NULL;
	highKey = NULL;
	lowKeyInclusive = false;
	highKeyInclusive = false;
	offset = UINT_MAX;
	lowKeyRid.key = NULL;
	highKeyRid.key = NULL;
	traversedPage.pageNumber = USHRT_MAX;
	traversedPage.page = NULL;
	previousEntrySize = UINT_MAX;
	previousFreeSpaceOffset = UINT_MAX;
}

IX_ScanIterator::~IX_ScanIterator()
{
	close();
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if (offset == UINT_MAX){
		if (ixfileHandle.fileHandle.getNumberOfPages() == 0){
			return IX_EOF;
		}
		if (!lowKey){
			IndexManager::instance()->navigateTreeLeft(ixfileHandle, traversedPage);
		} else {
			IndexManager::instance()->navigateTree(ixfileHandle, lowKeyRid, attribute, traversedPage);
		}
		offset = 0;
	}
	while (traversedPage.pageNumber != USHRT_MAX){
		FreeSpaceOffset freeSpaceOffset = IndexManager::instance()->getFreeSpaceOffset(traversedPage.page);
		if (previousFreeSpaceOffset != UINT_MAX){
			if (freeSpaceOffset - previousFreeSpaceOffset == previousEntrySize){
				offset -= previousEntrySize;
			}
		}
		while(offset < freeSpaceOffset){
			KeySize keySize = 0;
			memcpy(&keySize, traversedPage.page + offset, sizeof(KeySize));
			unsigned short entrySize = IndexManager::instance()->getEntrySize(keySize);
			void *entry = malloc(entrySize);
			memcpy(entry, traversedPage.page + offset, entrySize);
			if (!lowKey && !highKey){
				memcpy(key, traversedPage.page + offset + sizeof(KeySize), keySize);
				memcpy(&rid, traversedPage.page + offset + sizeof(KeySize) + keySize, sizeof(RID));
				previousEntrySize = entrySize;
				previousFreeSpaceOffset = freeSpaceOffset;
				offset += entrySize;
				free(entry);
				return 0;
			}
			offset += entrySize;
			KeyRidPair entryKeyRid;
			entryKeyRid.key = NULL;
			IndexManager::instance()->createKeyRidPair(entryKeyRid, entry);
			int lowKeyCompare = -1;
			int highKeyCompare = 1;
			if (lowKey){
				lowKeyCompare = compareKeyRidPair(entryKeyRid, lowKeyRid, attribute);
			}
			if (highKey){
				highKeyCompare = compareKeyRidPair(entryKeyRid, highKeyRid, attribute);
			}
			if ((highKeyInclusive && highKeyCompare <= 0) || (!highKeyInclusive && highKeyCompare < 0) || !highKey){
				if ((lowKeyInclusive && lowKeyCompare >= 0) || !lowKey){
					memcpy(&rid, &entryKeyRid.rid, sizeof(RID));
					memcpy(key, entryKeyRid.key, keySize);
					free(entry);
					if (entryKeyRid.key){
						free(entryKeyRid.key);
					}
					previousEntrySize = entrySize;
					previousFreeSpaceOffset = freeSpaceOffset;
					return 0;
				} else if ((!lowKeyInclusive && lowKeyCompare > 0) || !lowKey){
					rid = entryKeyRid.rid;
					memcpy(key, entryKeyRid.key, keySize);
					free(entry);
					if (entryKeyRid.key){
						free(entryKeyRid.key);
					}
					previousEntrySize = entrySize;
					previousFreeSpaceOffset = freeSpaceOffset;
					return 0;
				}
			}
			else{
				free(entry);
				if (entryKeyRid.key){
					free(entryKeyRid.key);
				}
				return IX_EOF;
			}
			free(entry);
			if (entryKeyRid.key){
				free(entryKeyRid.key);
			}
		}
		traversedPage.pageNumber = IndexManager::instance()->getRightPageNumber(traversedPage.page);
		if (traversedPage.pageNumber != USHRT_MAX){
			ixfileHandle.fileHandle.readPage(traversedPage.pageNumber, traversedPage.page);
			freeSpaceOffset = IndexManager::instance()->getFreeSpaceOffset(traversedPage.page);
			previousEntrySize = 0;
			offset = 0;
		}
	}
	return IX_EOF;
}

RC IX_ScanIterator::close()
{
	if (lowKey){
		free(lowKey);
		lowKey = NULL;
	}
	if (highKey){
		free(highKey);
		highKey = NULL;
	}
	if (lowKeyRid.key){
		free(lowKeyRid.key);
		lowKeyRid.keySize = 0;
		lowKeyRid.key = NULL;
	}
	if (highKeyRid.key){
		free(highKeyRid.key);
		highKeyRid.keySize = 0;
		highKeyRid.key = NULL;
	}
	if (traversedPage.page){
		free(traversedPage.page);
		traversedPage.pageNumber = USHRT_MAX;
		traversedPage.page = NULL;
	}
	lowKeyInclusive = false;
	highKeyInclusive = false;
	offset = UINT_MAX;
	previousEntrySize = UINT_MAX;
	previousFreeSpaceOffset = UINT_MAX;
	return 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	fileHandle.collectCounterValues(ixReadPageCounter, ixWritePageCounter, ixAppendPageCounter);
	readPageCount = ixReadPageCounter;
	writePageCount = ixWritePageCounter;
	appendPageCount = ixAppendPageCounter;
    return 0;
}

void freeTraversedPageStack(stack<TraversedPage> &traversedPages){
	while(!traversedPages.empty()){
		free(traversedPages.top().page);
		traversedPages.pop();
	}
}
