#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <stack>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

typedef unsigned short LeftPageNumber;
typedef unsigned short RightPageNumber;
typedef unsigned short PageNumber;
typedef unsigned short KeySize;
typedef bool IsLeafPage;

struct KeyRidPair{
	KeySize keySize;
	void *key;
	RID rid;
};

struct NodeKeyRidPair : KeyRidPair{
	LeftPageNumber leftPage;
	RightPageNumber rightPage;
};

struct TraversedPage{
	PageNumber pageNumber;
	void *page;
};

int compareKey(const KeyRidPair &lhs, const KeyRidPair &rhs, const Attribute &attribute);
int compareKeyRidPair(const KeyRidPair &lhs, const KeyRidPair &rhs, const Attribute &attribute);
int compareRid(const RID &lhs, const RID &rhs);
void freeTraversedPageStack(stack<TraversedPage> &traversedPages);

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

        RC createKeyRidPair(KeyRidPair &keyRid, const void *entry) const;
        FreeSpaceOffset getFreeSpaceOffset(const void *data) const;
        TotalSlots getTotalSlots(const void *data) const;
        RC navigateTree(IXFileHandle &ixfileHandle, const KeyRidPair &keyRid, const Attribute &attribute, TraversedPage &traversedPage);
        RC navigateTreeLeft(IXFileHandle &ixfileHandle, TraversedPage &traversedPage);
        LeftPageNumber getLeftPageNumber(const void *data) const;
        RightPageNumber getRightPageNumber(const void *data) const;
        unsigned short getEntrySize(const KeySize &keySize) const;
        void printBtreeDetails(IXFileHandle &ixfileHandle, bool detailed);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        RC insertEntryIntoTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *entry, const unsigned short &entrySize);
        RC createEntry(void *entry, const KeyRidPair &keyRidPair);
        RC createNodeEntry(void *entry, const NodeKeyRidPair &nodeKeyRid);
        RC createNodeKeyRidPair(NodeKeyRidPair &nodeKeyRid, const void *entry) const;
        RC initializeIndexPageDirectory(void *data, IsLeafPage isLeafPage);
        RC setFreeSpaceOffset(void *data, FreeSpaceOffset &value);
        RC increaseFreeSpaceOffset(void *data, const FreeSpaceOffset &value);
        RC decreaseFreeSpaceOffset(void *data, const FreeSpaceOffset &value);
        RC setTotalSlots(void *data, TotalSlots &value);
        RC incrementTotalSlots(void *data);
        RC decrementTotalSlots(void *data);
        RC setLeftPageNumber(void *data, LeftPageNumber &value);
        RC setPageNumber(void *data, PageNumber &value);
        RC setRightPageNumber(void *data, RightPageNumber &value);
        RC setIsLeafPage(void *data, IsLeafPage &value);
        IsLeafPage getIsLeafPage(const void *data) const;
        unsigned short getIndexPageDirectorySize() const;
        unsigned short getIndexPageFreeSpace(const void *data) const;
        unsigned short getNodeEntrySize(const KeySize &keySize) const;
        RC navigateTree(IXFileHandle &ixfileHandle, const KeyRidPair &keyRid, const Attribute &attribute, stack<TraversedPage> &traversedPages);
        RC insertEntryIntoLeaf(IXFileHandle &ixfileHandle, TraversedPage traversedPage, const void *entry, const unsigned short &entrySize, const Attribute &attribute, NodeKeyRidPair &nodeKeyRid);
        RC insertKeyIntoNode(IXFileHandle &ixfileHandle, TraversedPage traversedPage, const void *entry, const unsigned short &entrySize, const Attribute &attribute, NodeKeyRidPair &nodeKeyRid);
        RC splitLeaf(void *leftPage, void *rightPage, const void *insertedEntry, const unsigned short &entrySize, const Attribute &attribute, NodeKeyRidPair &nodeKeyRid);
        RC splitNode(void *leftPage, void *rightPage, const void *insertedEntry, const unsigned short &entrySize, const Attribute &attribute, NodeKeyRidPair &nodeKeyRid);
        RC shiftChunk(void *page, const unsigned short &startOffset, const short &offset);
        RC shiftChunk(void *page, const unsigned short &startOffset, const short &offset, const FreeSpaceOffset &freeSpaceOffset);
        void printBtreeHelper(IXFileHandle &ixfileHandle, const Attribute &attribute, const PageNumber &pageNumber, const bool &root) const;
};


class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    FileHandle fileHandle;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};


class IX_ScanIterator {
    public:
		IXFileHandle ixfileHandle;
		Attribute attribute;
		void *lowKey;
		void *highKey;
		bool lowKeyInclusive;
		bool highKeyInclusive;
		unsigned offset;
		KeyRidPair lowKeyRid;
		KeyRidPair highKeyRid;
		TraversedPage traversedPage;
		unsigned previousEntrySize;
		unsigned previousFreeSpaceOffset;

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};

#endif
