#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <map>
#include <queue>
#include <functional>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

typedef enum{ LEFT_PARTITION=0, RIGHT_PARTITION } PartitionSide;
// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};

struct ValueCountPair {
	float value;
	float count;
};

struct QueueSizePair {
	queue<void*> *page;
	unsigned short size;
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
	Iterator *input;
	Condition condition;
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter();

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};


class Project : public Iterator {
    // Projection operator
	Iterator *input;
	vector<Attribute> recordDescriptor;
	vector<Attribute> projectedAttrs;
    public:
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames);   // vector containing attribute names
        ~Project();

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
	Iterator *leftIn;
	TableScan *rightIn;
	Condition condition;
	unsigned numPages;
	unsigned currentHashMapSize;
	unsigned maxHashMapSize;
	Attribute leftAttribute;
	Attribute rightAttribute;
	vector<Attribute> leftAttributes;
	vector<Attribute> rightAttributes;
	vector<Attribute> joinedAttributes;
//	queue<void*> inputPage;
	unsigned currentOutputPageSize;
	queue<void*> outputPage;
	map<int, void*> intHashMap;
	map<float, void*> realHashMap;
	map<string, void*> varcharHashMap;
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        );
        ~BNLJoin();

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        RC insertHashMap(void *data, const vector<Attribute> recordDescriptor, const Attribute &attribute);
        RC clearHashMap(const Attribute &attribute);
        int getHashMapCount(void *data, const vector<Attribute> recordDescriptor, const Attribute &attribute);
        void* getHashMapValue(void *data, const vector<Attribute> recordDescriptor, const Attribute &attribute);
        bool getHashMapEmpty(const Attribute &attribute);
        RC joinData(void *leftData, const vector<Attribute> &leftAttributes, void *rightData, const vector<Attribute> &rightAttributes, void *joinedData, const vector<Attribute> &joinedAttributes);
};


class INLJoin : public Iterator {
    // Index nested-loop join operator
	Iterator *leftIn;
	IndexScan *rightIn;
	Condition condition;
	Attribute leftAttribute;
	Attribute rightAttribute;
	vector<Attribute> leftAttributes;
	vector<Attribute> rightAttributes;
	vector<Attribute> joinedAttributes;
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        );
        ~INLJoin();

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        RC joinData(void *leftData, const vector<Attribute> &leftAttributes, void *rightData, const vector<Attribute> &rightAttributes, void *joinedData, const vector<Attribute> &joinedAttributes);
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
	Iterator *leftIn;
	Iterator *rightIn;
	Condition condition;
	unsigned numPartitions;
	Attribute leftAttribute;
	Attribute rightAttribute;
	vector<Attribute> leftAttributes;
	vector<Attribute> rightAttributes;
	vector<Attribute> joinedAttributes;
	unsigned joinNumber;
	string stringJoinNumber;
	unsigned currentPartition;
	hash<int> intHash;
	hash<float> realHash;
	hash<string> varcharHash;
	BNLJoin *probe;
	TableScan *leftTableIn;
	TableScan *rightTableIn;
	vector<QueueSizePair> partitionPages;
//	bool probePhase;


    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      );
      ~GHJoin();

      RC getNextTuple(void *data);
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const;

    private:
      RC createPartition();
      RC insertPartition(void *tuple, PartitionSide partitionSide);
};

class Aggregate : public Iterator {
    // Aggregation operator
	Iterator *input;
	Attribute aggAttr;
	Attribute groupAttr;
	AggregateOp op;
	map<int, ValueCountPair> intRealHashMap;
	map<string, ValueCountPair> varcharRealHashMap;
	map<float, ValueCountPair> realRealHashMap;
	map<int, ValueCountPair>::iterator intIt;
	map<float, ValueCountPair>::iterator realIt;
	map<string, ValueCountPair>::iterator varcharIt;

    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );
        ~Aggregate();

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        RC calculate(float &returnValue, float &count, const int &value);
        RC calculate(float &returnValue, float &count, const float &value);
        RC insertHashMap(void *data, const vector<Attribute> recordDescriptor);
};

bool compareValues(const void *left, const void *right, const CompOp compOp, AttrType attrType);
unsigned getDataSize(void *data, const vector<Attribute> &recordDescriptor);
void printData(void *data, const vector<Attribute> &recordDescriptor);
#endif
