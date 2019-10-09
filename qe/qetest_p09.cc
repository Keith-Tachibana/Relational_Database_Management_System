#include <fstream>
#include <iostream>

#include <vector>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "qe_test_util.h"

RC privateTestCase_9() {
	// Mandatory for all
	// (+5 extra credit points will be given based on the results of the basic aggregation related tests)
	// 1. Basic aggregation - MIN
	// SELECT MIN(largeleft.B) from largeleft
    cerr << endl << "***** In QE Private Test Case 9 *****" << endl;

	RC rc = success;

	int compVal = 1000;
	
    // Create IndexScan
    TableScan *input = new TableScan(*rm, "largeleft");

	// Set up condition
	Condition cond2;
	cond2.lhsAttr = "largeleft.B";
	cond2.op = LT_OP;
	cond2.bRhsIsAttr = false;
	Value value2;
	value2.type = TypeInt;
	value2.data = malloc(bufSize);
	*(int *) value2.data = compVal;
	cond2.rhsValue = value2;

	// Create Filter
	Filter *filter = new Filter(input, cond2);

    // Create Aggregate
    Attribute aggAttr;
    aggAttr.name = "largeleft.B";
    aggAttr.type = TypeInt;
    aggAttr.length = 4;
    Aggregate *agg = new Aggregate(filter, aggAttr, MIN);

	int count = 0;
    void *data = malloc(bufSize);
    
    // An aggregation returns a float value
    float minVal = 0.0;
	
    while(agg->getNextTuple(data) != QE_EOF)
    {
    	minVal = *(float *) ((char *) data + 1);
        cerr << "MIN(largeleft.B) " << minVal << endl;
        memset(data, 0, sizeof(int));
        count++;
        if (count > 1) {
        	cerr << "***** The number of returned tuple is not correct. *****" << endl;
        	rc = fail;
        	break;
        }
    }

    if (minVal != 10.0) {
        cerr << "***** The returned value: " << minVal << " is not correct. *****" << endl;
    	rc = fail;
    }

    delete agg;
    delete filter;
    delete input;
    free(data);
    return rc;

}


int main() {
	
	if (privateTestCase_9() != success) {
		cerr << "***** [FAIL] QE Test Case 9 failed. *****" << endl;
		return fail;
	} else {
		cerr << "***** QE Private Test Case 9 finished. The result will be examined. *****" << endl;
		return success;
	}
}
