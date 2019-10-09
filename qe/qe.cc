
#include "qe.h"
#include <cstring>
#include <cmath>
#include <iostream>
#include <cfloat>
#include <sstream>

Filter::Filter(Iterator* input, const Condition &condition) {
	this->input = input;
	this->condition = condition;
}

Filter::~Filter(){
//	input->~Iterator()
}

// ... the rest of your implementations go here
RC Filter::getNextTuple(void *data){
	vector<Attribute> recordDescriptor;
	while (input->getNextTuple(data) != QE_EOF){
		if (recordDescriptor.empty()){
			input->getAttributes(recordDescriptor);
		}
		void *left = NULL;
		void *right = NULL;
		bool leftFound = false;
		bool rightFound = false;
		AttrType attrType = TypeDefault;
		int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
	    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
	    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
	    memcpy(nullsIndicator, data, nullFieldsIndicatorActualSize);
		if (!condition.bRhsIsAttr){
			rightFound = true;
			if (condition.rhsValue.type == TypeInt){
				right = malloc(sizeof(int));
				memcpy(right, condition.rhsValue.data, sizeof(int));
			} else if (condition.rhsValue.type == TypeReal){
				right = malloc(sizeof(float));
				memcpy(right, condition.rhsValue.data, sizeof(float));
			} else if (condition.rhsValue.type == TypeVarChar){
				int length = 0;
				memcpy(&length, condition.rhsValue.data, sizeof(int));
				right = malloc(sizeof(int) + length);
				memcpy(right, condition.rhsValue.data, sizeof(int) + length);
			}
		}
		for (int i = 0, bytePosition = 0, offset = nullFieldsIndicatorActualSize; i < recordDescriptor.size(); ++i){
			Attribute attribute = recordDescriptor.at(i);
			if (i != 0 && i % 8 == 0){
				++bytePosition;
			}
			if (attribute.name == condition.lhsAttr){
				leftFound = true;
				attrType = attribute.type;
				if(getBit(nullsIndicator[bytePosition], i % 8)){
					left = NULL;
				} else if (attribute.type == TypeInt){
					left = malloc(sizeof(int));
					memcpy(left, data + offset, sizeof(int));
				} else if (attribute.type == TypeReal){
					left = malloc(sizeof(float));
					memcpy(left, data + offset, sizeof(float));
				} else if (attribute.type == TypeVarChar){
					int length = 0;
					memcpy(&length, data + offset, sizeof(int));
					left = malloc(sizeof(int) + length);
					memcpy(left, data + offset, sizeof(int) + length);
				}
			}
			if (condition.bRhsIsAttr && attribute.name == condition.rhsAttr){
				rightFound = true;
				if(getBit(nullsIndicator[bytePosition], i % 8)){
					right == NULL;
				} else if (attribute.type == TypeInt){
					right = malloc(sizeof(int));
					memcpy(right, data + offset, sizeof(int));
				} else if (attribute.type == TypeReal){
					right = malloc(sizeof(float));
					memcpy(right, data + offset, sizeof(int));
				} else if (attribute.type == TypeVarChar){
					int length = 0;
					memcpy(&length, data + offset, sizeof(int));
					right = malloc(sizeof(int) + length);
					memcpy(right, data + offset, sizeof(int) + length);
				}
			}
			if(!getBit(nullsIndicator[bytePosition], i % 8)){
				if (attribute.type == TypeInt){
					offset += sizeof(int);
				} else if (attribute.type == TypeReal){
					offset += sizeof(float);
				} else if (attribute.type == TypeVarChar){
					int length = 0;
					memcpy(&length, data + offset, sizeof(int));
					offset += sizeof(int) + length;
				}
			}
		}
		if (!leftFound || !rightFound){
			if (left){
				free(left);
				left = NULL;
			}
			if (right){
				free(right);
				right = NULL;
			}
			return -1;
		}
		if (compareValues(left, right, condition.op, attrType)){
			if (left){
				free(left);
				left = NULL;
			}
			if (right){
				free(right);
				right = NULL;
			}
			return 0;
		}
		if (left){
			free(left);
			left = NULL;
		}
		if (right){
			free(right);
			right = NULL;
		}
	}
	return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	input->getAttributes(attrs);
}

Project::Project(Iterator *input, const vector<string> &attrNames){
	this->input = input;
	input->getAttributes(recordDescriptor);
	for (Attribute attribute : recordDescriptor){
		for (string attrName : attrNames){
			if (attribute.name == attrName){
				projectedAttrs.push_back(attribute);
				break;
			}
		}
		if (projectedAttrs.size() == attrNames.size()){
			break;
		}
	}
}

Project::~Project(){
	projectedAttrs.clear();
//	input->~Iterator();
}

RC Project::getNextTuple(void *data){
	void *record = malloc(PAGE_SIZE);
	memset(record, 0, PAGE_SIZE);
	if (input->getNextTuple(record) == QE_EOF){
		free(record);
		return -1;
	}
	int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
	unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
	memcpy(nullsIndicator, record, nullFieldsIndicatorActualSize);
	int projectedNullFieldsIndicatorActualSize = (int)ceil(projectedAttrs.size() / 8.0);
	unsigned char projectedNullsIndicator[projectedNullFieldsIndicatorActualSize];
	memset(projectedNullsIndicator, 0, projectedNullFieldsIndicatorActualSize);
	for (int i = 0, bytePosition = 0, offset = nullFieldsIndicatorActualSize, j = 0, projectedBytePosition = 0, projectedOffset = projectedNullFieldsIndicatorActualSize; i < recordDescriptor.size(); ++i){
		Attribute attribute = recordDescriptor.at(i);
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
		for (Attribute projectedAttribute : projectedAttrs){
			if (attribute.name == projectedAttribute.name){
				if (j != 0 && j % 8 == 0){
					++projectedBytePosition;
				}
				if(getBit(nullsIndicator[bytePosition], i % 8)){
					setNullBit(nullsIndicator[projectedBytePosition], j % 8);
				} else {
					if (attribute.type == TypeInt){
						memcpy(data + projectedOffset, record + offset, sizeof(int));
						projectedOffset += sizeof(int);
					} else if (attribute.type == TypeReal){
						memcpy(data + projectedOffset, record + offset, sizeof(float));
						projectedOffset += sizeof(float);
					} else if (attribute.type == TypeVarChar){
						int length = 0;
						memcpy(&length, record + offset, sizeof(int));
						memcpy(data + projectedOffset, record + offset, sizeof(int) + length);
						projectedOffset += sizeof(int) + length;
					}
				}
				++j;
			}
		}
		if(!getBit(nullsIndicator[bytePosition], i % 8)){
			if (attribute.type == TypeInt){
				offset += sizeof(int);
			} else if (attribute.type == TypeReal){
				offset += sizeof(float);
			} else if (attribute.type == TypeVarChar){
				int length = 0;
				memcpy(&length, record + offset, sizeof(int));
				offset += sizeof(int) + length;
			}
		}
	}
	memcpy(data, projectedNullsIndicator, projectedNullFieldsIndicatorActualSize);
	free(record);
	return 0;
}

void Project::getAttributes(vector<Attribute> &attrs) const{
	attrs = projectedAttrs;
}

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	this->numPages = numPages;
	currentHashMapSize = 0;
	//needs at least 1 page for input, 1 page for output, 1 page for hash table
	if ((numPages - 2) < 1){
		maxHashMapSize = PAGE_SIZE;
	} else {
		maxHashMapSize = (numPages - 2) * PAGE_SIZE;
	}
	joinedAttributes.clear();
	this->leftIn->getAttributes(leftAttributes);
	for (Attribute attribute : leftAttributes){
		joinedAttributes.push_back(attribute);
	}
	for (Attribute attribute : leftAttributes){
		if (attribute.name == this->condition.lhsAttr){
			leftAttribute = attribute;
			break;
		}
	}
	this->rightIn->getAttributes(rightAttributes);
	for (Attribute attribute: rightAttributes){
		joinedAttributes.push_back(attribute);
	}
	for (Attribute attribute : rightAttributes){
		if (attribute.name == this->condition.rhsAttr){
			rightAttribute = attribute;
			break;
		}
	}
	currentOutputPageSize = 0;
}

BNLJoin::~BNLJoin(){
	joinedAttributes.clear();
}

RC BNLJoin::getNextTuple(void *data){
	if (outputPage.empty()){
		currentOutputPageSize = 0;
		if (currentHashMapSize == 0){
			void *leftData = malloc(PAGE_SIZE);
			memset(leftData, 0, PAGE_SIZE);
			while (leftIn->getNextTuple(leftData) != QE_EOF){
				unsigned dataSize = getDataSize(leftData, leftAttributes);
				void *insertedData = malloc(dataSize);
				memcpy(insertedData, leftData, dataSize);
				int offset = sizeof(int) + sizeof(int);
				float c = *(float *) ((char *) insertedData + offset + 1);
				if (insertHashMap(insertedData, leftAttributes, leftAttribute) == -1){
					free(leftData);
					free(insertedData);
					return -1;
				}
				currentHashMapSize += dataSize;
				memset(leftData, 0, PAGE_SIZE);
				if (currentHashMapSize >= maxHashMapSize){
					break;
				}
			}
			free(leftData);
		}
		void *leftData = malloc(PAGE_SIZE);
		memset(leftData, 0, PAGE_SIZE);
		void *rightData = malloc(PAGE_SIZE);
		memset(rightData, 0, PAGE_SIZE);
		while (currentHashMapSize != 0){
			while (rightIn->getNextTuple(rightData) != QE_EOF){
				unsigned dataSize = getDataSize(rightData, rightAttributes);
				void *actualRightData = malloc(dataSize);
				memcpy(actualRightData, rightData, dataSize);
				if (getHashMapCount(actualRightData, rightAttributes, rightAttribute) > 0){
					void *actualLeftData = getHashMapValue(actualRightData, rightAttributes, rightAttribute);
//					std::cout << "LeftIn" << std::endl;
//					printData(actualLeftData, leftAttributes);
//					std::cout << "RightIn" << std::endl;
//					printData(actualRightData, rightAttributes);
					void *tempJoinedData = malloc(PAGE_SIZE);
					memset(tempJoinedData, 0, PAGE_SIZE);
					joinData(actualLeftData, leftAttributes, actualRightData, rightAttributes, tempJoinedData, joinedAttributes);
					unsigned joinedDataSize = getDataSize(tempJoinedData, joinedAttributes);
//					memcpy(data, tempJoinedData, joinedDataSize);
//					return 0;
					void *joinedData = malloc(joinedDataSize);
					memcpy(joinedData, tempJoinedData, joinedDataSize);
					free(tempJoinedData);
//					std::cout << "Joined" << std::endl;
//					printData(joinedData, joinedAttributes);
					outputPage.push(joinedData);
					currentOutputPageSize += joinedDataSize;
				}
				memset(rightData, 0, PAGE_SIZE);
				free(actualRightData);
			}
			if (currentOutputPageSize > PAGE_SIZE){
				break;
			}
			clearHashMap(leftAttribute);
			currentHashMapSize = 0;
			memset(leftData, 0, PAGE_SIZE);
			while (leftIn->getNextTuple(leftData) != QE_EOF){
				unsigned dataSize = getDataSize(leftData, leftAttributes);
				void *insertedData = malloc(dataSize);
				memcpy(insertedData, leftData, dataSize);
				if (insertHashMap(insertedData, leftAttributes, leftAttribute) == -1){
					free(leftData);
					free(insertedData);
					return -1;
				}
				currentHashMapSize += dataSize;
				memset(leftData, 0, PAGE_SIZE);
				if (currentHashMapSize >= maxHashMapSize){
					break;
				}
			}
			rightIn->setIterator();
		}
		free(leftData);
		free(rightData);
	}
	if (!outputPage.empty()){
		void *frontData = outputPage.front();
		outputPage.pop();
		unsigned dataSize = getDataSize(frontData, joinedAttributes);
		memcpy(data, frontData, dataSize);
		free(frontData);
		return 0;
	}
	return QE_EOF;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs = joinedAttributes;
}

RC BNLJoin::insertHashMap(void *data, const vector<Attribute> recordDescriptor, const Attribute &attribute){
	int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy(nullsIndicator, data, nullFieldsIndicatorActualSize);
    int offset = nullFieldsIndicatorActualSize;
    for (int i = 0, bytePosition = 0; i < recordDescriptor.size(); ++i){
    	Attribute currentAttribute = recordDescriptor.at(i);
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
    	if (currentAttribute.name == attribute.name){
    		if(!getBit(nullsIndicator[bytePosition], i % 8)){
    			if (attribute.type == TypeInt){
    				int key;
    				memcpy(&key, data + offset, sizeof(int));
    				intHashMap.emplace(key, data);
    				return 0;
    			} else if (attribute.type == TypeReal){
    				float key;
    				memcpy(&key, data + offset, sizeof(float));
    				realHashMap.emplace(key, data);
    				return 0;
    			} else if (attribute.type == TypeVarChar){
    				int length = 0;
    				memcpy(&length, data + offset, sizeof(int));
    				char key[length + 1] = {0};
    				memcpy(key, data + offset + sizeof(int), length);
    				varcharHashMap.emplace(string(key), data);
    				return 0;
    			}
    		}
    	}
		if(!getBit(nullsIndicator[bytePosition], i % 8)){
			if (currentAttribute.type == TypeInt){
				offset += sizeof(int);
			} else if (currentAttribute.type == TypeReal){
				offset += sizeof(float);
			} else if (currentAttribute.type == TypeVarChar){
				int length = 0;
				memcpy(&length, data + offset, sizeof(int));
				offset += sizeof(int) + length;
			}
		}
    }
	return -1;
}

RC BNLJoin::clearHashMap(const Attribute &attribute){
	if (attribute.type == TypeInt){
		map<int, void*>::iterator it;
		for (it = intHashMap.begin(); it != intHashMap.end(); ++it){
			if (it->second){
				free(it->second);
			}
		}
		intHashMap.clear();
		return 0;
	} else if (attribute.type == TypeReal){
		map<float, void*>::iterator it;
		for (it = realHashMap.begin(); it != realHashMap.end(); ++it){
			if (it->second){
				free(it->second);
			}
		}
		realHashMap.clear();
		return 0;
	} else if (attribute.type == TypeVarChar){
		map<string, void*>::iterator it;
		for (it = varcharHashMap.begin(); it != varcharHashMap.end(); ++it){
			if (it->second){
				free(it->second);
			}
		}
		varcharHashMap.clear();
		return 0;
	}
	return -1;
}

int BNLJoin::getHashMapCount(void *data, const vector<Attribute> recordDescriptor, const Attribute &attribute){
	int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy(nullsIndicator, data, nullFieldsIndicatorActualSize);
    int offset = nullFieldsIndicatorActualSize;
    for (int i = 0, bytePosition = 0; i < recordDescriptor.size(); ++i){
    	Attribute currentAttribute = recordDescriptor.at(i);
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
    	if (currentAttribute.name == attribute.name){
    		if(!getBit(nullsIndicator[bytePosition], i % 8)){
    			if (attribute.type == TypeInt){
    				int key;
    				memcpy(&key, data + offset, sizeof(int));
    				return intHashMap.count(key);
    			} else if (attribute.type == TypeReal){
    				float key;
    				memcpy(&key, data + offset, sizeof(float));
    				return realHashMap.count(key);
    			} else if (attribute.type == TypeVarChar){
    				int length = 0;
    				memcpy(&length, data + offset, sizeof(int));
    				char key[length + 1] = {0};
    				memcpy(key, data + offset + sizeof(int), length);
    				return varcharHashMap.count(key);
    			}
    		}
    	}
		if(!getBit(nullsIndicator[bytePosition], i % 8)){
			if (currentAttribute.type == TypeInt){
				offset += sizeof(int);
			} else if (currentAttribute.type == TypeReal){
				offset += sizeof(float);
			} else if (currentAttribute.type == TypeVarChar){
				int length = 0;
				memcpy(&length, data + offset, sizeof(int));
				offset += sizeof(int) + length;
			}
		}
    }
	return -1;
}

void* BNLJoin::getHashMapValue(void *data, const vector<Attribute> recordDescriptor, const Attribute &attribute){
	int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy(nullsIndicator, data, nullFieldsIndicatorActualSize);
    int offset = nullFieldsIndicatorActualSize;
    for (int i = 0, bytePosition = 0; i < recordDescriptor.size(); ++i){
    	Attribute currentAttribute = recordDescriptor.at(i);
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
    	if (currentAttribute.name == attribute.name){
    		if(!getBit(nullsIndicator[bytePosition], i % 8)){
    			if (attribute.type == TypeInt){
    				int key;
    				memcpy(&key, data + offset, sizeof(int));
    				return intHashMap.at(key);
    			} else if (attribute.type == TypeReal){
    				float key;
    				memcpy(&key, data + offset, sizeof(float));
    				return realHashMap.at(key);
    			} else if (attribute.type == TypeVarChar){
    				int length = 0;
    				memcpy(&length, data + offset, sizeof(int));
    				char key[length + 1] = {0};
    				memcpy(key, data + offset + sizeof(int), length);
    				return varcharHashMap.at(key);
    			}
    		}
    	}
		if(!getBit(nullsIndicator[bytePosition], i % 8)){
			if (currentAttribute.type == TypeInt){
				offset += sizeof(int);
			} else if (currentAttribute.type == TypeReal){
				offset += sizeof(float);
			} else if (currentAttribute.type == TypeVarChar){
				int length = 0;
				memcpy(&length, data + offset, sizeof(int));
				offset += sizeof(int) + length;
			}
		}
    }
}

bool BNLJoin::getHashMapEmpty(const Attribute &attribute){
	if (attribute.type == TypeInt){
		return intHashMap.empty();
	} else if (attribute.type == TypeReal){
		return realHashMap.empty();
	} else if (attribute.type == TypeVarChar){
		return varcharHashMap.empty();
	}
}

RC BNLJoin::joinData(void *leftData, const vector<Attribute> &leftAttributes, void *rightData, const vector<Attribute> &rightAttributes, void *joinedData, const vector<Attribute> &joinedAttributes){
	int leftNullFieldsIndicatorActualSize = (int)ceil(leftAttributes.size() / 8.0);
	int rightNullFieldsIndicatorActualSize = (int)ceil(rightAttributes.size() / 8.0);
	int joinedNullFieldsIndicatorActualSize = (int)ceil(joinedAttributes.size() / 8.0);
    unsigned char leftNullsIndicator[leftNullFieldsIndicatorActualSize];
    memset(leftNullsIndicator, 0, leftNullFieldsIndicatorActualSize);
    memcpy(leftNullsIndicator, leftData, leftNullFieldsIndicatorActualSize);
    unsigned char rightNullsIndicator[rightNullFieldsIndicatorActualSize];
    memset(rightNullsIndicator, 0, rightNullFieldsIndicatorActualSize);
    memcpy(rightNullsIndicator, rightData, rightNullFieldsIndicatorActualSize);
    unsigned char joinedNullsIndicator[joinedNullFieldsIndicatorActualSize];
    memset(joinedNullsIndicator, 0, joinedNullFieldsIndicatorActualSize);
    unsigned leftDataSize = getDataSize(leftData, leftAttributes);
    unsigned rightDataSize = getDataSize(rightData, rightAttributes);
    unsigned joinedDataSize = leftDataSize - leftNullFieldsIndicatorActualSize + rightDataSize - rightNullFieldsIndicatorActualSize + joinedNullFieldsIndicatorActualSize;
    int joinedPosition = 0;
    int joinedOffset = joinedNullFieldsIndicatorActualSize;
    int joinedBytePosition = 0;
    for (int i = 0, bytePosition = 0; i < leftAttributes.size(); ++i){
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
		if (joinedPosition != 0 && i % 8 == 0){
			++joinedBytePosition;
		}
		if(getBit(leftNullsIndicator[bytePosition], i % 8)){
			setNullBit(joinedNullsIndicator[joinedBytePosition], joinedPosition % 8);
		}
		++joinedPosition;
    }

    for (int i = 0, bytePosition = 0; i < rightAttributes.size(); ++i){
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
		if (joinedPosition != 0 && i % 8 == 0){
			++joinedBytePosition;
		}
		if(getBit(rightNullsIndicator[bytePosition], i % 8)){
			setNullBit(joinedNullsIndicator[joinedBytePosition], joinedPosition % 8);
		}
		++joinedPosition;
	}
    memcpy(joinedData, joinedNullsIndicator, joinedNullFieldsIndicatorActualSize);
    memcpy(joinedData + joinedOffset, leftData + leftNullFieldsIndicatorActualSize, leftDataSize - leftNullFieldsIndicatorActualSize);
    joinedOffset += leftDataSize - leftNullFieldsIndicatorActualSize;
    memcpy(joinedData + joinedOffset, rightData + rightNullFieldsIndicatorActualSize, rightDataSize - rightNullFieldsIndicatorActualSize);
//    printData(joinedData, joinedAttributes);
    return 0;
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	joinedAttributes.clear();
	this->leftIn->getAttributes(leftAttributes);
	for (Attribute attribute : leftAttributes){
		joinedAttributes.push_back(attribute);
	}
	for (Attribute attribute : leftAttributes){
		if (attribute.name == this->condition.lhsAttr){
			leftAttribute = attribute;
			break;
		}
	}
	this->rightIn->getAttributes(rightAttributes);
	for (Attribute attribute: rightAttributes){
		joinedAttributes.push_back(attribute);
	}
	for (Attribute attribute : rightAttributes){
		if (attribute.name == this->condition.rhsAttr){
			rightAttribute = attribute;
			break;
		}
	}
}

INLJoin::~INLJoin(){
	joinedAttributes.clear();
}

RC INLJoin::getNextTuple(void *data){
	void *leftData = malloc(PAGE_SIZE);
	memset(leftData, 0, PAGE_SIZE);
	void *rightData = malloc(PAGE_SIZE);
	memset(rightData, 0, PAGE_SIZE);
	while (leftIn->getNextTuple(leftData) != QE_EOF){
		unsigned leftDataSize = getDataSize(leftData, leftAttributes);
		void *actualLeftData = malloc(leftDataSize);
		memcpy(actualLeftData, leftData, leftDataSize);
		int leftNullFieldsIndicatorActualSize = (int)ceil(leftAttributes.size() / 8.0);
	    unsigned char leftNullsIndicator[leftNullFieldsIndicatorActualSize];
	    memset(leftNullsIndicator, 0, leftNullFieldsIndicatorActualSize);
	    memcpy(leftNullsIndicator, actualLeftData, leftNullFieldsIndicatorActualSize);
	    void *leftValue = NULL;
	    for (int i = 0, bytePosition = 0, offset = leftNullFieldsIndicatorActualSize; i < leftAttributes.size(); ++i){
	    	Attribute currentAttribute = leftAttributes.at(i);
			if (i != 0 && i % 8 == 0){
				++bytePosition;
			}
	    	if (currentAttribute.name == leftAttribute.name){
	    		if(!getBit(leftNullsIndicator[bytePosition], i % 8)){
	    			if (currentAttribute.type == TypeInt){
	    				leftValue = malloc(sizeof(int));
	    				memcpy(leftValue, actualLeftData + offset, sizeof(int));
	    			} else if (currentAttribute.type == TypeReal){
	    				leftValue = malloc(sizeof(float));
	    				memcpy(leftValue, actualLeftData + offset, sizeof(float));
	    			} else if (currentAttribute.type == TypeVarChar){
	    				int length = 0;
	    				memcpy(&length, actualLeftData + offset, sizeof(int));
	    				leftValue = malloc(sizeof(int) + length);
	    				memcpy(leftValue, actualLeftData + offset, sizeof(int) + length);
	    			}
	    		}
	    		break;
	    	}
			if(!getBit(leftNullsIndicator[bytePosition], i % 8)){
				if (currentAttribute.type == TypeInt){
					offset += sizeof(int);
				} else if (currentAttribute.type == TypeReal){
					offset += sizeof(float);
				} else if (currentAttribute.type == TypeVarChar){
					int length = 0;
					memcpy(&length, actualLeftData + offset, sizeof(int));
					offset += sizeof(int) + length;
				}
			}
	    }
	    if (!leftValue){
			free(actualLeftData);
	    	continue;
	    }
		while(rightIn->getNextTuple(rightData) != QE_EOF){
			unsigned rightDataSize = getDataSize(rightData, rightAttributes);
			void *actualRightData = malloc(rightDataSize);
			memcpy(actualRightData, rightData, rightDataSize);
			int rightNullFieldsIndicatorActualSize = (int)ceil(rightAttributes.size() / 8.0);
		    unsigned char rightNullsIndicator[rightNullFieldsIndicatorActualSize];
		    memset(rightNullsIndicator, 0, rightNullFieldsIndicatorActualSize);
		    memcpy(rightNullsIndicator, actualRightData, rightNullFieldsIndicatorActualSize);
		    void *rightValue = NULL;
		    for (int i = 0, bytePosition = 0, offset = rightNullFieldsIndicatorActualSize; i < rightAttributes.size(); ++i){
		    	Attribute currentAttribute = rightAttributes.at(i);
				if (i != 0 && i % 8 == 0){
					++bytePosition;
				}
		    	if (currentAttribute.name == rightAttribute.name){
		    		if(!getBit(rightNullsIndicator[bytePosition], i % 8)){
		    			if (currentAttribute.type == TypeInt){
		    				rightValue = malloc(sizeof(int));
		    				memcpy(rightValue, actualRightData + offset, sizeof(int));
		    			} else if (currentAttribute.type == TypeReal){
		    				rightValue = malloc(sizeof(float));
		    				memcpy(rightValue, actualRightData + offset, sizeof(float));
		    			} else if (currentAttribute.type == TypeVarChar){
		    				int length = 0;
		    				memcpy(&length, actualRightData + offset, sizeof(int));
		    				rightValue = malloc(sizeof(int) + length);
		    				memcpy(rightValue, actualRightData + offset, sizeof(int) + length);
		    			}
		    		}
		    		break;
		    	}
				if(!getBit(rightNullsIndicator[bytePosition], i % 8)){
					if (currentAttribute.type == TypeInt){
						offset += sizeof(int);
					} else if (currentAttribute.type == TypeReal){
						offset += sizeof(float);
					} else if (currentAttribute.type == TypeVarChar){
						int length = 0;
						memcpy(&length, actualRightData + offset, sizeof(int));
						offset += sizeof(int) + length;
					}
				}
		    }
		    if (!rightValue){
			    free(actualRightData);
		    	continue;
		    }
		    if (compareValues(leftValue, rightValue, condition.op, leftAttribute.type)){
//				std::cout << "LeftIn" << std::endl;
//				printData(actualLeftData, leftAttributes);
//				std::cout << "RightIn" << std::endl;
//				printData(actualRightData, rightAttributes);
				void *tempJoinedData = malloc(PAGE_SIZE);
				memset(tempJoinedData, 0, PAGE_SIZE);
				joinData(actualLeftData, leftAttributes, actualRightData, rightAttributes, tempJoinedData, joinedAttributes);
				unsigned joinedDataSize = getDataSize(tempJoinedData, joinedAttributes);
				void *joinedData = malloc(joinedDataSize);
				memcpy(joinedData, tempJoinedData, joinedDataSize);
				free(tempJoinedData);
//				std::cout << "Joined" << std::endl;
//				printData(joinedData, joinedAttributes);
				memcpy(data, joinedData, joinedDataSize);
				free(actualLeftData);
				free(actualRightData);
				free(joinedData);
		    	free(leftValue);
		    	free(rightValue);
		    	free(leftData);
		    	free(rightData);
		    	return 0;
		    }
		    free(actualRightData);
		    free(rightValue);
			memset(rightData, 0, PAGE_SIZE);
		}
		rightIn->setIterator(NULL, NULL, true, true);
		free(actualLeftData);
		free(leftValue);
		memset(leftData, 0, PAGE_SIZE);
	}
	free(leftData);
	free(rightData);
	return QE_EOF;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs = joinedAttributes;
}

RC INLJoin::joinData(void *leftData, const vector<Attribute> &leftAttributes, void *rightData, const vector<Attribute> &rightAttributes, void *joinedData, const vector<Attribute> &joinedAttributes){
	int leftNullFieldsIndicatorActualSize = (int)ceil(leftAttributes.size() / 8.0);
	int rightNullFieldsIndicatorActualSize = (int)ceil(rightAttributes.size() / 8.0);
	int joinedNullFieldsIndicatorActualSize = (int)ceil(joinedAttributes.size() / 8.0);
    unsigned char leftNullsIndicator[leftNullFieldsIndicatorActualSize];
    memset(leftNullsIndicator, 0, leftNullFieldsIndicatorActualSize);
    memcpy(leftNullsIndicator, leftData, leftNullFieldsIndicatorActualSize);
    unsigned char rightNullsIndicator[rightNullFieldsIndicatorActualSize];
    memset(rightNullsIndicator, 0, rightNullFieldsIndicatorActualSize);
    memcpy(rightNullsIndicator, rightData, rightNullFieldsIndicatorActualSize);
    unsigned char joinedNullsIndicator[joinedNullFieldsIndicatorActualSize];
    memset(joinedNullsIndicator, 0, joinedNullFieldsIndicatorActualSize);
    unsigned leftDataSize = getDataSize(leftData, leftAttributes);
    unsigned rightDataSize = getDataSize(rightData, rightAttributes);
    unsigned joinedDataSize = leftDataSize - leftNullFieldsIndicatorActualSize + rightDataSize - rightNullFieldsIndicatorActualSize + joinedNullFieldsIndicatorActualSize;
    int joinedPosition = 0;
    int joinedOffset = joinedNullFieldsIndicatorActualSize;
    int joinedBytePosition = 0;
    for (int i = 0, bytePosition = 0; i < leftAttributes.size(); ++i){
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
		if (joinedPosition != 0 && i % 8 == 0){
			++joinedBytePosition;
		}
		if(getBit(leftNullsIndicator[bytePosition], i % 8)){
			setNullBit(joinedNullsIndicator[joinedBytePosition], joinedPosition % 8);
		}
		++joinedPosition;
    }

    for (int i = 0, bytePosition = 0; i < rightAttributes.size(); ++i){
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
		if (joinedPosition != 0 && i % 8 == 0){
			++joinedBytePosition;
		}
		if(getBit(rightNullsIndicator[bytePosition], i % 8)){
			setNullBit(joinedNullsIndicator[joinedBytePosition], joinedPosition % 8);
		}
		++joinedPosition;
	}
    memcpy(joinedData, joinedNullsIndicator, joinedNullFieldsIndicatorActualSize);
    memcpy(joinedData + joinedOffset, leftData + leftNullFieldsIndicatorActualSize, leftDataSize - leftNullFieldsIndicatorActualSize);
    joinedOffset += leftDataSize - leftNullFieldsIndicatorActualSize;
    memcpy(joinedData + joinedOffset, rightData + rightNullFieldsIndicatorActualSize, rightDataSize - rightNullFieldsIndicatorActualSize);
//    printData(joinedData, joinedAttributes);
    return 0;
}

GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPartitions){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	this->numPartitions = numPartitions;
	joinedAttributes.clear();
	this->leftIn->getAttributes(leftAttributes);
	for (Attribute attribute : leftAttributes){
		joinedAttributes.push_back(attribute);
	}
	for (Attribute attribute : leftAttributes){
		if (attribute.name == this->condition.lhsAttr){
			leftAttribute = attribute;
			break;
		}
	}
	this->rightIn->getAttributes(rightAttributes);
	for (Attribute attribute: rightAttributes){
		joinedAttributes.push_back(attribute);
	}
	for (Attribute attribute : rightAttributes){
		if (attribute.name == this->condition.rhsAttr){
			rightAttribute = attribute;
			break;
		}
	}
	for (int i = 0; i < numPartitions; ++i){
		QueueSizePair queueSizePair;
		queueSizePair.size = 0;
		queueSizePair.page = new queue<void*>;
		partitionPages.push_back(queueSizePair);
	}
	joinNumber = 1;
	RelationManager *rm = RelationManager::instance();

	string stringPartitionNumber;
	stringstream stringStream;
	stringStream << joinNumber;
	stringStream >> stringJoinNumber;
	stringStream.clear();
	stringStream << 0;
	stringStream >> stringPartitionNumber;
	stringStream.clear();
	string tableName = "left_join" + stringJoinNumber + "_" + stringPartitionNumber;
	while (rm->createTable(tableName, leftAttributes) != 0){
		++joinNumber;
		stringStream << joinNumber;
		stringStream >> stringJoinNumber;
		stringStream.clear();
		tableName = "left_join" + stringJoinNumber + "_" + stringPartitionNumber;
	}
	tableName = "right_join" + stringJoinNumber + "_" + stringPartitionNumber;
	rm->createTable(tableName, rightAttributes);

	for (int i = 1; i < numPartitions; ++i){
		stringStream << i;
		stringStream >> stringPartitionNumber;
		stringStream.clear();
		tableName = "left_join" + stringJoinNumber + "_" + stringPartitionNumber;
		rm->createTable(tableName, leftAttributes);
		tableName = "right_join" + stringJoinNumber + "_" + stringPartitionNumber;
		rm->createTable(tableName, rightAttributes);
	}
	currentPartition = 0;
	probe = NULL;
	leftTableIn = NULL;
	rightTableIn = NULL;
	//Partitioning Phase
	createPartition();
//	probePhase = false;
}

GHJoin::~GHJoin(){
	RelationManager *rm = RelationManager::instance();
	stringstream stringStream;
	string stringPartitionNumber;
	for (int i = 0; i < numPartitions; ++i){
		stringStream << i;
		stringStream >> stringPartitionNumber;
		stringStream.clear();
		string leftTableName = "left_join" + stringJoinNumber + "_" + stringPartitionNumber;
		string rightTableName = "right_join" + stringJoinNumber + "_" + stringPartitionNumber;
		rm->deleteTable(leftTableName);
		rm->deleteTable(rightTableName);
	}
	for (int i = 0; i < numPartitions; ++i){
		delete(partitionPages[i].page);
	}
}

RC GHJoin::getNextTuple(void *data){
	//Partitioning Phase
//	if (!probePhase){
//		if (createPartition() == -1){
//			return -1;
//		}
//		probePhase = true;
//	}
	//Probing Phase

	stringstream stringStream;
	RelationManager *rm = RelationManager::instance();
	while (currentPartition < numPartitions){
		string stringPartitionNumber;
		stringStream << currentPartition;
		stringStream >> stringPartitionNumber;
		stringStream.clear();
		if (!probe){
			string leftTableName = "left_join" + stringJoinNumber + "_" + stringPartitionNumber;
			string rightTableName = "right_join" + stringJoinNumber + "_" + stringPartitionNumber;
			leftTableIn = new TableScan(*rm, leftTableName);
			rightTableIn = new TableScan(*rm, rightTableName);
			Condition partitionCondition = condition;
			partitionCondition.lhsAttr = leftTableName + "." + condition.lhsAttr;
			partitionCondition.rhsAttr = rightTableName + "." + condition.rhsAttr;
			probe = new BNLJoin(leftTableIn, rightTableIn, partitionCondition, numPartitions);
		}

		if (probe->getNextTuple(data) != QE_EOF){
			return 0;
		}



		if (leftTableIn){
			delete(leftTableIn);
		}
		if (rightTableIn){
			delete(rightTableIn);
		}
		if (probe){
			delete(probe);
		}

		probe = NULL;
		leftTableIn = NULL;
		rightTableIn = NULL;
		++currentPartition;
	}
	return QE_EOF;
}

void GHJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs = joinedAttributes;
}

RC GHJoin::createPartition(){
	RelationManager *rm = RelationManager::instance();
	void *tuple = malloc(PAGE_SIZE);
	memset(tuple, 0, PAGE_SIZE);
	while (leftIn->getNextTuple(tuple) != QE_EOF){
		if (insertPartition(tuple, LEFT_PARTITION) == -1){
			free(tuple);
			return -1;
		}
		memset(tuple, 0, PAGE_SIZE);
	}
	stringstream stringStream;

	for (int i = 0; i < numPartitions; ++i){
		string tableName = "left_join" + stringJoinNumber + "_";
		string stringPartitionNumber;
		stringStream << i;
		stringStream >> stringPartitionNumber;
		stringStream.clear();
		tableName += stringPartitionNumber;
		RID rid;
		while (!partitionPages[i].page->empty()){
			void *currentData = partitionPages[i].page->front();
			unsigned currentDataSize = getDataSize(currentData, leftAttributes);
			if (rm->insertTuple(tableName, currentData, rid) == -1){
				free(tuple);
				free(currentData);
				return -1;
			}
			partitionPages[i].page->pop();
			partitionPages[i].size -= currentDataSize;
			free(currentData);
		}
	}

	while (rightIn->getNextTuple(tuple) != QE_EOF){
		if (insertPartition(tuple, RIGHT_PARTITION) == -1){
			free(tuple);
			return -1;
		}
		memset(tuple, 0, PAGE_SIZE);
	}

	for (int i = 0; i < numPartitions; ++i){
		string tableName = "right_join" + stringJoinNumber + "_";
		string stringPartitionNumber;
		stringStream << i;
		stringStream >> stringPartitionNumber;
		stringStream.clear();
		tableName += stringPartitionNumber;
		RID rid;
		while (!partitionPages[i].page->empty()){
			void *currentData = partitionPages[i].page->front();
			unsigned currentDataSize = getDataSize(currentData, rightAttributes);
			if (rm->insertTuple(tableName, currentData, rid) == -1){
				free(tuple);
				free(currentData);
				return -1;
			}
			partitionPages[i].page->pop();
			partitionPages[i].size -= currentDataSize;
			free(currentData);
		}
	}
	free(tuple);
	return 0;
}

RC GHJoin::insertPartition(void *tuple, PartitionSide partitionSide){
	RelationManager *rm = RelationManager::instance();
	vector<Attribute> attrs;
	string tableName = "";
	Attribute conditionAttribute;
	switch (partitionSide){
	case LEFT_PARTITION:
		attrs = leftAttributes;
		tableName = "left_join" + stringJoinNumber + "_";
		conditionAttribute = leftAttribute;
		break;
	case RIGHT_PARTITION:
		attrs = rightAttributes;
		tableName = "right_join" + stringJoinNumber + "_";
		conditionAttribute = rightAttribute;
		break;
	}
	unsigned dataSize = getDataSize(tuple, attrs);
	void *insertedData = malloc(dataSize);
	memcpy(insertedData, tuple, dataSize);
	int nullFieldsIndicatorActualSize = (int)ceil(attrs.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy(nullsIndicator, insertedData, nullFieldsIndicatorActualSize);
    unsigned hash = 0;
    for (int i = 0, bytePosition = 0, offset = nullFieldsIndicatorActualSize; i < attrs.size(); ++i){
    	Attribute currentAttribute = attrs.at(i);
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
    	if (currentAttribute.name == conditionAttribute.name){
    		if(!getBit(nullsIndicator[bytePosition], i % 8)){
    			if (currentAttribute.type == TypeInt){
    				int key;
    				memcpy(&key, insertedData + offset, sizeof(int));
    				hash = intHash(key);
    			} else if (currentAttribute.type == TypeReal){
    				float key;
    				memcpy(&key, insertedData + offset, sizeof(float));
    				hash = realHash(key);
    			} else if (currentAttribute.type == TypeVarChar){
    				int length = 0;
    				memcpy(&length, insertedData + offset, sizeof(int));
    				char key[length + 1] = {0};
    				memcpy(key, insertedData + offset + sizeof(int), length);
    				string sKey = key;
    				hash = varcharHash(key);
    			}
    		}
    	}
		if(!getBit(nullsIndicator[bytePosition], i % 8)){
			if (currentAttribute.type == TypeInt){
				offset += sizeof(int);
			} else if (currentAttribute.type == TypeReal){
				offset += sizeof(float);
			} else if (currentAttribute.type == TypeVarChar){
				int length = 0;
				memcpy(&length, insertedData + offset, sizeof(int));
				offset += sizeof(int) + length;
			}
		}
    }
	unsigned partitionNumber = hash % numPartitions;
	string stringPartitionNumber;
	stringstream stringStream;
	stringStream << partitionNumber;
	stringStream >> stringPartitionNumber;
	stringStream.clear();
	tableName += stringPartitionNumber;
	if (partitionPages[partitionNumber].size + dataSize > PAGE_SIZE){
		RID rid;
		while (!partitionPages[partitionNumber].page->empty()){
			void *data = partitionPages[partitionNumber].page->front();
			unsigned currentDataSize = getDataSize(data, attrs);
			if (rm->insertTuple(tableName, data, rid) == -1){
				free(insertedData);
				free(data);
				return -1;
			}
			partitionPages[partitionNumber].page->pop();
			partitionPages[partitionNumber].size -= currentDataSize;
			free(data);
		}
	}


	partitionPages[partitionNumber].page->push(insertedData);
	partitionPages[partitionNumber].size += dataSize;
	return 0;
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op){
	this->input = input;
	this->aggAttr = aggAttr;
	this->op = op;
	this->groupAttr.type = TypeDefault;
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute groupAttr, AggregateOp op){
	this->input = input;
	this->aggAttr = aggAttr;
	this->op = op;
	this->groupAttr = groupAttr;
	intIt = intRealHashMap.end();
	realIt = realRealHashMap.end();
	varcharIt = varcharRealHashMap.end();
}

Aggregate::~Aggregate(){
}

RC Aggregate::getNextTuple(void *data){
	void *tuple = malloc(PAGE_SIZE);
	vector<Attribute> recordDescriptor;
	input->getAttributes(recordDescriptor);
	if (groupAttr.type == TypeDefault){
		float returnValue = 0;
		if (op == MIN){
			returnValue = FLT_MAX;
		} else if (op == MAX){
			returnValue = FLT_MIN;
		}
		float count = 0;
		RC rc = QE_EOF;
		while (input->getNextTuple(tuple) != QE_EOF){
			rc = 0;
//			++count;
			unsigned dataSize = getDataSize(tuple, recordDescriptor);
			void *actualTuple = malloc(dataSize);
			memcpy(actualTuple, tuple, dataSize);
			int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
		    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
		    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
		    memcpy(nullsIndicator, actualTuple, nullFieldsIndicatorActualSize);
		    for (int i = 0, bytePosition = 0, offset = nullFieldsIndicatorActualSize; i < recordDescriptor.size(); ++i){
		    	Attribute currentAttribute = recordDescriptor.at(i);
				if (i != 0 && i % 8 == 0){
					++bytePosition;
				}
		    	if (currentAttribute.name == aggAttr.name){
		    		if(!getBit(nullsIndicator[bytePosition], i % 8)){
		    			if (currentAttribute.type == TypeInt){
		    				int value = 0;
		    				memcpy(&value, actualTuple + offset, sizeof(int));
		    				calculate(returnValue, count, value);
		    			} else if (currentAttribute.type == TypeReal){
		    				float value = 0;
		    				memcpy(&value, actualTuple + offset, sizeof(float));
		    				calculate(returnValue, count, value);
		    			}
		    		}
		    		break;
		    	}
				if(!getBit(nullsIndicator[bytePosition], i % 8)){
					if (currentAttribute.type == TypeInt){
						offset += sizeof(int);
					} else if (currentAttribute.type == TypeReal){
						offset += sizeof(float);
					} else if (currentAttribute.type == TypeVarChar){
						int length = 0;
						memcpy(&length, actualTuple + offset, sizeof(int));
						offset += sizeof(int) + length;
					}
				}
		    }
		    free(actualTuple);
		}
		free(tuple);
		if (op == AVG){
			returnValue /= count;
		}
		char nullIndicator = {0};
		memset(&nullIndicator, 0, sizeof(char));
		memcpy(data, &nullIndicator, sizeof(char));
		memcpy(data + sizeof(char), &returnValue, sizeof(float));
		return rc;
	} else {
		RC rc = QE_EOF;
		while (input->getNextTuple(tuple) != QE_EOF){
			rc = 0;
			unsigned dataSize = getDataSize(tuple, recordDescriptor);
			void *actualTuple = malloc(dataSize);
			memcpy(actualTuple, tuple, dataSize);
			insertHashMap(actualTuple, recordDescriptor);
		    free(actualTuple);
		}
		if (rc == 0){
			if (groupAttr.type == TypeInt){
				intIt = intRealHashMap.begin();
			} else if (groupAttr.type == TypeReal){
				realIt = realRealHashMap.begin();
			} else if (groupAttr.type == TypeVarChar){
				varcharIt = varcharRealHashMap.begin();
			}
		}

		if (groupAttr.type == TypeInt){
			if (intIt == intRealHashMap.end()){
				free(tuple);
				return QE_EOF;
			} else {
				char nullIndicator = {0};
				memset(&nullIndicator, 0, sizeof(char));
				int offset = 0;
				memcpy(data + offset, &nullIndicator, sizeof(char));
				offset +=sizeof(char);
				int group = intIt->first;
				memcpy(data + offset, &group, sizeof(int));
				offset += sizeof(int);
				ValueCountPair valueCountPair = intIt->second;
				float aggregatedValue = valueCountPair.value;
				if (op == AVG){
					aggregatedValue /= valueCountPair.count;
				}
				memcpy(data + offset, &aggregatedValue, sizeof(float));
				++intIt;
				free(tuple);
				return 0;
			}
		} else if (groupAttr.type == TypeReal){
			if (realIt == realRealHashMap.end()){
				free(tuple);
				return QE_EOF;
			} else {
				char nullIndicator = {0};
				memset(&nullIndicator, 0, sizeof(char));
				int offset = 0;
				memcpy(data + offset, &nullIndicator, sizeof(char));
				offset +=sizeof(char);
				float group = realIt->first;
				memcpy(data + offset, &group, sizeof(float));
				offset += sizeof(float);
				ValueCountPair valueCountPair = realIt->second;
				float aggregatedValue = valueCountPair.value;
				if (op == AVG){
					aggregatedValue /= valueCountPair.count;
				}
				memcpy(data + offset, &aggregatedValue, sizeof(float));
				++realIt;
				free(tuple);
				return 0;
			}
		} else if (groupAttr.type == TypeVarChar){
			if (varcharIt == varcharRealHashMap.end()){
				free(tuple);
				return QE_EOF;
			} else {
				char nullIndicator = {0};
				memset(&nullIndicator, 0, sizeof(char));
				int offset = 0;
				memcpy(data + offset, &nullIndicator, sizeof(char));
				offset +=sizeof(char);
				string group = varcharIt->first;
				int length = group.length();
				memcpy(data + offset, &length, sizeof(int));
				offset += sizeof(int);
				memcpy(data + offset, group.c_str(), length);
				offset += length;
				ValueCountPair valueCountPair = varcharIt->second;
				float aggregatedValue = valueCountPair.value;
				if (op == AVG){
					aggregatedValue /= valueCountPair.count;
				}
				memcpy(data + offset, &aggregatedValue, sizeof(float));
				++varcharIt;
				free(tuple);
				return 0;
			}
		}
		free(tuple);
	}
	return QE_EOF;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	if (groupAttr.type != TypeDefault){
		attrs.push_back(groupAttr);
	}
	Attribute attribute = aggAttr;
	string name = "";
	switch(op){
	case MIN:
		name = "MIN(";
		break;
	case MAX:
		name = "MAX(";
		break;
	case COUNT:
		name = "COUNT(";
		break;
	case SUM:
		name = "SUM(";
		break;
	case AVG:
		name = "AVG(";
		break;
	}
	name += attribute.name + ")";
	attribute.name = name;
	attrs.push_back(attribute);
}

RC Aggregate::calculate(float &returnValue, float &count, const int &value){
	switch(op){
	case MIN:
		if (returnValue > value){
			returnValue = value;
		}
		break;
	case MAX:
		if (returnValue < value){
			returnValue = value;
		}
		break;
	case COUNT:
		++returnValue;
		++count;
		break;
	case SUM:
		returnValue += value;
		break;
	case AVG:
		returnValue += value;
		++count;
		break;
	}
	return 0;
}

RC Aggregate::calculate(float &returnValue, float &count, const float &value){
	switch(op){
	case MIN:
		if (returnValue > value){
			returnValue = value;
		}
		break;
	case MAX:
		if (returnValue < value){
			returnValue = value;
		}
		break;
	case COUNT:
		++returnValue;
		++count;
		break;
	case SUM:
		returnValue += value;
		break;
	case AVG:
		returnValue += value;
		++count;
		break;
	}
	return 0;
}

RC Aggregate::insertHashMap(void *data, const vector<Attribute> recordDescriptor){
	int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy(nullsIndicator, data, nullFieldsIndicatorActualSize);
    int offset = nullFieldsIndicatorActualSize;
    void *key = NULL;
    void *value = NULL;
    for (int i = 0, bytePosition = 0; i < recordDescriptor.size(); ++i){
    	Attribute currentAttribute = recordDescriptor.at(i);
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
    	if (currentAttribute.name == groupAttr.name){
    		if(!getBit(nullsIndicator[bytePosition], i % 8)){
    			if (currentAttribute.type == TypeInt){
    				key = malloc(sizeof(int));
    				memcpy(key, data + offset, sizeof(int));
    			} else if (currentAttribute.type == TypeReal){
    				key == malloc(sizeof(float));
    				memcpy(key, data + offset, sizeof(float));
    			} else if (currentAttribute.type == TypeVarChar){
    				int length = 0;
    				memcpy(&length, data + offset, sizeof(int));
    				key = malloc(sizeof(int) + length);
    				memcpy(key, data + offset, length);
    			}
    		}
    	}
    	if (currentAttribute.name == aggAttr.name){
    		if(!getBit(nullsIndicator[bytePosition], i % 8)){
    			if (currentAttribute.type == TypeInt){
    				value = malloc(sizeof(int));
    				memcpy(value, data + offset, sizeof(int));
    			} else if (currentAttribute.type == TypeReal){
    				value = malloc(sizeof(float));
    				memcpy(value, data + offset, sizeof(float));
    			}
    		}
    	}
		if(!getBit(nullsIndicator[bytePosition], i % 8)){
			if (currentAttribute.type == TypeInt){
				offset += sizeof(int);
			} else if (currentAttribute.type == TypeReal){
				offset += sizeof(float);
			} else if (currentAttribute.type == TypeVarChar){
				int length = 0;
				memcpy(&length, data + offset, sizeof(int));
				offset += sizeof(int) + length;
			}
		}
		if (key && value){
			if (groupAttr.type == TypeInt){
				int intKey = 0;
				int intValue = 0;
				float realValue = 0;
				memcpy(&intKey, key, sizeof(int));
				if (aggAttr.type == TypeInt){
					memcpy(&intValue, value, sizeof(int));
				} else {
					memcpy(&realValue, value, sizeof(float));
				}
				if (intRealHashMap.count(intKey) > 0){
					map<int, ValueCountPair>::iterator it = intRealHashMap.find(intKey);
					if (it != intRealHashMap.end()){
						ValueCountPair valueCountPair = it->second;
						switch(op){
						case MIN:
							if (aggAttr.type == TypeInt){
								if (valueCountPair.value > intValue){
									valueCountPair.value = intValue;
								}
							} else {
								if (valueCountPair.value > realValue){
									valueCountPair.value = realValue;
								}
							}
							break;
						case MAX:
							if (aggAttr.type == TypeInt){
								if (valueCountPair.value < intValue){
									valueCountPair.value = intValue;
								}
							} else {
								if (valueCountPair.value < realValue){
									valueCountPair.value = realValue;
								}
							}
							break;
						case COUNT:
							valueCountPair.count += 1;
							valueCountPair.value += 1;
							break;
						case SUM:
							if (aggAttr.type == TypeInt){
								valueCountPair.value += intValue;
							} else {
								valueCountPair.value += realValue;
							}
							break;
						case AVG:
							if (aggAttr.type == TypeInt){
								valueCountPair.value += intValue;
							} else {
								valueCountPair.value += realValue;
							}
							valueCountPair.count += 1;
							break;
						}
						it->second = valueCountPair;
					}
				} else {
					ValueCountPair valueCountPair;
					if (op == COUNT){
						valueCountPair.value = 1;
					} else if (aggAttr.type == TypeInt){
						valueCountPair.value = intValue;
					} else {
						valueCountPair.value = realValue;
					}
					valueCountPair.count = 1;
					intRealHashMap.emplace(intKey, valueCountPair);
				}
			} else if (groupAttr.type == TypeReal){
				float realKey = 0;
				int intValue = 0;
				float realValue = 0;
				memcpy(&realKey, key, sizeof(float));
				if (aggAttr.type == TypeInt){
					memcpy(&intValue, value, sizeof(int));
				} else {
					memcpy(&realValue, value, sizeof(float));
				}
				if (realRealHashMap.count(realKey) > 0){
					map<float, ValueCountPair>::iterator it = realRealHashMap.find(realKey);
					if (it != realRealHashMap.end()){
						ValueCountPair valueCountPair = it->second;
						switch(op){
						case MIN:
							if (aggAttr.type == TypeInt){
								if (valueCountPair.value > intValue){
									valueCountPair.value = intValue;
								}
							} else {
								if (valueCountPair.value > realValue){
									valueCountPair.value = realValue;
								}
							}
							break;
						case MAX:
							if (aggAttr.type == TypeInt){
								if (valueCountPair.value < intValue){
									valueCountPair.value = intValue;
								}
							} else {
								if (valueCountPair.value < realValue){
									valueCountPair.value = realValue;
								}
							}
							break;
						case COUNT:
							valueCountPair.count += 1;
							valueCountPair.value += 1;
							break;
						case SUM:
							if (aggAttr.type == TypeInt){
								valueCountPair.value += intValue;
							} else {
								valueCountPair.value += realValue;
							}
							break;
						case AVG:
							if (aggAttr.type == TypeInt){
								valueCountPair.value += intValue;
							} else {
								valueCountPair.value += realValue;
							}
							valueCountPair.count += 1;
							break;
						}
						it->second = valueCountPair;
					}
				} else {
					ValueCountPair valueCountPair;
					if (op == COUNT){
						valueCountPair.value = 1;
					} else if (aggAttr.type == TypeInt){
						valueCountPair.value = intValue;
					} else {
						valueCountPair.value = realValue;
					}
					valueCountPair.count = 1;
					realRealHashMap.emplace(realKey, valueCountPair);
				}
			} else if (groupAttr.type == TypeVarChar){
				int length = 0;
				memcpy(&length, key, sizeof(int));
				char varcharKey[length + 1] = {0};
				memcpy(varcharKey, key + sizeof(int), length);
				int intValue = 0;
				float realValue = 0;
				if (aggAttr.type == TypeInt){
					memcpy(&intValue, value, sizeof(int));
				} else {
					memcpy(&realValue, value, sizeof(float));
				}
				if (varcharRealHashMap.count(string(varcharKey)) > 0){
					map<string, ValueCountPair>::iterator it = varcharRealHashMap.find(string(varcharKey));
					if (it != varcharRealHashMap.end()){
						ValueCountPair valueCountPair = it->second;
						switch(op){
						case MIN:
							if (aggAttr.type == TypeInt){
								if (valueCountPair.value > intValue){
									valueCountPair.value = intValue;
								}
							} else {
								if (valueCountPair.value > realValue){
									valueCountPair.value = realValue;
								}
							}
							break;
						case MAX:
							if (aggAttr.type == TypeInt){
								if (valueCountPair.value < intValue){
									valueCountPair.value = intValue;
								}
							} else {
								if (valueCountPair.value < realValue){
									valueCountPair.value = realValue;
								}
							}
							break;
						case COUNT:
							valueCountPair.count += 1;
							valueCountPair.value += 1;
							break;
						case SUM:
							if (aggAttr.type == TypeInt){
								valueCountPair.value += intValue;
							} else {
								valueCountPair.value += realValue;
							}
							break;
						case AVG:
							if (aggAttr.type == TypeInt){
								valueCountPair.value += intValue;
							} else {
								valueCountPair.value += realValue;
							}
							valueCountPair.count += 1;
							break;
						}
						it->second = valueCountPair;
					}
				} else {
					ValueCountPair valueCountPair;
					if (op == COUNT){
						valueCountPair.value = 1;
					} else if (aggAttr.type == TypeInt){
						valueCountPair.value = intValue;
					} else {
						valueCountPair.value = realValue;
					}
					valueCountPair.count = 1;
					varcharRealHashMap.emplace(string(varcharKey), valueCountPair);
				}
			}
			free(key);
			free(value);
			return 0;
		}
    }
    if (key){
    	free(key);
    }
    if (value){
    	free(value);
    }
	return -1;
}

bool compareValues(const void *left, const void *right, const CompOp compOp, AttrType attrType){
	unsigned char nullIndicator;
	if(!left){
		switch(compOp){
		case EQ_OP:
			return right == NULL;
		case LT_OP:
			return false;
		case LE_OP:
			return right == NULL;
		case GT_OP:
			return right;
		case GE_OP:
			return right == NULL;
		case NE_OP:
			return right != NULL;
		case NO_OP:
			return true;
		default:
			return false;
		}
	}
	if (!right){
		if(compOp == NO_OP){
			return true;
		} else {
			return false;
		}
	}
	if (attrType == TypeInt){
		int leftValue;
		int rightValue;
		memcpy(&leftValue, left, sizeof(int));
		memcpy(&rightValue, right, sizeof(int));
		switch(compOp){
		case EQ_OP:
			return leftValue == rightValue;
		case LT_OP:
			return leftValue < rightValue;
		case LE_OP:
			return leftValue <= rightValue;
		case GT_OP:
			return leftValue > rightValue;
		case GE_OP:
			return leftValue >= rightValue;
		case NE_OP:
			return leftValue != rightValue;
		case NO_OP:
			return true;
		default:
			return false;
		}
	} else if (attrType == TypeReal){
		float leftValue;
		float rightValue;
		memcpy(&leftValue, left, sizeof(float));
		memcpy(&rightValue, right, sizeof(float));
		switch(compOp){
		case EQ_OP:
			return leftValue == rightValue;
		case LT_OP:
			return leftValue < rightValue;
		case LE_OP:
			return leftValue <= rightValue;
		case GT_OP:
			return leftValue > rightValue;
		case GE_OP:
			return leftValue >= rightValue;
		case NE_OP:
			return leftValue != rightValue;
		case NO_OP:
			return true;
		default:
			return false;
		}
	} else if (attrType == TypeVarChar){
		int leftVarcharLength = 0;
		memcpy(&leftVarcharLength, left, sizeof(int));
		char leftValue[leftVarcharLength + 1] = {0};
		int rightVarcharLength = 0;
		memcpy(&rightVarcharLength, right, sizeof(int));
		char rightValue[rightVarcharLength + 1] = {0};
		memcpy(&leftValue, left + sizeof(int), leftVarcharLength);
		memcpy(&rightValue, right + sizeof(int), rightVarcharLength);
		int comparison = strcmp(leftValue, rightValue);
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
		default:
			return false;
		}
	} else if (attrType == TypeDefault){
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
	}
	return false;
}

unsigned getDataSize(void *data, const vector<Attribute> &recordDescriptor){
	int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy(nullsIndicator, data, nullFieldsIndicatorActualSize);
    int offset = nullFieldsIndicatorActualSize;
	for (int i = 0, bytePosition = 0; i < recordDescriptor.size(); ++i){
		Attribute attribute = recordDescriptor.at(i);
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
		if(!getBit(nullsIndicator[bytePosition], i % 8)){
			if (attribute.type == TypeInt){
				offset += sizeof(int);
			} else if (attribute.type == TypeReal){
				offset += sizeof(float);
			} else if (attribute.type == TypeVarChar){
				int length = 0;
				memcpy(&length, data + offset, sizeof(int));
				offset += sizeof(int) + length;
			}
		}
    }
	return offset;
}

void printData(void *data, const vector<Attribute> &recordDescriptor){
	int nullFieldsIndicatorActualSize = (int)ceil(recordDescriptor.size() / 8.0);
    unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy(nullsIndicator, data, nullFieldsIndicatorActualSize);
    int offset = nullFieldsIndicatorActualSize;
	for (int i = 0, bytePosition = 0; i < recordDescriptor.size(); ++i){
		Attribute attribute = recordDescriptor.at(i);
		if (i != 0 && i % 8 == 0){
			++bytePosition;
		}
		if(!getBit(nullsIndicator[bytePosition], i % 8)){
			if (attribute.type == TypeInt){
				int value;
				memcpy(&value, data + offset, sizeof(int));
				std::cout<< attribute.name << ": " << value << std::endl;
				offset += sizeof(int);
			} else if (attribute.type == TypeReal){
				float value;
				memcpy(&value, data + offset, sizeof(float));
				std::cout<< attribute.name << ": " << value << std::endl;
				offset += sizeof(float);
			} else if (attribute.type == TypeVarChar){
				int length = 0;
				memcpy(&length, data + offset, sizeof(int));
				offset += sizeof(int);
				char value[length + 1] = {0};
				memcpy(value, data + offset, length);
				std::cout<< attribute.name << ": " << value << std::endl;
				offset += length;
			}
		} else {
			std::cout<< attribute.name << ": NULL" << std::endl;
		}
    }
}
