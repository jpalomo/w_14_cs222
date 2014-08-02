
#include "qe.h"

/**********************
 * three tool methods *
 **********************/

// read attribute from a tuple given its descriptor and attribute position and type,
void readField(const void *input, void *data, vector<Attribute> attrs, int attrPos, AttrType type) {
	int offset = 0;
	int attrLength = 0;

	for (int i = 0; i < attrPos; i++) {
		if (attrs[i].type == TypeInt)
			offset += sizeof(int);
		else if (attrs[i].type == TypeReal)
			offset += sizeof(float);
		else {
			int stringLength = *(int *)((char *)input + offset);
			offset += sizeof(int) + stringLength;
		}
	}

	if (type == TypeInt) {
		attrLength = sizeof(int);
	}
	else if (type == TypeReal) {
		attrLength = sizeof(float);
	}
	else {
		attrLength = *(int *)((char *)input + offset) + sizeof(int);
	}

	memcpy(data, (char *)input + offset, attrLength);
}


// get tuple length given tuple and descriptor
int getTupleLength(const void *tuple, vector<Attribute> attrs) {
	int result = 0;

	for (unsigned i = 0; i < attrs.size(); i++) {
		if (attrs[i].type == TypeInt)
			result += sizeof(int);
		else if (attrs[i].type == TypeReal)
			result += sizeof(float);
		else {
			int stringLength = *(int *)((char *)tuple + result);
			result += sizeof(int) + stringLength;
		}
	}

	return result;
}


// compare two attribute
bool compareField(const void *attribute, const void *condition, AttrType type, CompOp compOp) {
	if (condition == NULL)
		return true;

	bool result = true;

	switch (type) {
        case TypeInt: {
            int attr = *(int *)attribute;
            int cond = *(int *)condition;

            switch(compOp) {
                case EQ_OP: result = attr == cond; break;
                case LT_OP: result = attr < cond; break;
                case GT_OP: result = attr > cond; break;
                case LE_OP: result = attr <= cond; break;
                case GE_OP: result = attr >= cond; break;
                case NE_OP: result = attr != cond; break;
                case NO_OP: break;
            }

            break;
        }

        case TypeReal: {
            float attr = *(float *)attribute;
            float cond = *(float *)condition;

            int temp = 0;

            if (attr - cond > 0.00001)
            	temp = 1;
            else if (attr - cond < -0.00001)
            	temp = -1;

            switch(compOp) {
                case EQ_OP: result = temp == 0; break;
                case LT_OP: result = temp < 0; break;
                case GT_OP: result = temp > 0; break;
                case LE_OP: result = temp <= 0; break;
                case GE_OP: result = temp >= 0; break;
                case NE_OP: result = temp != 0; break;
                case NO_OP: break;
            }

            break;
        }

        case TypeVarChar: {
            int attriLeng = *(int *)attribute;
            string attr((char *)attribute + sizeof(int), attriLeng);
            int condiLeng = *(int *)condition;
            string cond((char *)condition + sizeof(int), condiLeng);

            switch(compOp) {
                case EQ_OP: result = strcmp(attr.c_str(), cond.c_str()) == 0; break;
                case LT_OP: result = strcmp(attr.c_str(), cond.c_str()) < 0; break;
                case GT_OP: result = strcmp(attr.c_str(), cond.c_str()) > 0; break;
                case LE_OP: result = strcmp(attr.c_str(), cond.c_str()) <= 0; break;
                case GE_OP: result = strcmp(attr.c_str(), cond.c_str()) >= 0;break;
                case NE_OP: result = strcmp(attr.c_str(), cond.c_str()) != 0; break;
                case NO_OP: break;
            }

            break;
        }
	}
	return result;
}


/****************
 * FIlTER CLASS *
 ****************/
Filter::Filter(Iterator* input, const Condition &condition) : itr(input) {
	attrs.clear();
	op = condition.op;
	this->condition = condition.rhsValue.data;
	conditionType = condition.rhsValue.type;

	// get the attributes from input Iterator
	this->itr->getAttributes(attrs);

	for (unsigned i = 0; i < attrs.size(); i++) {
		if (attrs[i].name.compare(condition.lhsAttr) == 0) {
			attrPos = i;
			value = malloc(attrs[i].length + sizeof(int));
			break;
		}
	}
}

Filter::~Filter() {
	free(value);
}

RC Filter::getNextTuple(void *data) {
	int returnValue = SUCCESS;

	do {
		returnValue = itr->getNextTuple(data);

		if (returnValue != SUCCESS)
			return returnValue;

		readField(data, this->value, attrs, attrPos, conditionType);
	}
	while (!compareField(this->value, condition, conditionType, op));

	return returnValue;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}


/*****************
 * PROJECT CLASS *
 *****************/
Project::Project(Iterator *input, const vector<string> &attrNames) : itr(input) {
	attrs.clear();
	oriAttrs.clear();

	itr->getAttributes(oriAttrs);

	unsigned i = 0, j = 0;
	for (; i < oriAttrs.size(); i++) {
		if (oriAttrs[i].name.compare(attrNames[j]) == 0) {
			attrs.push_back(oriAttrs[i]);
			j++;
		}
	}

	tuple = malloc(PAGE_SIZE);
}

Project::~Project() {
	free(tuple);
}

RC Project::getNextTuple(void *data) {
	int returnValue = SUCCESS;

	returnValue = itr->getNextTuple(tuple);

	if (returnValue != SUCCESS) {
		return returnValue;
	}

	projectFields(tuple, data, oriAttrs, attrs);
	return returnValue;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

void Project::projectFields(const void *input, void *data, vector<Attribute> inputAttrs, vector<Attribute> attrs) {
	unsigned i = 0, j = 0;
	int offsetI = 0, offsetJ = 0;

	while (i < inputAttrs.size() && j < attrs.size()) {
		// get the length of field input[i]
		int length = 0;
		if (inputAttrs[i].type == TypeInt)
			length = sizeof(int);
		else if (inputAttrs[i].type == TypeReal)
			length = sizeof(float);
		else {
			length = sizeof(int) + *(int *)((char *)input + offsetI);
		}

		// if same attribute as data[j] copy attribute and increase offset of data
		if (inputAttrs[i].name.compare(attrs[j].name) == 0) {
			memcpy((char *)data + offsetJ, (char *)input + offsetI, length);
			offsetJ += length;
			j++;
		}

		// go to next field
		i++;
		offsetI += length;
	}
}


/****************
 * NLJoin CLASS *
 ****************/
NLJoin::NLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages)
: leftItr(leftIn), rightItr(rightIn){
	attrs.clear();
	leftAttrs.clear();
	rightAttrs.clear();

	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);

	for (unsigned i = 0; i < leftAttrs.size(); i++) {
		attrs.push_back(leftAttrs[i]);

		if (leftAttrs[i].name.compare(condition.lhsAttr) == 0) {
			leftAttrPos = i;
			type = leftAttrs[i].type;
			leftValue = malloc(leftAttrs[i].length + sizeof(int));
		}
	}

	for (unsigned i = 0; i < rightAttrs.size(); i++) {
		attrs.push_back(rightAttrs[i]);

		if (rightAttrs[i].name.compare(condition.rhsAttr) == 0) {
			rightAttrPos = i;
			rightValue = malloc(rightAttrs[i].length + sizeof(int));
		}
	}

	op = condition.op;

	leftTuple = malloc(PAGE_SIZE);
	rightTuple = malloc(PAGE_SIZE);

	// read first tuple in left relation
	isEnd = leftItr->getNextTuple(leftTuple);

	// setIterator in TableScan
	rightItr->setIterator();

	if (isEnd != QE_EOF) {
		readField(leftTuple, leftValue, leftAttrs, leftAttrPos, type);
	}
}

NLJoin::~NLJoin() {
	free(leftValue);
	free(rightValue);
	free(leftTuple);
	free(rightTuple);
}

RC NLJoin::getNextTuple(void *data) {
	if (isEnd == QE_EOF)
		return isEnd;

	int returnValue = SUCCESS;
	bool result = false;

	do {
		// read next tuple from right relation
		returnValue = rightItr->getNextTuple(rightTuple);

		// if reaches the end of right relation
		if (returnValue == QE_EOF) {
			// read next tuple in left relation, if no, return eof
			isEnd = leftItr->getNextTuple(leftTuple);
			if (isEnd == QE_EOF) {
				return isEnd;
			}

			// read left value
			readField(leftTuple, leftValue, leftAttrs, leftAttrPos, type);

			// reset right relation
			rightItr->setIterator();

			// read first tuple in right relation
			returnValue = rightItr->getNextTuple(rightTuple);

			// if fails again, no tuple in right relation, return eof
			if (returnValue == QE_EOF)
				return QE_EOF;
		}

		// read attribute from right tuple
		readField(rightTuple, rightValue, rightAttrs, rightAttrPos, type);

		result = compareField(leftValue, rightValue, type, op);
	}
	while (!result);

	int leftTupleLength = getTupleLength(leftTuple, leftAttrs);
	int rightTupleLength = getTupleLength(rightTuple, rightAttrs);

	memcpy(data, leftTuple, leftTupleLength);
	memcpy((char *)data + leftTupleLength, rightTuple, rightTupleLength);

	return returnValue;
}

void NLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}


/*****************
 * INLJoin CLASS *
 *****************/
INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition, const unsigned numPages)
: leftItr(leftIn), rightItr(rightIn) {
	attrs.clear();
	leftAttrs.clear();
	rightAttrs.clear();

	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);

	for (unsigned i = 0; i < leftAttrs.size(); i++) {
		attrs.push_back(leftAttrs[i]);

		if (leftAttrs[i].name.compare(condition.lhsAttr) == 0) {
			leftAttrPos = i;
			type = leftAttrs[i].type;
			leftValue = malloc(leftAttrs[i].length + sizeof(int));
		}
	}

	for (unsigned i = 0; i < rightAttrs.size(); i++) {
		attrs.push_back(rightAttrs[i]);
	}

	op = condition.op;

	leftTuple = malloc(PAGE_SIZE);
	rightTuple = malloc(PAGE_SIZE);

	leftHalf = false;

	// read first tuple in left relation
	isEnd = leftItr->getNextTuple(leftTuple);

	if (isEnd != QE_EOF) {
		readField(leftTuple, leftValue, leftAttrs, leftAttrPos, type);

		void *lowKey = NULL;
		void *highKey = NULL;
		bool lowKeyInclusive = false;
		bool highKeyInclusive = false;

		setCondition(op, &lowKey, &highKey, lowKeyInclusive, highKeyInclusive);
		rightItr->setIterator(lowKey, highKey, lowKeyInclusive, highKeyInclusive);

		leftHalf = true;
	}
}


INLJoin::~INLJoin() {
	free(leftValue);

	free(leftTuple);
	free(rightTuple);
}


RC INLJoin::getNextTuple(void *data) {
	if (isEnd == QE_EOF)
		return isEnd;

	RC isSuccess = SUCCESS;

	do {
		isSuccess = rightItr->getNextTuple(rightTuple);

		// only for operator = NE_OP and rightItr finishes scanning lefthalf of index
		if (isSuccess == QE_EOF && leftHalf && op == NE_OP) {
			void *lowKey = NULL;
			void *highKey = NULL;
			bool lowKeyInclusive = false;
			bool highKeyInclusive = false;

			setCondition(op, &lowKey, &highKey, lowKeyInclusive, highKeyInclusive);
			rightItr->setIterator(lowKey, highKey, lowKeyInclusive, highKeyInclusive);

			leftHalf = false;
		}

		// finish index scanning
		else if (isSuccess == QE_EOF) {
			// read next tuple in left relation
			isEnd = leftItr->getNextTuple(leftTuple);

			// no more tuple in left relation, return eof
			if (isEnd == QE_EOF)
				return isEnd;

			// set index scan
			readField(leftTuple, leftValue, leftAttrs, leftAttrPos, type);

			void *lowKey = NULL;
			void *highKey = NULL;
			bool lowKeyInclusive = false;
			bool highKeyInclusive = false;

			setCondition(op, &lowKey, &highKey, lowKeyInclusive, highKeyInclusive);
			rightItr->setIterator(lowKey, highKey, lowKeyInclusive, highKeyInclusive);

			leftHalf = true;
		}

	}
	while (isSuccess == QE_EOF);

	int leftTupleLength = getTupleLength(leftTuple, leftAttrs);
	int rightTupleLength = getTupleLength(rightTuple, rightAttrs);

	memcpy(data, leftTuple, leftTupleLength);
	memcpy((char *)data + leftTupleLength, rightTuple, rightTupleLength);

	return isSuccess;

}


void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}


void INLJoin::setCondition(CompOp op, void **lowKey, void **highKey, bool &lowKeyInclusive, bool &highKeyInclusive) {
	switch (op) {
		case EQ_OP: {
			*lowKey = leftValue;
			*highKey = leftValue;
			lowKeyInclusive = true;
			highKeyInclusive = true;
			break;
		}
		case LT_OP: {
			*lowKey = leftValue;
			*highKey = NULL;
			lowKeyInclusive = false;
			highKeyInclusive = false;
			break;
		}
		case GT_OP: {
			*lowKey = NULL;
			*highKey = leftValue;
			lowKeyInclusive = false;
			highKeyInclusive = false;
			break;
		}
		case LE_OP: {
			*lowKey = leftValue;
			*highKey = NULL;
			lowKeyInclusive = true;
			highKeyInclusive = false;
			break;
		}
		case GE_OP: {
			*lowKey = NULL;
			*highKey = leftValue;
			lowKeyInclusive = true;
			highKeyInclusive = false;
			break;
		}
		case NO_OP: {
			*lowKey = NULL;
			*highKey = NULL;
			lowKeyInclusive = false;
			highKeyInclusive = false;
			break;
		}
		case NE_OP: {
			if (leftHalf) {
				*lowKey = leftValue;
				*highKey = NULL;
			}
			else {
				*lowKey = NULL;
				*highKey = leftValue;
			}
			lowKeyInclusive = false;
			highKeyInclusive = false;
			break;
		}
	}
}



/*******************
 * AGGREGATE CLASS *
 *******************/

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
    
    this->type = aggAttr.type;
    this->op = op;
    this->aggrAttribute = aggAttr;
    this->itr = input;
    
    short field_no = 0;
    int max_tuple_size = 0;
    isNextTuple = true;

    input->getAttributes(tblAttributes);
    for(int i = 0; i < tblAttributes.size(); i++) {
        
        Attribute attribute = tblAttributes[i];
        if (attribute.type == TypeVarChar) {
            max_tuple_size += sizeof(int);
        }
        
        string attrName = tblAttributes[i].name;
        if(attrName.compare(aggAttr.name) == 0) {
            field_no = i;
            max_tuple_size += tblAttributes[i].length;
            break;
        }
        max_tuple_size += tblAttributes[i].length;
    }
    
    this->attrPos = field_no;
    this->max_tuple_size = max_tuple_size;

}
void Aggregate::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    Attribute attr;
    string name (tblAttributes[attrPos].name);
    string close (")");
    
    if (op == MIN){
        string min ("MIN(");
        attr.name = min + name + close;
    }
    else if (op == MAX) {
        string max ("MAX(");
        attr.name = max + name + close;
    }
    else if (op == COUNT) {
        string count ("COUNT(");
        attr.name = count + name + close;
    }
    else if (op == AVG) {
        string avg ("AVG(");
        attr.name = avg + name + close;
    }
    else if (op == SUM) {
        string sum ("SUM(");
        attr.name = sum + name + close;
    }
    attrs.push_back(attr);
}

RC Aggregate::getNextTuple(void *data) {

    if (isNextTuple == false){
        return QE_EOF;
    }
    
    if (op == MIN){
        return getMin(data);
    }
    else if (op == MAX) {
        return getMax(data);
    }
    else if (op == COUNT) {
        return getCount(data);
    }
    else if (op == AVG) {
        return getAvg(data);
    }
    else if (op == SUM) {
        return getSum(data);
    }
    return QE_EOF;
}

RC Aggregate::getSum(void *data) {
    void * fieldData = malloc(max_tuple_size);
    void * runningSum = malloc(sizeof(int));
    
    if (type == TypeInt){
        *((int*)runningSum) = 0;
    }
    else{
        *((float*)runningSum) = 0.0;
    }
    
    float count = 0;
    while (itr->getNextTuple(fieldData) != QE_EOF){
        count++;
        short currentField = 0;
        char * dataPtr = (char*) fieldData;
        
        //go to the beginning of the field we want to read for current tuple
        while(currentField != attrPos) {
            if (tblAttributes[currentField].type == TypeVarChar) {
                int varCharLength;
                memcpy(&varCharLength, dataPtr, sizeof(int));
                dataPtr += sizeof(int) + varCharLength;
            }
            else{
                dataPtr += sizeof(int);
            }
            ++currentField;
        }
        
        //read the field
        if (type == TypeInt) {
            *((int*)runningSum) += *((int*)dataPtr);
        }
        else {
            float fieldValue;
            float currentSum;
            
            memcpy(&currentSum, runningSum, sizeof(int));
            memcpy(&fieldValue, dataPtr, sizeof(int));
            
            currentSum += fieldValue;
            memcpy(runningSum, &currentSum, sizeof(int));
        }
    }
    memcpy(data, runningSum, sizeof(int));
    
    free(runningSum);
    free(fieldData);
    
    isNextTuple = false;
    return 0;
}

RC Aggregate::getMin(void *data) {
    void * fieldData = malloc(max_tuple_size);
    void * minimum = malloc(sizeof(int));
    
    if (type == TypeInt){
        int max = INT_MAX;
        memcpy(minimum, &max, sizeof(int));
    }
    else{
        float max = FLT_MAX;
        memcpy(minimum, &max, sizeof(int));
    }

    while (itr->getNextTuple(fieldData) != QE_EOF){
        short currentField = 0;
        char * dataPtr = (char*) fieldData;
        
        //go to the beginning of the field we want to read for current tuple
        while(currentField != attrPos) {
            if (tblAttributes[currentField].type == TypeVarChar) {
                int varCharLength;
                memcpy(&varCharLength, dataPtr, sizeof(int));
                dataPtr += sizeof(int) + varCharLength;
            }
            else{
                dataPtr += sizeof(int);
            }
            ++currentField;
        }
        
        //read the field
        if (type == TypeInt) {
            int fieldValue;
            int currentMin;
            
            memcpy(&currentMin, minimum, sizeof(int));
            memcpy(&fieldValue, dataPtr, sizeof(int));
            
            if(fieldValue < currentMin) {
                memcpy(minimum, dataPtr, sizeof(int));
            }
        }
        else {
            float fieldValue;
            float currentMin;
            
            memcpy(&currentMin, minimum, sizeof(int));
            memcpy(&fieldValue, dataPtr, sizeof(int));
            
            if(fieldValue < currentMin) {
                memcpy(minimum, dataPtr, sizeof(int));
            }
        }
    }
    
    memcpy(data, minimum, sizeof(int));
    
    free(minimum);
    free(fieldData);
    
    isNextTuple = false;
    return 0;
}

RC Aggregate::getMax(void *data) {
    void * fieldData = malloc(max_tuple_size);
    void * maximum = malloc(sizeof(int));
    
    if (type == TypeInt){
        int max = INT_MIN;
        memcpy(maximum, &max, sizeof(int));
    }
    else{
        float max = FLT_MIN;
        memcpy(maximum, &max, sizeof(int));
    }
    
    while (itr->getNextTuple(fieldData) != QE_EOF){
        short currentField = 0;
        char * dataPtr = (char*) fieldData;
        
        //go to the beginning of the field we want to read for current tuple
        while(currentField != attrPos) {
            if (tblAttributes[currentField].type == TypeVarChar) {
                int varCharLength;
                memcpy(&varCharLength, dataPtr, sizeof(int));
                dataPtr += sizeof(int) + varCharLength;
            }
            else{
                dataPtr += sizeof(int);
            }
            ++currentField;
        }
        
        //read the field
        if (type == TypeInt) {
            int fieldValue;
            int currentMax;
            
            memcpy(&currentMax, maximum, sizeof(int));
            memcpy(&fieldValue, dataPtr, sizeof(int));
            
            if(fieldValue > currentMax) {
                memcpy(maximum, dataPtr, sizeof(int));
            }
        }
        else {
            float fieldValue;
            float currentMax;
            
            memcpy(&currentMax, maximum, sizeof(int));
            memcpy(&fieldValue, dataPtr, sizeof(int));
            
            if(fieldValue > currentMax) {
                memcpy(maximum, dataPtr, sizeof(int));
            }
        }
    }
    
    memcpy(data, maximum, sizeof(int));
    
    free(maximum);
    free(fieldData);
    
    isNextTuple = false;
    return 0;
}

RC Aggregate::getCount(void *data) {
    void * fieldData = malloc(max_tuple_size);
    
    int count = 0;
    while (itr->getNextTuple(fieldData) != QE_EOF){
        count++;
    }
    
    memcpy(data, &count, sizeof(int));
    free(fieldData);
    
    isNextTuple = false;
    return 0;
}

RC Aggregate::getAvg(void *data) {
    void * fieldData = malloc(max_tuple_size);
    void * runningSum = malloc(sizeof(int));
    
    if (type == TypeInt){
        *((int*)runningSum) = 0;
    }
    else{
        *((float*)runningSum) = 0.0;
    }
    
    float count = 0;
    while (itr->getNextTuple(fieldData) != QE_EOF){
        count++;
        short currentField = 0;
        char * dataPtr = (char*) fieldData;
        
        //go to the beginning of the field we want to read for current tuple
        while(currentField != attrPos) {
            if (tblAttributes[currentField].type == TypeVarChar) {
                int varCharLength;
                memcpy(&varCharLength, dataPtr, sizeof(int));
                dataPtr += sizeof(int) + varCharLength;
            }
            else{
                dataPtr += sizeof(int);
            }
            ++currentField;
        }
        
        //read the field
        if (type == TypeInt) {
            int fieldValue;
            int currentSum;
            
            memcpy(&currentSum, runningSum, sizeof(int));
            memcpy(&fieldValue, dataPtr, sizeof(int));
            
            currentSum += fieldValue;
            memcpy(runningSum, &currentSum, sizeof(int));
        }
        else {
            float fieldValue;
            float currentSum;
            
            memcpy(&currentSum, runningSum, sizeof(int));
            memcpy(&fieldValue, dataPtr, sizeof(int));
            
            currentSum += fieldValue;
            memcpy(runningSum, &currentSum, sizeof(int));
        }
    }
    
    if(type == TypeInt) {
        int currentRunningSum;
        memcpy(&currentRunningSum, runningSum, sizeof(int));
        
        float avg = currentRunningSum / (float)count;
        
        memcpy(data, &avg, sizeof(int));
    }
    else{
        float currentRunningSum;
        memcpy(&currentRunningSum, runningSum, sizeof(int));
        
        float avg = currentRunningSum / (float) count;
        
        memcpy(data, &avg, sizeof(int));
    }
    
    free(runningSum);
    free(fieldData);
    
    isNextTuple = false;
    return 0;
}




