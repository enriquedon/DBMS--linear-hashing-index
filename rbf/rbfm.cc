#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
	pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager() {
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	try {
		const char *name = fileName.c_str();
		return pfm->createFile(name);

	} catch (int e) {
		return e;
	}
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	try {
		const char *name = fileName.c_str();
		return pfm->destroyFile(name);

	} catch (int e) {
		return e;
	}
}

RC RecordBasedFileManager::openFile(const string &fileName,
		FileHandle &fileHandle) {
	try {
		const char *name = fileName.c_str();
		return pfm->openFile(name, fileHandle);
	} catch (int e) {
		return e;
	}

}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	try {
		return pfm->closeFile(fileHandle);
	} catch (int e) {
		return e;
	}
}

RC RecordBasedFileManager::newPageInit(FileHandle &fileHandle,
		unsigned int pagenum) {
	try {
		void *data = malloc(PAGE_SIZE);
		unsigned int freePointer = 0;
		unsigned int slotSize = 0;
		memcpy((char*) data + PAGE_SIZE - sizeof(int), &freePointer,
				sizeof(int));
		memcpy((char*) data + PAGE_SIZE - 2 * sizeof(int), &slotSize,
				sizeof(int));
		fileHandle.writePage(pagenum, data);
		free(data);
		return 0;
	} catch (int e) {
		return e;
	}
}

unsigned int RecordBasedFileManager::dataToRecord(
		const vector<Attribute> &recordDescription, const void *data,
		void *record) {
	unsigned size = recordDescription.size();
	unsigned offset = 0;
	unsigned intLength = sizeof(int);
	unsigned tmp;
	for (int i = 0; i < size; i++) {
		tmp = offset + (1 + size) * intLength;
		memcpy((char*) record + intLength * i, &tmp, intLength);
        if (recordDescription[i].length == -1) {
            continue;
        }
		if (recordDescription[i].type == 0) {
			offset += intLength;
		} else if (recordDescription[i].type == 1) {
			offset += sizeof(float);
		} else {
			unsigned int varLength;
			memcpy(&varLength, (char *) data + offset, intLength);
			offset = offset + intLength + varLength;
		}
	}
	//copy data to the new address
	tmp = offset + (1 + size) * intLength;
	memcpy((char*) (record) + (1 + size) * intLength, (char*) data, offset);
	memcpy((char *) record + size * intLength, &tmp, intLength);
	return offset + (1 + size) * intLength;
}
unsigned int RecordBasedFileManager::getValidPage(FileHandle &fileHandle,
		unsigned int recordLength) {
	unsigned int pageCount = 0;
	unsigned int pageNum = fileHandle.getNumberOfPages();
	while (pageCount < pageNum) {
		unsigned freespace = getFreeSpace(fileHandle, pageCount);
		if (recordLength + 2 * sizeof(int) < freespace) {
			return pageCount;
		}
		pageCount++;
	}
	void *data = malloc(PAGE_SIZE);
	fileHandle.appendPage(data);
	newPageInit(fileHandle, pageCount);
	free(data);
	return pageCount;
}
unsigned int RecordBasedFileManager::getFreeSpace(FileHandle &fileHandle,
		unsigned int pageNum) {
	unsigned int freeSpace;

	void *data = malloc(2 * sizeof(int));
	fileHandle.readMeta(pageNum, data);
	memcpy(&freeSpace, (char*) data + sizeof(int), sizeof(int));
	unsigned int slotSize;
	memcpy(&slotSize, (char*) data, sizeof(int));
	free(data);

	return PAGE_SIZE - freeSpace - (2 + slotSize * 2) * sizeof(int);
}

unsigned int RecordBasedFileManager::insertToPage(FileHandle &fileHandle,
		unsigned int pageNum, void *record, unsigned recordLength) {
	void *data = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum, data);
	unsigned freePointer;
	memcpy(&freePointer, (char*) data + PAGE_SIZE - sizeof(int), sizeof(int)); //get freePointer
	unsigned slotSize;
	memcpy(&slotSize, (char*) data + PAGE_SIZE - 2 * sizeof(int), sizeof(int)); //get slotSize
	slotSize += 1;
	memcpy((char*) data + freePointer, (char*) record, recordLength); //write record
	unsigned slotOffset = PAGE_SIZE - (2 + slotSize * 2) * sizeof(int);
	memcpy((char*) data + slotOffset, &freePointer, sizeof(int)); //modify slotDirectory
	memcpy((char*) data + slotOffset + sizeof(int), &recordLength, sizeof(int));
	freePointer += recordLength;
	memcpy((char*) data + PAGE_SIZE - sizeof(int), &freePointer, sizeof(int)); //change freePointer
	memcpy((char*) data + PAGE_SIZE - 2 * sizeof(int), &slotSize, sizeof(int)); //change slotSize
	fileHandle.writePage(pageNum, data);
	free(data);
	return slotSize;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	try {
		void *record = malloc(PAGE_SIZE);
		unsigned int recordLength = dataToRecord(recordDescriptor, data,
				record);       //convert data to formatted record
		rid.pageNum = getValidPage(fileHandle, recordLength);      //get pageNum
		rid.slotNum = insertToPage(fileHandle, rid.pageNum, record,
				recordLength);      //get slotNum
		free(record);
		return 0;
	} catch (int e) {
		return e;
	}
}
/*
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	void *record = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, record);
	int slotTotal;
	memcpy(&slotTotal, (char*) record + PAGE_SIZE - 2 * sizeof(int), sizeof(int));                    //read offset
    if (slotTotal < rid.slotNum)
    {
        return -1;
    }
	unsigned int offset;
	int length;
	memcpy(&offset, (char*) record + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int), sizeof(int));                    //read offset
	memcpy(&length, (char*) record + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int)+ sizeof(int), sizeof(int));      //read length
	if (length == -1) {
		return -2;
	}
	if (length == -2) {
		RID newRid;
		memcpy(&newRid.pageNum, (char*) record + offset, sizeof(int));
		memcpy(&newRid.slotNum, (char*) record + offset + sizeof(int),sizeof(int));
		return readRecord(fileHandle, recordDescriptor, newRid, data);
	}
	unsigned int dataLength = length - sizeof(int) * (recordDescriptor.size() + 1);
	unsigned int dataOffset = offset + sizeof(int) * (recordDescriptor.size() + 1);
	memcpy((char*) data, (char*) record + dataOffset, dataLength);
    free(record);
	return 0;
}
*/
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    void *record = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, record);
    unsigned slotTotal;
    memcpy(&slotTotal, (char*) record + PAGE_SIZE - 2 * sizeof(int), sizeof(int));                    //read offset
    if (slotTotal < rid.slotNum)
    {
        return -1;
    }
    unsigned int offset;
    int length;
    memcpy(&offset, (char*) record + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int), sizeof(int));                    //read offset
    memcpy(&length, (char*) record + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int)+ sizeof(int), sizeof(int));      //read length
    if (length == -1) {
        return -2;
    }
    if (length == -2) {
        RID newRid;
        memcpy(&newRid.pageNum, (char*) record + offset, sizeof(int));
        memcpy(&newRid.slotNum, (char*) record + offset + sizeof(int),sizeof(int));
        return readRecord(fileHandle, recordDescriptor, newRid, data);
    }
    int offsetIndata = 0;
    for (unsigned i=0; i<recordDescriptor.size(); i++) {
        int attributeLocation;
        memcpy(&attributeLocation, (char*)record+offset+sizeof(int)*i, sizeof(int));
        if (attributeLocation == length)
        {
            return 0;
        }
        if(recordDescriptor[i].length == -1)
        {
            continue;
        }
        int nextAttributeLocation;
        memcpy(&nextAttributeLocation, (char*)record+offset+sizeof(int)*(1+i), sizeof(int));
        memcpy((char*)data+offsetIndata, (char*)record+ offset+attributeLocation,nextAttributeLocation-attributeLocation);
        offsetIndata+=nextAttributeLocation-attributeLocation;
    }
    free(record);
    return 0;
}
RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	int *in;
	float *fl;
	char *ch;
	int offset = 0;
	try {
		for (unsigned i = 0; i < recordDescriptor.size(); i++) {
			switch (recordDescriptor[i].type) {
			case 0:
				in = (int *) ((char *) data + offset);
				cout << *in << "\n";
				offset += sizeof(int);
				break;
			case 1:
				fl = (float *) ((char *) data + offset);
				cout << *fl << "\n";
				offset += sizeof(int);
				break;
			case 2:
				in = (int *) ((char *) data + offset);
				ch = (char *) data + offset + sizeof(int);
				for (int j = 0; j < *in; j++)
				cout << *(ch + j);
				cout << "\n";
				offset += *in + sizeof(int);
				break;
			}
		}
		return 0;
	} catch (int e) {
		return e;
	}
}
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data) {
	void *record = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, record);
	unsigned int offset;
	memcpy(&offset,(char*) record + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int),sizeof(int));                    //read offset
    int length;
    memcpy(&length, (char*) record + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int)+ sizeof(int), sizeof(int));      //read length
    if( length == -1)
    {
        return -1;
    }
    if (length == -2) {
        RID newRid;
        memcpy(&newRid.pageNum, (char*) record + offset, sizeof(int));
        memcpy(&newRid.slotNum, (char*) record + offset + sizeof(int),sizeof(int));
        return readAttribute(fileHandle, recordDescriptor, newRid, attributeName, data);
    }
	for (unsigned i = 0; i < recordDescriptor.size(); i++) {
		if (recordDescriptor[i].name == attributeName) {
            //maybe wrong when add attribute happens
            if (recordDescriptor[i].length == -1)
            {
                return -1;
            }
			unsigned attributeLocation;
			memcpy(&attributeLocation,(char*) record + offset + i * sizeof(int), sizeof(int));
			if (recordDescriptor[i].type == 2) {
				unsigned length;
				memcpy(&length, (char*) record + offset + attributeLocation,
						sizeof(int));
				memcpy(data, &length, sizeof(int));
				attributeLocation += sizeof(int);
				memcpy((char*) data + sizeof(int),
						(char*) record + offset + attributeLocation, length);
			} else {
				memcpy(data, (char*) record + offset + attributeLocation,
						sizeof(int));
			}
			free(record);
			return 0;
		}

	}
	free(record);
	return 1;
}
unsigned int RecordBasedFileManager::getValidPageForUpdate(FileHandle &fileHandle, unsigned int recordLength, unsigned num) {
	unsigned int pageCount = num + 1;
	unsigned int pageNum = fileHandle.getNumberOfPages();
	while (pageCount < pageNum) {
		unsigned freespace = getFreeSpace(fileHandle, pageCount);
		if (recordLength + 2 * sizeof(int) < freespace) {
			return pageCount;
		}
		pageCount++;
	}
	void *data = malloc(PAGE_SIZE);
	fileHandle.appendPage(data);
	newPageInit(fileHandle, pageCount);
	free(data);
	return pageCount;
}
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,const vector<Attribute> &recordDescriptor, const void *data,const RID &rid)
{
	void *page = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, page);
	int length;
	memcpy(&length,(char*) page + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int)+ sizeof(int), sizeof(int));                   //read length
	if (length == -1) {
		return -1;
	}
	if (length == -2) {
		unsigned offset;
		memcpy(&offset,(char*) page + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int), sizeof(int));                   //read offset
		RID newRid;
		memcpy(&newRid.pageNum, (char*) page + offset, sizeof(int));
		memcpy(&newRid.slotNum, (char*) page + offset + sizeof(int),sizeof(int));
		return updateRecord(fileHandle, recordDescriptor, data, newRid);
	}

	unsigned offset;
	memcpy(&offset, (char*) page + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int), sizeof(int));                   //read offset
	void *record = malloc(PAGE_SIZE);
	int newLength = dataToRecord(recordDescriptor, data, record); //get the length of the updated record
	if (newLength > length) {
		int mark = -2;
		memcpy((char*) page + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int)+ sizeof(int), &mark, sizeof(int)); //mark -2
		RID newRid;
		newRid.pageNum = getValidPageForUpdate(fileHandle, newLength,rid.pageNum); //get the number of page where the new record is stored
		newRid.slotNum = insertToPage(fileHandle, newRid.pageNum, record, newLength);   //get the new slotNum and insert the record
		free(record);
		memcpy((char*) page + offset, &newRid.pageNum, sizeof(int)); //write slotNumber
		memcpy((char*) page + offset + sizeof(int), &newRid.slotNum,
				sizeof(int));       //write pageNumber
		fileHandle.writePage(rid.pageNum, page);
		free(page);
	} else {
		memcpy((char*) page + offset, record, newLength);   //update record data
		memcpy((char*) page + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int)+ sizeof(int), &newLength, sizeof(int)); //update record length
		fileHandle.writePage(rid.pageNum, page);
		free(page);
	}
	return 0;

}

RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle) {
	try {
		//the data in the file is a little be strange
		unsigned NumberOfPages = fileHandle.getNumberOfPages();
		if (NumberOfPages >= 1) {
			//int pagenum = 1;      //reserve the DIRpage.
			unsigned pagenum = 0;
			while (pagenum < NumberOfPages) {
				newPageInit(fileHandle, pagenum);
				pagenum++;
				//cout << pagenum<< endl;
			}
			return 0;
		}
		return -1;
	} catch (int e) {
		return e;
	}
	//void *data = malloc(PAGE_SIZE);
	//fileHandle.readPage(0,data);//read the DIRpage.
	//PagedFileManager::instance()->destroyFile();//cant get the file name
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {

	//cant set length = -1 on the final page
	int delPointer = -1;
	if (rid.pageNum >= fileHandle.getNumberOfPages()) {
		return -1;
	}
	void *Page = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, Page);

	unsigned numSlot;
	memcpy(&numSlot, (char*) Page + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
	if (rid.slotNum > numSlot) {
		//cout << "error on slotnum" << endl;
		return -1;
	}

	int length;
	memcpy(&length,
			(char*) Page + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int)
					+ sizeof(int), sizeof(int));
	if (length == -2) {
		unsigned offset;
		memcpy(&offset,
				(char*) Page + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int),
				sizeof(int));                   //read offset
		RID newRid;
		memcpy(&newRid.pageNum, (char*) Page + offset, sizeof(int));
		memcpy(&newRid.slotNum, (char*) Page + offset + sizeof(int),
				sizeof(int)); //read the rid from the 8 byte formerly at the head of a record

		memcpy(
				(char*) Page + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int)
						+ sizeof(int), &delPointer, sizeof(int));
		deleteRecord(fileHandle, recordDescriptor, newRid);
	} else {
		memcpy(
				(char*) Page + PAGE_SIZE - (1 + rid.slotNum) * 2 * sizeof(int)
						+ sizeof(int), &delPointer, sizeof(int));
	}
	fileHandle.writePage(rid.pageNum, Page);
	//cout<<"write"<<rid.pageNum<<endl;
	free(Page);
	return 0;

}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle,const vector<Attribute> &recordDescriptor, const unsigned pageNumber) {
	if (pageNumber >= fileHandle.getNumberOfPages()) {
		return -1;
	}
	void *Page = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNumber, Page);
	int numDir;
	memcpy(&numDir, (char *) Page + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
	//int numDir = *(int *) ((char *) Page + PAGE_SIZE - 2 * sizeof(int));
	int MagnetPointer = 0;
	for (int i = 1; i <= numDir; i++) {
		int length;
		memcpy(&length, (char *) Page + PAGE_SIZE - (2 * i + 1) * sizeof(int), sizeof(int));    //offset,length
		if (length == -1) {
			//cout << "MagnetPointer-1:" << MagnetPointer << endl;
		} else if (length == -2) {
			int offset;
			memcpy(&offset,(char *) Page + PAGE_SIZE - 2 * (i + 1) * sizeof(int),sizeof(int));    //get offset of record
			memcpy((char *) Page + MagnetPointer, (char *) Page + offset,sizeof(int) * 2);
			MagnetPointer += sizeof(int) * 2;
			//cout << "MagnetPointer-2:" << MagnetPointer << endl;
		} else {
			int offset;
			memcpy(&offset,(char *) Page + PAGE_SIZE - 2 * (i + 1) * sizeof(int), sizeof(int));    //get offset of record
			memcpy((char *) Page + MagnetPointer, (char *) Page + offset,length);    //move record
			memcpy((char *) Page + PAGE_SIZE - 2 * (i + 1) * sizeof(int),&MagnetPointer, sizeof(int));    //update offset in DIR
			//cout << "MagnetPointer:" << MagnetPointer << endl;
			MagnetPointer += length;
		}
		//cout << "numDir:" << i << endl;
	}
	memcpy((char *) Page + PAGE_SIZE - sizeof(int), &MagnetPointer,sizeof(int)); //update the freeOffset
	fileHandle.writePage(pageNumber, Page);
	free(Page);
	return 0;
}
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
        const vector<Attribute> &recordDescriptor,
        const string &conditionAttribute,
        const CompOp compOp,                  // comparision type such as "<" and "="
        const void *value,                    // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RBFM_ScanIterator &rbfm_ScanIterator)
{
    
    rbfm_ScanIterator.init(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    return 0;
}
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    if(currRid.pageNum > fileHandle.getNumberOfPages())
    {
        return -1;
    }
    RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
    void *currPage=malloc(PAGE_SIZE);
    void * attrValue=malloc(PAGE_SIZE);
    while (currRid.pageNum < fileHandle.getNumberOfPages()) {
        fileHandle.readPage(currRid.pageNum,currPage);
        unsigned slotTotal;
        memcpy(&slotTotal, (char *)currPage + PAGE_SIZE - 2*sizeof(int), sizeof(int));
        if (slotTotal <  currRid.slotNum)
        {   currRid.pageNum += 1;
            currRid.slotNum = 1;
            continue;
        }
        else
        {   int length;
            memcpy(&length, (char*) currPage + PAGE_SIZE - (1 + currRid.slotNum) * 2 * sizeof(int)+ sizeof(int), sizeof(int));
            if (length == -1 or length == -2)
            {
                currRid.slotNum += 1;
                continue;
            }
            rbfm->readAttribute(fileHandle, recordDescriptor, currRid, conditionAttribute, attrValue);
            if (checkValue(attrValue))       //attrbute satisfied
            {
                free(currPage);
                free(attrValue);
                void *record = malloc(PAGE_SIZE);
                rbfm->readRecord(fileHandle, recordDescriptor, currRid, record);
                selectAttrFromRecord(data, record);
                free(record);
                rid.pageNum = currRid.pageNum;
                rid.slotNum = currRid.slotNum;
                currRid.slotNum += 1;
                return 0;
            }
            else
            {
                currRid.slotNum += 1;
            }
        }
        
    }
    cout<<"page of numbers:"<<fileHandle.getNumberOfPages()<<endl;
    cout<<"Currentrid"<<currRid.pageNum<<" "<<currRid.slotNum<<endl;
    free(currPage);
    free(attrValue);
    return RBFM_EOF;
};
RC RBFM_ScanIterator::selectAttrFromRecord(void *data, void *record)
{
    map <string,AttrType> hashtype;
    map <string,int> hashpos;
    int offset = 0;
    for(unsigned i=0;i<recordDescriptor.size();i++)
    {
        hashtype[recordDescriptor[i].name]=recordDescriptor[i].type;    //convert name to type
        if (recordDescriptor[i].type == 2) {
            hashpos[recordDescriptor[i].name] = offset;
            int length;
            memcpy(&length, (char*)record + offset, sizeof(int));
            offset += sizeof(int);
            offset += length;
        }
        else
        {
            hashpos[recordDescriptor[i].name] = offset;
            offset += sizeof(int);
        }
    }
    int currPosition = 0;
    for (unsigned i=0; i < attributeNames.size();i++)
    {
        AttrType ty;
        ty = hashtype[attributeNames[i]];
        int position;
        position = hashpos[attributeNames[i]];
        if (ty == 2) {
            unsigned length;
            memcpy(&length, (char*)record + position,sizeof(int));
            memcpy((char*)data+currPosition, &length, sizeof(int));
            memcpy((char*)data+currPosition+sizeof(int), (char*)record + position + sizeof(int),length);
            currPosition += sizeof(int) + length;
        }
        else{
            memcpy((char*)data+currPosition, (char*)record + position,sizeof(int));
            currPosition += sizeof(int);
        }
    }
    return 0;
}
bool RBFM_ScanIterator::checkValue(void *attrValue)
{
    if(compOp==NO_OP)
        return true;
    if(attrType==0)
    {   int vl;
        memcpy(&vl,value , sizeof(int));
        int att;
        memcpy(&att, attrValue , sizeof(int));
        if(compOp==EQ_OP)
            return vl == att;
        if(compOp==GT_OP)
            return vl < att;
        if(compOp==LT_OP)
            return vl > att;
        if(compOp==GE_OP)
            return vl <= att;
        if(compOp==LE_OP)
            return vl >= att;
        if(compOp==NE_OP)
            return vl != att;
        return false;
        
    }
    if(attrType==1)
    {   float vl;
        memcpy(&vl,value , sizeof(float));
        float att;
        memcpy(&att, attrValue , sizeof(float));
        if(compOp==EQ_OP)
            return vl == att;
        if(compOp==GT_OP)
            return vl < att;
        if(compOp==LT_OP)
            return vl > att;
        if(compOp==GE_OP)
            return vl <= att;
        if(compOp==LE_OP)
            return vl >= att;
        if(compOp==NE_OP)
            return vl != att;
        return false;
    }
    if(attrType==2)
    {
        string attr="", target="";
        int attrLength;
        int targetLength;
        memcpy(&targetLength, (char*)value, sizeof(int));
        memcpy(&attrLength, (char*)attrValue, sizeof(int));
        attr.assign((char*)attrValue+sizeof(int), (char*)attrValue+sizeof(int) + attrLength);
        target.assign((char*)value+sizeof(int), (char*)value+sizeof(int) + targetLength);
        if(compOp==EQ_OP)
            return attr == target;
        if(compOp==LT_OP)
            return attr < target;
        if(compOp==GT_OP)
            return attr > target;
        if(compOp==LE_OP)
            return attr <= target;
        if(compOp==GE_OP)
            return attr >= target;
        if(compOp==NE_OP)
            return attr != target;
        return false;
    }
    return false;
}
void RBFM_ScanIterator::init(FileHandle &fileHandle, const vector<Attribute> &recordDescript, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames)
{
    this->fileHandle = fileHandle;
    this->recordDescriptor = recordDescript;
    this->conditionAttribute = conditionAttribute;
    this->attributeNames = attributeNames;
    this->value = value;
    this->compOp = compOp;
    currRid.slotNum = 1;
    currRid.pageNum = 0;
    attrType = 0;
    for (int i=0; i<recordDescriptor.size(); i++) {
        if (recordDescriptor[i].name == conditionAttribute)
        {
            attrType = recordDescriptor[i].type;
            break;
        }
    }
    
    
}
RC RecordBasedFileManager::reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor)
{
    int writingPage = 0;
    int readingPage = 0;
    int totalPage = fileHandle.getNumberOfPages();
    void *read = malloc(PAGE_SIZE);
    void *write = malloc(PAGE_SIZE);
    int freeSpacePointer = 0;
    int currentSlotWriting=0;
    while (readingPage<totalPage) {
        fileHandle.readPage(readingPage, read);
        int currentSlot = 1;
        int slotTotal;
        memcpy(&slotTotal, (char*)read + PAGE_SIZE - 2*sizeof(int), sizeof(int));
        while(currentSlot <= slotTotal)
        {
            if (freeSpacePointer == 0) {
                unsigned int tmp = 0;
                memcpy((char*)write+PAGE_SIZE-sizeof(int), &tmp, sizeof(int));
                memcpy((char*)write+PAGE_SIZE-2*sizeof(int), &tmp, sizeof(int));
            }
            int length;
            memcpy(&length, (char*)read + PAGE_SIZE - (1 + currentSlot) * 2 * sizeof(int)+ sizeof(int), sizeof(int));
                if (currentSlot == 58 and readingPage == 1)
                {
                    cout<<"Here"<<endl;
                }
            if (length == -1 or length == -2)
            {
                currentSlot ++;
                continue;
            }
            int offset;
            memcpy(&offset,(char*) read + PAGE_SIZE - (1 + currentSlot) * 2 * sizeof(int),sizeof(int));                    //read offset
            if (length + freeSpacePointer > PAGE_SIZE-(currentSlotWriting+1)*2*sizeof(int)) {
                fileHandle.writePage(writingPage,write);
                writingPage++;
                freeSpacePointer=0;
                currentSlotWriting =0;
            }
            else
            {
                memcpy((char*)write+freeSpacePointer, (char*)read+offset,length); //write data
                currentSlotWriting+=1;
                unsigned slotOffset = PAGE_SIZE - (2 + currentSlotWriting * 2) * sizeof(int);
                memcpy((char*) write + slotOffset, &freeSpacePointer, sizeof(int)); //modify slotDirectory
                memcpy((char*) write + slotOffset + sizeof(int), &length, sizeof(int));
                freeSpacePointer += length;
                memcpy((char*)write + PAGE_SIZE - sizeof(int), &freeSpacePointer, sizeof(int)); //change freePointer
                memcpy((char*)write + PAGE_SIZE - 2 * sizeof(int), &currentSlotWriting, sizeof(int)); //change slotSize
                currentSlot++;
            }
        }
        readingPage++;
    }
    fileHandle.writePage(writingPage, write);
    writingPage +=1;
    while (writingPage < totalPage) {
        newPageInit(fileHandle, writingPage);
        writingPage+=1;
    }
    free(read);
    free(write);
    return 0;
}
