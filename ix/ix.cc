#include "ix.h"
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <iostream>
#include <bitset>
#include <math.h>
int getVarcharOffset(void* data, int offset)
{
    int exactPosition;
    memcpy(&exactPosition, (char*)data+PAGE_SIZE-(3+offset)*sizeof(int),sizeof(int));
    return exactPosition;
}
string getVarcharValue(void* data, int offset)
{
    string targetValue = "";
    int targetlength;
    int exactPosition;
    memcpy(&exactPosition, (char*)data+PAGE_SIZE-(3+offset)*sizeof(int),sizeof(int));
    memcpy(&targetlength, (char*)data+exactPosition, sizeof(int));
    targetValue.assign((char*)data+exactPosition+sizeof(int),(char*)data+exactPosition+sizeof(int)+targetlength);
    return targetValue;
}
int getVarcharSlot(void *slotData, void* data, int offset)
{
    int exactPosition;
    memcpy(&exactPosition, (char*)data+PAGE_SIZE-(3+offset)*sizeof(int),sizeof(int));
    int slotlength;
    memcpy(&slotlength, (char*)data+PAGE_SIZE-(4+offset)*sizeof(int),sizeof(int));
    slotlength -= exactPosition;
    memcpy(slotData, (char*)data+exactPosition, slotlength);
    return slotlength;
}
void getVarcharRid(RID &rid, void *data, int offset)
{
    void *slotData=malloc(PAGE_SIZE);
    int length=getVarcharSlot(slotData, data, offset);
    memcpy(&rid.pageNum, (char*)slotData+length-2*sizeof(int), sizeof(int));
    memcpy(&rid.slotNum, (char*)slotData+length-sizeof(int), sizeof(int));
}
void getValueForVarchar(string &str, void *data)
{
    int attrLength;
    memcpy(&attrLength, (char*)data, sizeof(int));
    str.assign((char*)data+sizeof(int), (char*)data+sizeof(int) + attrLength);
}

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance() {
	if (!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager() {

}

IndexManager::~IndexManager() {
}

RC IndexManager::createFile(const string &fileName,
		const unsigned &numberOfPages) {
	string metaFile = fileName + "_meta";
	string primFile = fileName + "_primary";
	if (PagedFileManager::instance()->createFile(metaFile.c_str()) == 0) {
		initiateMeta(metaFile, numberOfPages);
		if (PagedFileManager::instance()->createFile(primFile.c_str()) == 0) {
			initiatePrim(primFile, numberOfPages);
			return 0;
		}
	}
	return -1;
}

RC IndexManager::initiateMeta(string metaFile, const unsigned &numberOfPages) {
	FileHandle fhMeta;
	PagedFileManager::instance()->openFile(metaFile.c_str(), fhMeta);
	void *dataPage = malloc(PAGE_SIZE);
	memset(dataPage, 0, PAGE_SIZE);
	memcpy((char*) dataPage + 2 * sizeof(int), &numberOfPages, sizeof(int));
	//useful?
	//memcpy((char*) dataPage + PAGE_SIZE - sizeof(int), &usedSize, sizeof(int));
	fhMeta.appendPage(dataPage);
	free(dataPage);
	return 0;
}

RC IndexManager::initiatePrim(string primFile, const unsigned &numberOfPages) {
	FileHandle fhPrim;
	PagedFileManager::instance()->openFile(primFile.c_str(), fhPrim);
	void *dataPage = malloc(PAGE_SIZE);
	memset(dataPage, 0, PAGE_SIZE);
	//fhPrim.writePage(0, dataPage);
	for (int i = 0; i < numberOfPages; i++) {
		fhPrim.appendPage(dataPage);
	}
	free(dataPage);
	return 0;
}

RC IndexManager::destroyFile(const string &fileName) {
	string metaFile = fileName + "_meta";
	string primFile = fileName + "_primary";
	PagedFileManager::instance()->destroyFile(metaFile.c_str());
	PagedFileManager::instance()->destroyFile(primFile.c_str());
	return 0;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {

	string metaFile = fileName + "_meta";
	string primFile = fileName + "_primary";
	FileHandle fhMeta;
	FileHandle fhPrim;

	if (PagedFileManager::instance()->openFile(metaFile.c_str(), fhMeta) == 0) {
		getLevelNext(ixfileHandle);
		if (PagedFileManager::instance()->openFile(primFile.c_str(), fhPrim)
				== 0) {
            cout<<primFile.c_str()<<endl;
			ixfileHandle.passFileHandle(fhMeta, fhPrim);
			fseek(fhPrim.getFile(), 0, SEEK_END);
			ixfileHandle.setPrimPageNumber(ftell(fhPrim.getFile()) / PAGE_SIZE);
			fseek(fhPrim.getFile(), 0, SEEK_SET);
			ixfileHandle.passFileHandle(fhMeta, fhPrim);
			fseek(fhMeta.getFile(), 0, SEEK_END);
			ixfileHandle.setMetaPageNumber(ftell(fhMeta.getFile()) / PAGE_SIZE);
			fseek(fhMeta.getFile(), 0, SEEK_SET);

			return 0;
		}
	}
	return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
	FileHandle fhMeta;
	FileHandle fhPrim;
	ixfileHandle.fetchFileHandle(fhMeta, fhPrim);
	void *meta = malloc(PAGE_SIZE);
	fhMeta.readPage(0, meta);
	int level = ixfileHandle.getLevel();
	int next = ixfileHandle.getNext();
	//int bucket = ixfileHandle.getBucket();
	memcpy((char*) meta, &level, sizeof(int));
	memcpy((char*) meta + sizeof(int), &next, sizeof(int));
	//memcpy((char*)meta + 2*sizeof(int), &bucket, sizeof(int));
	fhMeta.writePage(0, meta);
	PagedFileManager::instance()->closeFile(fhMeta);
	PagedFileManager::instance()->closeFile(fhPrim);
	free(meta);
	return 0;
}

RC IndexManager::getLevelNext(IXFileHandle &ixfileHandle) {
	FileHandle fhMeta;
	FileHandle fhPrim; //useless
	ixfileHandle.fetchFileHandle(fhMeta, fhPrim);
	void *dataPage = malloc(PAGE_SIZE);
	memset(dataPage, 0, PAGE_SIZE);
	fhMeta.readPage(0, dataPage);
	int level;
	int next;
	int bucket;
	memcpy(&level, dataPage, sizeof(int));
	memcpy(&next, (char*) dataPage + sizeof(int), sizeof(int));
	memcpy(&bucket, (char*) dataPage + 2 * sizeof(int), sizeof(int));
	ixfileHandle.setLevel(level);
	ixfileHandle.setNext(next);
	ixfileHandle.setBucket(bucket);
	free(dataPage);
	//PagedFileManager::instance()->closeFile(fhMeta);
	//PagedFileManager::instance()->closeFile(fhPrim);
	return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
               const void *key, const RID &rid)
{
    //check if attribute is in attributeTable
    if (attributeTable.find(attribute.name) == attributeTable.end()) {
        //get information from IXFileHandle
        getLevelNext(ixfileHandle);
        vector<int> metaInfo;
        metaInfo.push_back(ixfileHandle.getLevel());
        metaInfo.push_back(ixfileHandle.getNext());
        metaInfo.push_back(ixfileHandle.getBucket());
        attributeTable[attribute.name] = metaInfo;
    }
    unsigned bucketNumber = hash(attribute, key);
    
    FileHandle &metaHandle = ixfileHandle.getMetaFile();
    FileHandle &primaryHandle = ixfileHandle.getPrimFile();
    //FindinsertPosition
    vector<void *> pages;
    void *targetPage = malloc(PAGE_SIZE);
    memset(targetPage, 0, PAGE_SIZE);
    
    //get total number of slots， pageNumber and number of slots per page
    primaryHandle.readPage(bucketNumber, targetPage);
    
    pages.push_back(targetPage);
    int totalNumber = 0;
    vector<int> pageNumbers;
    vector<int> pagePositions;
    memcpy(&totalNumber, (char*)targetPage+PAGE_SIZE-2*sizeof(int), sizeof(int));
    pageNumbers.push_back(totalNumber);
    pagePositions.push_back(bucketNumber);
    int nextPage= 0;
    memcpy(&nextPage, (char*)targetPage+PAGE_SIZE-sizeof(int), sizeof(int));
    while (nextPage!=0) {
        pagePositions.push_back(nextPage);
        void *overfowPage = malloc(PAGE_SIZE);
        memset(overfowPage, 0, PAGE_SIZE);
        metaHandle.readPage(nextPage, overfowPage);
        
        pages.push_back(overfowPage);
        memcpy(&nextPage, (char*)overfowPage + PAGE_SIZE - sizeof(int), sizeof(int));
        int slotNumber;
        memcpy(&slotNumber, (char*)overfowPage + PAGE_SIZE - 2*sizeof(int), sizeof(int));
        pageNumbers.push_back(slotNumber);
        totalNumber += slotNumber;
    }
    
    //find the position
    int position = findInsertPosition(key, attribute, pages, totalNumber,pageNumbers);
    int overflowPages = metaHandle.getNumberOfPages();
    int startPage = insertToPage(position, pageNumbers, key, attribute, pages, rid);
    writeToPages(ixfileHandle, metaHandle, primaryHandle, startPage, pages, pagePositions, overflowPages);
    if (pageNumbers.size()>1) {
        split(attribute,attribute.name, metaHandle, primaryHandle);        
    }

	return 0;
}
int IndexManager::writeToPages(IXFileHandle &ixfileHandle , FileHandle &metaFile, FileHandle &primFile, int startPage, vector<void*> &page, vector<int> &pagePosition, int nextPageNumber)
{
    if (page.size()>pagePosition.size()) {
        memcpy((char*)page[page.size()-2]+PAGE_SIZE-sizeof(int), &nextPageNumber, sizeof(int));
        pagePosition.push_back(nextPageNumber);
        void *tmp = malloc(PAGE_SIZE);
        memset(tmp, 0, PAGE_SIZE);
        metaFile.appendPage(tmp);
        
    }
    if (startPage == 0) {
        primFile.writePage(pagePosition[0], page[0]);
        
        startPage += 1;
    }
    while (startPage < page.size()) {
        metaFile.writePage(pagePosition[startPage], page[startPage]);
        
        startPage+=1;
        
    }
    while (page.size()!=0) {
        free(page[0]);
        page.erase(page.begin());
    }
    /*
    for (int i =0; i<page.size(); i++) {
        free(page[i]);
    }
     */
    return 0;
}

int IndexManager::insertToPage(int position, vector<int> &pageNumbers, const void *key, const Attribute attribute, vector<void *> &page, const RID &rid)
{
    if (attribute.type !=2) {
        //get slotData
        void *slotData = malloc(INTREAL_SLOT);
        memset(slotData, 0, INTREAL_SLOT);
        memcpy(slotData, key, sizeof(int));
        memcpy((char*)slotData + sizeof(int), &rid.pageNum, sizeof(int));
        memcpy((char*)slotData + 2*sizeof(int), &rid.slotNum, sizeof(int));
        
        //find the position of the page
        int pagePosition = 0;
        while(pageNumbers[pagePosition]>0 and position > INTREAL_NUMBER-1) {
            position -= pageNumbers[pagePosition];
            pagePosition += 1;
            }
        if (pagePosition==pageNumbers.size())
        {
            pageNumbers.push_back(0);
            void *appendPage = malloc(PAGE_SIZE);
            memset(appendPage, 0, PAGE_SIZE);
            memcpy(appendPage, slotData, INTREAL_SLOT);
            int number = 1;
            memcpy((char*)appendPage+PAGE_SIZE-2*sizeof(int), &number, sizeof(int));
            page.push_back(appendPage);
            return 0;
        }


        //move within the same page
        if (pageNumbers[pagePosition] < INTREAL_NUMBER)
        {
            int moveSize = INTREAL_SLOT*(pageNumbers[pagePosition]-position);
            void *tmp = malloc(moveSize);
            memset(tmp, 0, moveSize);
            memcpy(tmp, (char*)page[pagePosition]+position*INTREAL_SLOT ,moveSize);
            memcpy((char*)page[pagePosition]+(position+1)*INTREAL_SLOT, tmp, moveSize);
            memcpy((char*)page[pagePosition]+position*INTREAL_SLOT , slotData, INTREAL_SLOT);
            pageNumbers[pagePosition] += 1;
            memcpy( (char*)page[pagePosition] + PAGE_SIZE - 2*sizeof(int), &pageNumbers[pagePosition], sizeof(int));
            free(tmp);
            free(slotData);
            return pagePosition;
        }
        //move among pages
        else
        {
            //getTailData
            void *tailData = malloc(INTREAL_SLOT);
            memset(tailData, 0, INTREAL_SLOT);
            memcpy((char*)tailData, (char*)page[pagePosition]+(INTREAL_NUMBER-1)*INTREAL_SLOT, INTREAL_SLOT);

            //modify the first page
            int currentPage = pagePosition;
            int currentPosition = position;
            int moveSize = INTREAL_SLOT*(pageNumbers[currentPage]-1-currentPosition);
            void *tmp = malloc(moveSize);
            memset(tmp, 0, moveSize);
            memcpy(tmp, (char*)page[currentPage]+currentPosition*INTREAL_SLOT ,moveSize);
            memcpy((char*)page[currentPage]+(currentPosition+1)*INTREAL_SLOT , tmp, moveSize);
            memcpy((char*)page[currentPage]+position*INTREAL_SLOT , slotData, INTREAL_SLOT);

            free(slotData);
            free(tmp);
            currentPage += 1;
            currentPosition = 0;

            //if it is the last page, appendPage
            
            if (currentPage == pageNumbers.size()) {
                void *appendPage = malloc(PAGE_SIZE);
                memset(appendPage, 0, PAGE_SIZE);
                int numberofSlots = 1;
                memcpy((char*)appendPage+PAGE_SIZE-2*sizeof(int), &numberofSlots, sizeof(int));
                memcpy((char*)appendPage, tailData, INTREAL_SLOT);
                pageNumbers.push_back(1);
                page.push_back(appendPage);
                free(tailData);
                return pagePosition;
            }

            //move to the last page
            while (currentPage < pageNumbers.size()-1)
            {
                //moveData
                int moveSize = INTREAL_SLOT*(pageNumbers[currentPage]-1-currentPosition);
                void *tmp = malloc(moveSize);
                memset(tmp, 0, moveSize);
                memcpy(tmp, (char*)page[currentPage]+currentPosition*INTREAL_SLOT ,moveSize);
                memcpy((char*)page[currentPage]+currentPosition*INTREAL_SLOT, tailData, INTREAL_SLOT);
                //get new tailData
                memcpy((char*)tailData, (char*)page[currentPage]+(INTREAL_NUMBER-1)*INTREAL_SLOT, INTREAL_SLOT);
                //write data to page
                memcpy((char*)page[currentPage]+(currentPosition+1)*INTREAL_SLOT , tmp, moveSize);
                free(tmp);
                currentPage += 1;
            }

            // if the last page is full
            if (pageNumbers[currentPage] == INTREAL_NUMBER) {
                //modify this page
                int move = INTREAL_SLOT*(pageNumbers[currentPage]-1-currentPosition);
                void *tmp2 = malloc(move);
                memset(tmp2, 0, move);
                memcpy(tmp2, (char*)page[currentPage]+currentPosition*INTREAL_SLOT ,move);
                memcpy((char*)page[currentPage]+currentPosition*INTREAL_SLOT, tailData, INTREAL_SLOT);
                memcpy((char*)tailData, (char*)page[currentPage]+(INTREAL_NUMBER-1)*INTREAL_SLOT, INTREAL_SLOT);
                memcpy((char*)page[currentPage]+(currentPosition+1)*INTREAL_SLOT, tmp2, move);
                //append another page
                void *appendPage = malloc(PAGE_SIZE);
                memset(appendPage, 0, PAGE_SIZE);
                int numberofSlots = 1;
                memcpy((char*)appendPage+PAGE_SIZE-2*sizeof(int), &numberofSlots, sizeof(int));
                memcpy((char*)appendPage, tailData, INTREAL_SLOT);
                pageNumbers.push_back(1);
                page.push_back(appendPage);
                free(tailData);
                free(tmp2);
                return pagePosition;

            }
            //if the last page is not full
            else{
                int move = INTREAL_SLOT*(pageNumbers[currentPage]-currentPosition);
                void *tmp2 = malloc(move);
                memset(tmp2, 0, move);
                memcpy(tmp2, (char*)page[currentPage]+currentPosition*INTREAL_SLOT ,move);
                memcpy((char*)page[currentPage]+currentPosition*INTREAL_SLOT, tailData, INTREAL_SLOT);
                memcpy((char*)page[currentPage]+(currentPosition+1)*INTREAL_SLOT, tmp2, move);
                pageNumbers[currentPage]++;
                memcpy( (char*)page[currentPage] + PAGE_SIZE - 2*sizeof(int), &pageNumbers[currentPage], sizeof(int));
                free(tmp2);
                free(tailData);
                return pagePosition;
            }
        }
    }
    else{
        //get slotData
        int length = *(int*)key;
        void *slotData = malloc(length+3*sizeof(int));
        memcpy(&length, key, sizeof(int));
        memcpy(slotData, key, sizeof(int));
        memcpy((char*)slotData + sizeof(int), (char*)key+sizeof(int), length);
        memcpy((char*)slotData + sizeof(int)+length, &rid.pageNum, sizeof(int));
        memcpy((char*)slotData + 2*sizeof(int) + length, &rid.slotNum, sizeof(int));
        length += 3*sizeof(int);
        
        //find the position of the page
        int pagePosition = 0;
        while(pageNumbers[pagePosition]>0 and position >=pageNumbers[pagePosition] ) {
            if (position==pageNumbers[pagePosition]) {
                if(checkVarcharFull(1,length, page[pagePosition])==true)
                {
                    position -= pageNumbers[pagePosition];
                    pagePosition += 1;
                }
                else
                    break;
            }
            else
            {
            position -= pageNumbers[pagePosition];
            pagePosition += 1;
            }
            }
        if (pagePosition==pageNumbers.size())
        //need to be revised!!!!!!!!!!!
        {
            pageNumbers.push_back(0);
            void *appendPage = malloc(PAGE_SIZE);
            memset(appendPage, 0, PAGE_SIZE);
            memcpy(appendPage, slotData, INTREAL_SLOT);
            int number = 1;
            memcpy((char*)appendPage+PAGE_SIZE-2*sizeof(int), &number, sizeof(int));
            page.push_back(appendPage);
            return pagePosition+1;
        }


        //move within the same page
        if(checkVarcharFull(1,length, page[pagePosition])==false)
        {
            //get movesize
            int slotNumber;
            memcpy(&slotNumber, (char*)page[pagePosition]+PAGE_SIZE-2*sizeof(int), sizeof(int));
            int directail;
            directail = PAGE_SIZE-(3+slotNumber)*sizeof(int);
            int lastslottail;
            memcpy(&lastslottail, (char*)page[pagePosition] + directail, sizeof(int));
            int startPosition = getVarcharOffset(page[pagePosition],position);
            int moveSize = lastslottail-startPosition;
            
           //moveslot
            void *tmp = malloc(moveSize);
            memset(tmp, 0, moveSize);
            memcpy(tmp, (char*)page[pagePosition]+startPosition ,moveSize);
            memcpy((char*)page[pagePosition]+startPosition+length, tmp, moveSize);
            
            //insertslot
            memcpy((char*)page[pagePosition]+startPosition , slotData, length);
            
            //movedirectory
           // if (slotNumber==0) {
            //}
            moveVarChardir(position,slotNumber-1, length, page[pagePosition]);
            
            
            //add pageNumbers
            pageNumbers[pagePosition] += 1;
            memcpy( (char*)page[pagePosition] + PAGE_SIZE - 2*sizeof(int), &pageNumbers[pagePosition], sizeof(int));
            
            free(tmp);
            free(slotData);
            return pagePosition;
        }
        //move among pages
        else
        {
            //getTailData
            int slotNumber;
            memcpy(&slotNumber, (char*)page[pagePosition]+PAGE_SIZE-2*sizeof(int), sizeof(int));
            vector<int> tailLengths;
            void *tailData = malloc(PAGE_SIZE);
            memset(tailData,0,PAGE_SIZE);
            int pointer = slotNumber-1;
            int taillength = getVarcharSlot(tailData, page[pagePosition],pointer);
            tailLengths.push_back(taillength);
            while (taillength<length) {
                pointer -= 1;
                int tmplength;
                //get data
                void *tmpslot = malloc(PAGE_SIZE);
                tmplength = getVarcharSlot(tmpslot, page[pagePosition], pointer);
                
                //move data
                void *a = malloc(taillength);
                memcpy(a, tailData, taillength);
                memcpy((char*)tailData+tmplength, a, taillength);
                memcpy(tailData, tmpslot, tmplength);
                
                //modify totallength and vector
                taillength+= tmplength;
                tailLengths.insert(tailLengths.begin(), tmplength);
                free(tmpslot);
                free(a);
            }
            //!!!!!!Here
            //insertTailTopage()

            //modify the first page
            int currentPage = pagePosition;
            int currentPosition = position;
            
            //get movesize
            int lastslottail = getVarcharOffset(page[pagePosition], pointer);
          //  memcpy(&lastslottail, (char*)page[currentPage] + getVarcharOffset(page[currentPage], pointer), sizeof(int));
            int startPosition = getVarcharOffset(page[pagePosition],currentPosition);
            int moveSize = lastslottail-startPosition;
            
            //moveslot
            void *tmp = malloc(moveSize);
            memset(tmp, 0, moveSize);
            memcpy(tmp, (char*)page[currentPage]+startPosition ,moveSize);
            memcpy((char*)page[currentPage]+startPosition+length, tmp, moveSize);
            free(tmp);
            
            //insertslot
            memcpy((char*)page[currentPage]+startPosition , slotData, length);
            free(slotData);

            moveVarChardir(currentPosition,pointer-1, length, page[currentPage]);
            
            //add pageNumbers
            pageNumbers[pagePosition] += 1;
            pageNumbers[pagePosition] -= tailLengths.size();
            memcpy( (char*)page[pagePosition] + PAGE_SIZE - 2*sizeof(int), &pageNumbers[pagePosition], sizeof(int));
            
            
            currentPage += 1;
            currentPosition = 0;

            insertTailTopage(tailData,tailLengths,taillength,currentPage,page,pageNumbers);
            return pagePosition;

        }
             
             
    }
}

int IndexManager::findInsertPosition(const void *key, const Attribute attribute, vector<void *> &pages, int totalNumber,vector<int> pageNumbers)
{
    if (attribute.type == 0)
    {
        int keyValue = *(int*)key;
        int head = 0;
        int tail = 0;
        if (totalNumber > 0)
            tail = totalNumber-1;
        else
            return 0;
        int insertPosion = 0;
        while (true) {
            int number =INTREAL_NUMBER;
            int middleNumber = (tail+head)/2 ;   //middleNumber th number in total
            int pageNumber = middleNumber/number;
            int offset = middleNumber%number;
            int targetValue;
            memcpy(&targetValue, (char*)pages[pageNumber]+offset*3*sizeof(int), sizeof(int));
            if (head >= tail) {
                int tmppagenumber = head/number;
                int tmpoffset = head%number;
                memcpy(&targetValue, (char*)pages[tmppagenumber]+tmpoffset*3*sizeof(int), sizeof(int));
                if (keyValue<=targetValue) {
                    insertPosion = head;
                }
                else
                    insertPosion = head +1;
                return insertPosion;
            }
            if (keyValue == targetValue)
                return middleNumber;
            if (keyValue > targetValue)
                head = middleNumber +1;
            else
                tail = middleNumber -1;
        }
    }
    if (attribute.type == 1)
    {
        float keyValue = *(float*)key;
        int head = 0;
        int tail = 0;
        if (totalNumber > 0)
            tail = totalNumber-1;
        else
            return 0;
        int insertPosion = 0;
        while (true) {
            int number =INTREAL_NUMBER;
            int middleNumber = (tail+head)/2 ;   //middleNumber th number in total
            int pageNumber = middleNumber/number;
            int offset = middleNumber%number;
            float targetValue;
            memcpy(&targetValue, (char*)pages[pageNumber]+offset*3*sizeof(int), sizeof(int));
            if (head >= tail) {
                int tmppagenumber = head/number;
                int tmpoffset = head%number;
                memcpy(&targetValue, (char*)pages[tmppagenumber]+tmpoffset*3*sizeof(int), sizeof(int));
                if (keyValue<=targetValue) {
                    insertPosion = head;
                }
                else
                    insertPosion = head +1;
                return insertPosion;
            }
            if (keyValue == targetValue)
                return middleNumber;
            if (keyValue > targetValue)
                head = middleNumber +1;
            else
                tail = middleNumber -1;
        }
    }
    else {
        string keyValue = "";
        int keyLength = *(int*)key;
        keyValue.assign((char*)key+sizeof(int),(char*)key+sizeof(int)+keyLength);
        int head = 0;
        int tail = 0;
        if (totalNumber > 0)
            tail = totalNumber-1;
        else
            return 0;
        int insertPosion = 0;
        while (true) {
            int middleNumber = (tail+head)/2 ;   //middleNumber th number in total
            int offset = 0;
            int pageNumber = 0;
            middleTopage(middleNumber, pageNumbers, offset, pageNumber);
            string targetValue= getVarcharValue(pages[pageNumber], offset);
            if (head >= tail) {
                int tmppagenumber;
                int tmpoffset;
                middleTopage(head, pageNumbers, tmpoffset, tmppagenumber);
                targetValue = getVarcharValue(pages[tmppagenumber], tmpoffset);
                if (keyValue<=targetValue) {
                    insertPosion = head;
                }
                else
                    insertPosion = head +1;
                return insertPosion;
            }
            if (keyValue == targetValue)
                return middleNumber;
            if (keyValue > targetValue)
                head = middleNumber +1;
            else
                tail = middleNumber -1;
        }
    }
}
void IndexManager::middleTopage(int middle, vector<int>pageNumbers, int&offset, int&pageNum)
{
    pageNum=0;
    for (int i =0 ; i<pageNumbers.size(); i++) {
        if (middle>=pageNumbers[i]) {
            middle -= pageNumbers[i];
            pageNum++;
        }
        else
            break;
    }
    offset = middle;
}
//insert tailData to currentPage
void  IndexManager::insertTailTopage(void *tailData,vector<int> &tailLengths,int taillength,int currentPage,vector<void*> &page,vector<int> &pageNumbers)
{
    //need to append new page
    if (currentPage == page.size()) {
        void *appendPage = malloc(PAGE_SIZE);
        memset(appendPage, 0, PAGE_SIZE);
        page.push_back(appendPage);
        //insertslot
        memcpy((char*)page[currentPage], tailData, taillength);
        free(tailData);
        //build directory
        for (int i=0;i<tailLengths.size();i++)
        {
            int writeOffset = PAGE_SIZE-(4+i)*sizeof(int);
            memcpy((char*)page[currentPage]+writeOffset, &tailLengths[i] , sizeof(int));
        }
        
        //add pageNumbers
        int pagenumber = tailLengths.size();
        pageNumbers.push_back(pagenumber);
        memcpy( (char*)page[currentPage] + PAGE_SIZE - 2*sizeof(int), &pagenumber, sizeof(int));
        return;
    }
    else
    {
        //move within the page
        if(checkVarcharFull(tailLengths.size(),taillength, page[currentPage])==false)
        {
            //get movesize
            int slotNumber;
            memcpy(&slotNumber, (char*)page[currentPage]+PAGE_SIZE-2*sizeof(int), sizeof(int));
            int lastslottail = getVarcharOffset(page[currentPage], slotNumber);
          //  memcpy(&lastslottail, (char*)page[currentPage] + getVarcharOffset(page[currentPage], pointer), sizeof(int));
            int moveSize = lastslottail;
            //moveslot
            void *tmp = malloc(moveSize);
            memset(tmp, 0, moveSize);
            memcpy(tmp, (char*)page[currentPage] ,moveSize);
            memcpy((char*)page[currentPage]+taillength, tmp, moveSize);
            //insertslot
            memcpy((char*)page[currentPage],tailData, taillength);
            free(tmp);
            free(tailData);
            
            //movedirectory
            // if (slotNumber==0) {
            //}
            for(int i=0;i<tailLengths.size();i++)
            {
                
                moveVarChardir(0,slotNumber-1 , tailLengths[i], page[currentPage]);
                slotNumber+=1;
            }
            
            //add pageNumbers
            pageNumbers[currentPage] = slotNumber;
            memcpy( (char*)page[currentPage] + PAGE_SIZE - 2*sizeof(int), &pageNumbers[currentPage], sizeof(int));
            return;
        }
        else
        {
            //get new TailData
            void *tmptailData = malloc(PAGE_SIZE);
            vector<int> tmptaillengths;
            int tmptaillength = getVarcharTailData(page[currentPage], taillength, tmptaillengths,tmptailData);
            
            //get movesize
            int slotNumber;
            memcpy(&slotNumber, (char*)page[currentPage]+PAGE_SIZE-2*sizeof(int), sizeof(int));
            slotNumber -= tmptaillengths.size();
            int lastslottail = getVarcharOffset(page[currentPage], slotNumber);
          //  memcpy(&lastslottail, (char*)page[currentPage] + getVarcharOffset(page[currentPage], pointer), sizeof(int));
            int moveSize = lastslottail;
            //moveslot
            void *tmp = malloc(moveSize);
            memset(tmp, 0, moveSize);
            memcpy(tmp, (char*)page[currentPage] ,moveSize);
            memcpy((char*)page[currentPage]+taillength, tmp, moveSize);
            //insertslot
            memcpy((char*)page[currentPage],tailData, taillength);
            free(tmp);
            free(tailData);
        
            //move directory
            for(int i=0;i<tailLengths.size();i++)
            {
                moveVarChardir(0,slotNumber-1 , tailLengths[i], page[currentPage]);
                slotNumber+=1;
            }
            
            //add pageNumbers
            pageNumbers[currentPage] = slotNumber;
            memcpy( (char*)page[currentPage] + PAGE_SIZE - 2*sizeof(int), &pageNumbers[currentPage], sizeof(int));
            
            tailLengths.clear();
            tailLengths = tmptaillengths;
            insertTailTopage(tmptailData, tailLengths, tmptaillength, currentPage+1, page, pageNumbers);
            
            return;
            }
        }
    
}
int IndexManager::getVarcharTailData(void *pagedata, int length, vector<int> &tailLengths, void* tailData)
{
    int slotNumber;
    memcpy(&slotNumber, (char*)pagedata+PAGE_SIZE-2*sizeof(int), sizeof(int));
    memset(tailData,0,PAGE_SIZE);
    int pointer = slotNumber-1;
    int taillength = getVarcharSlot(tailData, pagedata,pointer);
    
    tailLengths.push_back(taillength);
    while (taillength<length) {
        pointer -= 1;
        int tmplength;
        //get data
        void *tmpslot = malloc(PAGE_SIZE);
        tmplength = getVarcharSlot(tmpslot, pagedata, pointer);
        
        //move data
        void *a = malloc(taillength);
        memcpy(a, tailData, taillength);
        memcpy((char*)tailData+tmplength, a, taillength);
        memcpy(tailData, tmpslot, tmplength);
        
        //modify totallength and vector
        taillength+= tmplength;
        tailLengths.insert(tailLengths.begin(), tmplength);
        free(tmpslot);
        free(a);
    }
    return taillength;
}

void IndexManager::moveVarChardirDelete(int start, int end, int slotlength, void *data)
{
    int head;
    head = PAGE_SIZE-(4+end)*sizeof(int);
    int tail = PAGE_SIZE-(4+start)*sizeof(int);
    int tmp = head;
    //modify value
    while (tmp<=tail)
    {
        int offsetValue;
        int a = *(int*)((char*)data+tmp);
        memcpy(&offsetValue, (char*)data+tmp, sizeof(int));
        offsetValue-=slotlength;
        memcpy((char*)data+tmp,&offsetValue, sizeof(int));
        tmp+=sizeof(int);
    }
    //move slot without clear the last dir
    void *tmpdata = malloc(tail-head+sizeof(int));
    memcpy(tmpdata,(char*)data+head, tail-head+sizeof(int));
    memcpy((char*)data+head+sizeof(int), tmpdata, tail-head+sizeof(int));
    free(tmpdata);
}
void IndexManager::moveVarChardir(int start, int end, int slotlength, void *data)
{
    int head;
    head = PAGE_SIZE-(4+end)*sizeof(int);
    int tail = PAGE_SIZE-(3+start)*sizeof(int);
    int tmp = head;
    //modify value
    while (tmp<=tail)
    {
        int offsetValue;
        int a = *(int*)((char*)data+tmp);
        memcpy(&offsetValue, (char*)data+tmp, sizeof(int));
        offsetValue+=slotlength;
        memcpy((char*)data+tmp,&offsetValue, sizeof(int));
        tmp+=sizeof(int);
    }
    
    
    //move slot
    void *tmpdata = malloc(tail-head+sizeof(int));
    memcpy(tmpdata,(char*)data+head, tail-head+sizeof(int));
    memcpy((char*)data+head-sizeof(int), tmpdata, tail-head+sizeof(int));
    free(tmpdata);
    
    int orindata;
    memcpy(&orindata, (char*)data+tail, sizeof(int));
    orindata -= slotlength;
    memcpy((char*)data+tail,&orindata,  sizeof(int));
    
}
bool IndexManager::checkVarcharFull(int datasize, int length, void *data)
{
    int slotNumber;
    memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
    int directail;
    directail = PAGE_SIZE-(3+slotNumber)*sizeof(int);
    int lastslottail;
    memcpy(&lastslottail, (char*)data + directail, sizeof(int));
    return lastslottail+length+datasize*sizeof(int)>directail;
}
RC IndexManager::split(const Attribute &attribute, string attributeName,
                       FileHandle &metaFileHandle, FileHandle &primaryFileHandle) {
    int level = attributeTable[attributeName][0]; //level, next, bucket init
    int next = attributeTable[attributeName][1];
    void *pagePrim = malloc(PAGE_SIZE);
    memset(pagePrim, 0, PAGE_SIZE);
				//cout<<"!level:"<<level<<"  next:"<<next<<endl;
    if (primaryFileHandle.readPage(next, pagePrim) == 0) {
        vector<int> usedOFnumber;
        vector<void *> oldPages;
        oldPages.push_back(pagePrim);
        
        void *Page1 = malloc(PAGE_SIZE);
        memset(Page1, 0, PAGE_SIZE);
        primaryFileHandle.writePage(next,Page1);
        
        int OverflowPageNo;
        memcpy(&OverflowPageNo, (char *) pagePrim + PAGE_SIZE - sizeof(int),
               sizeof(int));

        while (OverflowPageNo != 0) {
            usedOFnumber.push_back(OverflowPageNo);
            void *oldPage = malloc(PAGE_SIZE);
            memset(oldPage, 0, PAGE_SIZE);
            metaFileHandle.readPage(OverflowPageNo, oldPage);
            oldPages.push_back(oldPage);
            void *Page = malloc(PAGE_SIZE);
            memset(Page, 0, PAGE_SIZE);
            metaFileHandle.writePage(OverflowPageNo,Page);
            memcpy(&OverflowPageNo, (char *) oldPage + PAGE_SIZE - sizeof(int),
                   sizeof(int));
        }

        reinsertData(attribute, oldPages, metaFileHandle, primaryFileHandle,usedOFnumber);
        
        updateMetaAfterSplit(attributeName, level, next);
        freeVector(oldPages);
    }
    
    //free(pagePrim);cout
    
    return 0;
}

RC IndexManager::reinsertData(const Attribute &attribute,
                              vector<void *> &oldPages, FileHandle &metaFileHandle, FileHandle &primaryFileHandle,vector<int> usedOFnumber) {
    void * newPage1 = malloc(PAGE_SIZE);
    memset(newPage1, 0, PAGE_SIZE);
    void * newPage2 = malloc(PAGE_SIZE);
    memset(newPage2, 0, PAGE_SIZE);
    vector<void *> newPages1;
    vector<void *> newPages2;
    newPages1.push_back(newPage1);
    newPages2.push_back(newPage2);
    unsigned size = 0;
    while (size < oldPages.size()) {

        void *oldPage = oldPages[size];
        int Entries;
        memcpy(&Entries, (char *) oldPage + PAGE_SIZE - sizeof(int) * 2,
               sizeof(int));

        for (int i = 0; i < Entries; i++) {
            void *entry = malloc(PAGE_SIZE);
            memset(entry, 0, PAGE_SIZE);
            splitGetEntry(entry, attribute, oldPage, i);
            splitInsertEntry(attribute, entry, oldPage, newPages1, newPages2);
            free(entry);
        }
        size++;
    }
    writeRehashedPages(attribute, metaFileHandle, primaryFileHandle, newPages1,
                       newPages2,usedOFnumber);

    freeVector(newPages1);
    freeVector(newPages2);
    return 0;
}

RC IndexManager::freeVector(vector<void *> &newPages) {
    for (unsigned i =0; i<newPages.size(); i++) {
        free(newPages[i]);
    }
    
    return 0;
}

RC IndexManager::writeRehashedPages(const Attribute &attribute,
                                    FileHandle &metaFileHandle, FileHandle &primaryFileHandle,
                                    vector<void *> &newPages1, vector<void *> &newPages2,vector<int> usedOFnumber) {
    int pageNumber1 = attributeTable[attribute.name][1];
    int numOfOverflowPages = metaFileHandle.getNumberOfPages()-1;
    //int numOfOverflowPages ;
    void * newPage1 = newPages1[0];
    void * newPage2 = newPages2[0];
    int used=0;

    if (newPages1.size() > 1) {
        memcpy((char *) newPage1 + PAGE_SIZE - sizeof(int), &usedOFnumber[used],
               sizeof(int));
                
        used++;
        primaryFileHandle.writePage(pageNumber1, newPage1);
        
        writeOverflow(metaFileHandle, newPages1, numOfOverflowPages,usedOFnumber,used);
    } else {
        primaryFileHandle.writePage(pageNumber1, newPage1);
        //cout<<"write primpage to:"<<pageNumber1<<endl;
    }
    if (newPages2.size() > 1) {
        memcpy((char *) newPage2 + PAGE_SIZE - sizeof(int), &usedOFnumber[used],
               sizeof(int));
        used++;
        primaryFileHandle.appendPage(newPage2);
        //primaryFileHandle.writePage(pageNumber2, newPage2);
        writeOverflow(metaFileHandle, newPages2, numOfOverflowPages,usedOFnumber,used);
        
    } else {
        primaryFileHandle.appendPage(newPage2);
        //cout<<"pageNumber2:"<<pageNumber2<<endl;
    }
    return 0;
}

RC IndexManager::writeOverflow(FileHandle &metaFileHandle,
                               vector<void*> &newPages, int &numOfOverflowPages,vector<int> &usedOFnumber,int &used) {
    int remainPages = newPages.size() - 1;
    int writeIN;
    
    while (remainPages > 0) {
        //cout<<"remainPages:"<<remainPages<<endl;
        if (remainPages > 1) {
            if(used<usedOFnumber.size()){
                writeIN=usedOFnumber[used];
            }else{
                numOfOverflowPages++;
                writeIN=numOfOverflowPages;
                usedOFnumber.push_back(numOfOverflowPages);
                void *dataPage = malloc(PAGE_SIZE);
                memset(dataPage,0,PAGE_SIZE);
                metaFileHandle.appendPage(dataPage);
                free(dataPage);
                //used++;
            }
            //cout<<"numOfOverflowPages:"<<numOfOverflowPages<<"usedOFnumber.size():"<<usedOFnumber.size()<<endl;
            void *dataPage2 = newPages[newPages.size() - remainPages];
            memcpy((char *) dataPage2 + PAGE_SIZE - sizeof(int),
                   &writeIN, sizeof(int));
            unsigned Entries;
            memcpy(&Entries, (char *) dataPage2 + PAGE_SIZE - sizeof(int) * 2,
                   sizeof(int));
            //cout<<"Entries:"<<Entries<<endl;
            writeIN=usedOFnumber[used-1];
            used++;
            metaFileHandle.writePage(writeIN, dataPage2);
        } else {
            //cout<<"numOfOverflowPages:"<<numOfOverflowPages<<"usedOFnumber.size():"<<usedOFnumber.size()<<endl;
            void *dataPage = newPages[newPages.size() - remainPages];
            //metaFileHandle.appendPage(dataPage);
            unsigned Entries;
            memcpy(&Entries, (char *) dataPage + PAGE_SIZE - sizeof(int) * 2,
                   sizeof(int));
            
            writeIN=usedOFnumber[used-1];
            metaFileHandle.writePage(writeIN, dataPage);
        }
        
        remainPages--;
    }
    return 0;
}
RC IndexManager::splitGetEntry(void *entry, const Attribute &attribute,
                               void *oldPage, int i) {
    int offset;
    switch (attribute.type) {
        case 0:
            offset = INTREAL_SLOT * i;
            memcpy(entry, (char *) oldPage + offset, INTREAL_SLOT);
            break;
        case 1:
            offset = INTREAL_SLOT * i;
            memcpy(entry, (char *) oldPage + offset, INTREAL_SLOT);
            break;
        case 2:   
            int PinDir = sizeof(int) * (i + 3); //???
            int offset;
            memcpy(&offset, (char *) oldPage + PAGE_SIZE - PinDir, sizeof(int));
            //cout<<"offset:"<<offset<<endl;
            int length;;
            memcpy(&length, (char *) oldPage + offset, sizeof(int));
            int totalLen = length + 3 * sizeof(int); //len+key+rid1+rid2
            memcpy(entry, (char *) oldPage + offset, totalLen);
            //cout<<*(int*)entry<<endl;
            break;
    }
    return 0;
}

RC IndexManager::splitInsertEntry(const Attribute &attribute, void *entry,
                                  void *oldPage, vector<void *> &newPages1, vector<void *> &newPages2) {
    switch (attribute.type) {
        case 0:
            splitInsertOneIntReal(attribute, entry, newPages1, newPages2);
            break;
        case 1:
            splitInsertOneIntReal(attribute, entry, newPages1, newPages2);
            break;
        case 2:
            splitInsertVarChar(attribute, entry, newPages1, newPages2);
            break;
    }
    return 0;
}

RC IndexManager::splitInsertOneIntReal(const Attribute &attribute, void *entry,
                                       vector<void *> &newPages1, vector<void *> &newPages2) {
    int key;
    memcpy(&key, (char *) entry, sizeof(int));
    int hashValue = hash(attribute, &key);
    //cout<<"hashValue:"<<hashValue<<endl;
    if(hashValue==attributeTable[attribute.name][1]){
        attributeTable[attribute.name][0]++;
        hashValue=hash(attribute,&key);
        attributeTable[attribute.name][0]--;
    }     
    if (hashValue == attributeTable[attribute.name][1]) {
      splitDoIntReal(entry,newPages1);

    } else {
        splitDoIntReal(entry,newPages2);
    }
    return 0;
}

RC IndexManager::splitDoIntReal(void *entry,vector<void *> &newPages) {
        void *newPage = newPages[newPages.size() - 1];
        unsigned Entries;
        memcpy(&Entries, (char *) newPage + PAGE_SIZE - sizeof(int) * 2,
               sizeof(int));
        if (Entries < INTREAL_NUMBER) {
            memcpy((char *) newPage + Entries * INTREAL_SLOT, (char *) entry,
                   INTREAL_SLOT);
            Entries++;
            memcpy((char *) newPage + PAGE_SIZE - sizeof(int) * 2, &Entries,sizeof(int));

        } else {
            void * newOFPage = malloc(PAGE_SIZE);
            memset(newOFPage, 0, PAGE_SIZE);
            memcpy((char *) newOFPage, (char *) entry, INTREAL_SLOT);
            int one = 1;
            memcpy((char *) newOFPage + PAGE_SIZE - sizeof(int) * 2, &one,
                   sizeof(int));
            newPages.push_back(newOFPage);
        }
        return 0;
}

RC IndexManager::splitInsertVarChar(const Attribute &attribute, void *entry,
                                    vector<void *> &newPages1, vector<void *> &newPages2) {
    int lenOfKey;
    memcpy(&lenOfKey, (char *) entry, sizeof(int));
    void *key = malloc(lenOfKey+sizeof(int));
    memset(key, 0, lenOfKey+sizeof(int));
   // cout<<"lenOfKey:"<<lenOfKey<<endl;
    memcpy(key, (char *) entry, lenOfKey + sizeof(int));
    int hashValue = hash(attribute, key); 
    if(hashValue==attributeTable[attribute.name][1]){ 
        attributeTable[attribute.name][0]++;
        hashValue=hash(attribute,key);
        attributeTable[attribute.name][0]--;
    }

    if (hashValue == attributeTable[attribute.name][1]) {
        splitDoInsChar(entry,newPages1,lenOfKey);
    } else {
        splitDoInsChar(entry,newPages2,lenOfKey);
    }
    free(key);
    return 0;
}

RC IndexManager::splitDoInsChar(void *entry,vector<void *> &newPages, int lenOfKey) {
   void * newPage = newPages[newPages.size() - 1];
        unsigned Entries;
        unsigned freePosition;
        unsigned freeSize = remainsForVarchar(newPage, freePosition, Entries);
        unsigned length = lenOfKey + 3 * sizeof(int);
        if (freeSize > length + sizeof(int)) {
            memcpy((char *) newPage + freePosition, (char *) entry, length);

            freePosition = freePosition + length;
                        Entries++;
            memcpy(
                   (char *) newPage + PAGE_SIZE
                   - sizeof(int) * (2 + Entries + 1), &freePosition,
                   sizeof(int));

            memcpy((char *) newPage + PAGE_SIZE - sizeof(int) * 2, &Entries,sizeof(int));
        } else { //full
            void * newOFPage = malloc(PAGE_SIZE);
            memset(newOFPage, 0, PAGE_SIZE);
            memcpy((char *) newOFPage, (char *) entry, length);
            int one = 1;
            memcpy((char *) newOFPage + PAGE_SIZE - sizeof(int) * 2, &one,
                   sizeof(int));
            memcpy((char *) newOFPage + PAGE_SIZE - sizeof(int) * 4,
                   &length, sizeof(int));
            newPages.push_back(newOFPage);
        }
    return 0;
}

unsigned IndexManager::remainsForVarchar(void* page, unsigned &freePosition,
                                         unsigned &Entries) {

    memcpy(&Entries, (char *) page + PAGE_SIZE - sizeof(int) * 2, sizeof(int));

    memcpy(&freePosition,
           (char *)page + PAGE_SIZE - sizeof(int) * (2 + Entries + 1),
           sizeof(int));
    return PAGE_SIZE - (freePosition + sizeof(int) * (2 + Entries + 1));
}

RC IndexManager::updateMetaAfterSplit(string attributeName, int level,
                                      int next) {
    int initBucket=attributeTable[attributeName][2];
    if (next == (int) pow(2, level)*initBucket - 1) {
        level++;
        next = 0;
    } else {
        next++;
    }
    attributeTable[attributeName][0] = level;
    attributeTable[attributeName][1] = next;
    
    return 0;
}

RC IndexManager::merge(const Attribute &attribute, string attributeName,
                       FileHandle &metaFileHandle, FileHandle &primaryFileHandle) {
    int level = attributeTable[attributeName][0]; //level, next, bucket init
    int next = attributeTable[attributeName][1];
    int bucketInit = attributeTable[attribute.name][2];
    if (level == 0 && next == 0) {
        //cout<<"level==0&&next==0"<<endl;
        return -1;
    }
    void *pagePrim1 = malloc(PAGE_SIZE);
    memset(pagePrim1, 0, PAGE_SIZE);
    void *pagePrim2 = malloc(PAGE_SIZE);
    memset(pagePrim2, 0, PAGE_SIZE);
    int hashvalue1 = next-1;
    int hashvalue2 = (int) (pow(2, level)*bucketInit + next-1);
    if(next==0){
        hashvalue1=(int) (pow(2, level-1)*bucketInit-1);
    }
    //cout<<"hashvalue1:"<<hashvalue1<<"   hashvalue2:"<<hashvalue2<<endl;
    if (primaryFileHandle.readPage(hashvalue1, pagePrim1) == 0
        && primaryFileHandle.readPage(hashvalue2, pagePrim2) == 0) {
        vector<void *> oldPages1;
        vector<void *> oldPages2;
        vector<int> usedOFnumber;
        oldPages1.push_back(pagePrim1);
        oldPages2.push_back(pagePrim2);
        prepareMerge(metaFileHandle, oldPages1, pagePrim1,usedOFnumber);
        prepareMerge(metaFileHandle, oldPages2, pagePrim2,usedOFnumber);
        
        vector<void *> newPages;
        void *newPage = malloc(PAGE_SIZE);
        memset(newPage, 0, PAGE_SIZE);
        primaryFileHandle.writePage(hashvalue2,newPage);//set 0
        newPages.push_back(newPage);
        sortMerge(attribute, oldPages1, oldPages2, newPages);
        //cout<<"mergeeeeeeeeeeeee"<<endl;
        writeMergedPages(newPages, primaryFileHandle, metaFileHandle, hashvalue1,usedOFnumber);
        updateMetaAfterMerge(attributeName, level, next);
        
        //free(pagePrim1);
        //free(pagePrim2);
        freeVector(oldPages1);
        freeVector(oldPages2);
        freeVector(newPages);
        return 0;
    }
    free(pagePrim1);
    free(pagePrim2);
    return 0;
}

RC IndexManager::prepareMerge(FileHandle &metaFileHandle,
                              vector<void *> &oldPages, void *pagePrim,vector<int> &usedOFnumber) {
    
    int PrimEntries;
    int OverflowPageNo;
    memcpy(&PrimEntries, (char *) pagePrim + PAGE_SIZE - sizeof(int) * 2,
           sizeof(int));
    memcpy(&OverflowPageNo, (char *) pagePrim + PAGE_SIZE - sizeof(int),
           sizeof(int));
    while (OverflowPageNo != 0) {
        usedOFnumber.push_back(OverflowPageNo);
        void *oldPage = malloc(PAGE_SIZE);
        memset(oldPage, 0, PAGE_SIZE);
        metaFileHandle.readPage(OverflowPageNo, oldPage);
        oldPages.push_back(oldPage);
        
        void *Page = malloc(PAGE_SIZE);
        memset(Page, 0, PAGE_SIZE);
        metaFileHandle.writePage(OverflowPageNo,Page);//set OFPages 0
        memcpy(&OverflowPageNo, (char *) oldPage + PAGE_SIZE - sizeof(int),
               sizeof(int));
        //free(oldPage);
    }
    return 0;
}

RC IndexManager::sortMerge(const Attribute attribute, vector<void *> &oldPages1,
                           vector<void *> &oldPages2, vector<void *> &newpages) {
    unsigned size1 = 0;
    unsigned size2 = 0;
    int i = 0;
    int j = 0;
    void *oldPage1 = malloc(PAGE_SIZE);
    memset(oldPage1, 0, PAGE_SIZE);
    void *oldPage2 = malloc(PAGE_SIZE);
    memset(oldPage2, 0, PAGE_SIZE);
    while (true) {
        
        
        
        oldPage1 = oldPages1[size1]; //start from 0
        oldPage2 = oldPages2[size2]; //start from 0
        
        void *entry1 = malloc(PAGE_SIZE);
        memset(entry1, 0, PAGE_SIZE);
        void *entry2 = malloc(PAGE_SIZE);
        memset(entry2, 0, PAGE_SIZE);
        int Entries1;
        memcpy(&Entries1, (char *) oldPage1 + PAGE_SIZE - sizeof(int) * 2,
               sizeof(int));
        
        int Entries2;
        memcpy(&Entries2, (char *) oldPage2 + PAGE_SIZE - sizeof(int) * 2,
               sizeof(int));
        
        splitGetEntry(entry1, attribute, oldPage1, i);
        splitGetEntry(entry2, attribute, oldPage2, j);
        bool winner = mergeCompareEntry(attribute, entry1, entry2);
        if (winner) {
            mergeInsertWinner(attribute, entry1, newpages);
            i++;
        } else {
            mergeInsertWinner(attribute, entry2, newpages);
            j++;
        }
        if (i == Entries1) {
            size1++;
            if (size1 == oldPages1.size()) {
                appendRemainPages(attribute, oldPages2, newpages,j,size2);
                return 0;
            }
            memset(oldPage1, 0, PAGE_SIZE);
            oldPage1 = oldPages1[size1];
            
        } else if (j == Entries2) {
            size2++;
            if (size2 == oldPages2.size()) {
                appendRemainPages(attribute, oldPages1, newpages,i,size1);
                return 0;
            }
            memset(oldPage2, 0, PAGE_SIZE);
            oldPage2 = oldPages2[size2];
            
        }
        free(entry1);
        free(entry2);
        
    }
    //free(oldPage1);
    //free(oldPage2);
    return 0;
}

RC IndexManager::appendRemainPages(const Attribute attribute, vector<void *> &oldPages, vector<void *> &newpages,int j,int size){
    while(true){
        void *entry = malloc(PAGE_SIZE);
        memset(entry, 0, PAGE_SIZE);
        void *oldPage = oldPages[size];
        int Entries;
        memcpy(&Entries, (char *) oldPage + PAGE_SIZE - sizeof(int) * 2, sizeof(int));
        splitGetEntry(entry, attribute, oldPage, j);
        mergeInsertWinner(attribute, entry, newpages);
        j++;
        if (j == Entries) {
            size++;
            if(size==oldPages.size()){
                return 0;
            }
            memset(oldPage, 0, PAGE_SIZE);
            oldPage = oldPages[size];
        }
    }
}

RC IndexManager::mergeCompareEntry(const Attribute &attribute, void *entry1,
                                   void *entry2) {
    int key1;
    int key2;
    switch (attribute.type) {
        case 0:
            memcpy(&key1, (char *) entry1, sizeof(int));
            memcpy(&key2, (char *) entry2, sizeof(int));
            if (key1 >= key2) {
                return 1;
            } else {
                return 0;
            }
            break;
        case 1:
            memcpy(&key1, (char *) entry1, sizeof(int));
            memcpy(&key2, (char *) entry2, sizeof(int));
            if (key1 >= key2) {
                return 1;
            } else {
                return 0;
            }
            break;
        case 2:
            int length1;
            memcpy(&length1, (char *) entry1, sizeof(int));
            memcpy(&key1, (char *) entry1, length1);
            int length2;
            memcpy(&length2, (char *) entry2, sizeof(int));
            memcpy(&key2, (char *) entry2, length2);
            if (key1 > key2) {
                return 1;
            } else {
                return 0;
            }
            break;
    }
    return 0;
}

RC IndexManager::mergeInsertWinner(const Attribute &attribute, void * entry,
                                   vector<void*> &newPages) {
    
    switch (attribute.type) {
        case 0:
            mergeInsertOneIntReal(attribute, entry, newPages);
            break;
        case 1:
            mergeInsertOneIntReal(attribute, entry, newPages);
            break;
        case 2:
            mergeInsertVarChar(attribute, entry, newPages);
            break;
    }
    return 0;
}

RC IndexManager::mergeInsertOneIntReal(const Attribute &attribute, void * entry,
                                       vector<void*> &newPages) {
    void *newPage = malloc(PAGE_SIZE);
    memset(newPage, 0, PAGE_SIZE);
    newPage = newPages[newPages.size() - 1];
    unsigned Entries;
    memcpy(&Entries, (char *) newPage + PAGE_SIZE - sizeof(int) * 2,
           sizeof(int));
    if (Entries < INTREAL_NUMBER) {
        memcpy((char *) newPage + Entries * INTREAL_SLOT, (char *) entry,
               INTREAL_SLOT);
        Entries++;
        memcpy((char *) newPage + PAGE_SIZE - sizeof(int) * 2, &Entries,sizeof(int));
        //newPages.pop_back();
        //newPages.push_back(newPage);
    } else { //(PrimEntries == INTREAL_NUMBER)
        void * newOFPage = malloc(PAGE_SIZE);
        memset(newOFPage, 0, PAGE_SIZE);
        memcpy((char *) newOFPage, (char *) entry, INTREAL_SLOT);
        int one = 1;
        memcpy((char *) newOFPage + PAGE_SIZE - sizeof(int) * 2, &one,
               sizeof(int));
        newPages.push_back(newOFPage);
        //free(newOFPage);
    }
    //free(newPage);
    return 0;
}
RC IndexManager::mergeInsertVarChar(const Attribute &attribute, void * entry,
                                    vector<void*> &newPages) {
    void * newPage = malloc(PAGE_SIZE);
    memset(newPage, 0, PAGE_SIZE);
    newPage = newPages[newPages.size() - 1]; //get primpage
    unsigned Entries;
    unsigned freePosition;
    unsigned freeSize = remainsForVarchar(newPage, freePosition, Entries);
    int lenOfKey;
    memcpy(&lenOfKey, (char *) entry, sizeof(int));
    unsigned length = lenOfKey + 3 * sizeof(int);
    if (freeSize < length + sizeof(int)) {
        memcpy((char *) newPage + freePosition, (char *) entry, length);
        freePosition = freePosition + length;
        memcpy((char *) newPage + PAGE_SIZE - sizeof(int) * (2 + Entries + 1),
               &freePosition, sizeof(int));
        Entries++;
        memcpy((char *) newPage + PAGE_SIZE - sizeof(int) * 2, &Entries,sizeof(int));
        //newPages.pop_back();
        //newPages.push_back(newPage);
    } else { //full
        void * newOFPage = malloc(PAGE_SIZE);
        memset(newOFPage, 0, PAGE_SIZE);
        memcpy((char *) newOFPage, (char *) entry, length);
        int one = 1;
        memcpy((char *) newOFPage + PAGE_SIZE - sizeof(int) * 2, &one,
               sizeof(int));
        memcpy((char *) newOFPage + PAGE_SIZE - sizeof(int) * 4, &freePosition,
               sizeof(int));
        newPages.push_back(newOFPage);
        //free(newOFPage);
    }
    //free(newPage);
    return 0;
}
RC IndexManager::writeMergedPages(vector<void *> &newPages,
                                  FileHandle &primaryFileHandle, FileHandle &metaFileHandle, int hashvalue1,vector<int> &usedOFnumber) {
    void *newPage = malloc(PAGE_SIZE);
    memset(newPage, 0, PAGE_SIZE);
    newPage = newPages[0];
    int used=0;
    if (newPages.size() > 1) {
        memcpy((char *) newPage + PAGE_SIZE - sizeof(int), &usedOFnumber[used],
               sizeof(int));
        used++;
        int numOfOverflowPages = metaFileHandle.getNumberOfPages() - 1;
        primaryFileHandle.writePage(hashvalue1, newPage);
        writeOverflow(metaFileHandle, newPages, numOfOverflowPages,usedOFnumber,used);
    } else {
        primaryFileHandle.writePage(hashvalue1, newPage);
    }
    //free(newPage);
    return 0;
}

RC IndexManager::updateMetaAfterMerge(string attributeName, int level,
                                      int next) {
    int initBucket=attributeTable[attributeName][2];
    if (next == 0 && level > 0) {
        level--;
        next = (int) pow(2, level)*initBucket - 1;
    } else {
        next--;
    }
    attributeTable[attributeName][0] = level;
    attributeTable[attributeName][1] = next;
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {
    //find bucket
    //cout<<"before deleting:"<<endl;
    //printIndexEntriesInAPage(ixfileHandle, attribute, 0);
    unsigned bucketNumber = hash(attribute, key);

    FileHandle &metaHandle = ixfileHandle.getMetaFile();
    FileHandle &primaryHandle = ixfileHandle.getPrimFile();

    //FindDeletePosition
    vector<void *> pages;
    void *targetPage = malloc(PAGE_SIZE);
    memset(targetPage, 0, PAGE_SIZE);
    //get total number of slots， pageNumber and number of slots per page
    
    primaryHandle.readPage(bucketNumber, targetPage);
    
    pages.push_back(targetPage);
    int totalNumber = 0;
    vector<int> pageNumbers;
    vector<int> pagePositions;
    memcpy(&totalNumber, (char*)targetPage+PAGE_SIZE- 2*sizeof(int), sizeof(int));
    if (totalNumber == 0) {
        return  -1;
    }
    pageNumbers.push_back(totalNumber);
    pagePositions.push_back(bucketNumber);
    int nextPage= 0;
    memcpy(&nextPage, (char*)targetPage+PAGE_SIZE-sizeof(int), sizeof(int));
    while (nextPage!=0) {
        pagePositions.push_back(nextPage);
        void *overfowPage = malloc(PAGE_SIZE);
        metaHandle.readPage(nextPage, overfowPage);
        
        pages.push_back(overfowPage);
        memcpy(&nextPage, (char*)overfowPage + PAGE_SIZE - sizeof(int), sizeof(int));
        int slotNumber;
        memcpy(&slotNumber, (char*)overfowPage + PAGE_SIZE - 2*sizeof(int), sizeof(int));
        totalNumber+=slotNumber;
        pageNumbers.push_back(slotNumber);
    }

    int deletePosition = findDeletePosition(0,key, rid,attribute,pages,totalNumber,pageNumbers);
    if (deletePosition == -1)
        return -1;

    int pagePosition = deleteFromPages(deletePosition, pageNumbers, key, attribute, pages, rid);

    //writeToPages
    for (int i=0; i<pages.size(); i++) {
        if (i==0) {
            primaryHandle.writePage(pagePositions[0], pages[0]);
            
        }
        else
        {
            metaHandle.writePage(pagePositions[i], pages[i]);
            
        }
    }

    int numberInFirst=0;
    memcpy(&numberInFirst, (char*)pages[0]+PAGE_SIZE-2*sizeof(int), sizeof(int));
    if (numberInFirst == 0) {
        merge(attribute,attribute.name,primaryHandle, metaHandle);
    }
	return 0;
}
int IndexManager::deleteFromPages(int position, vector<int> &pageNumbers, const void *key, const Attribute attribute, vector<void *> &page, const RID &rid)
{
    if (attribute.type !=2) {
        //find the position of the page
        int pagePosition = 0;
        while(pageNumbers[pagePosition]>0 and position > pageNumbers[pagePosition]-1) {
            position -= pageNumbers[pagePosition];
            pagePosition += 1;
        }
        //delete within the same page
        int nextPage;
        memcpy(&nextPage, (char*)page[pagePosition]+PAGE_SIZE-sizeof(int), sizeof(int));
        if (nextPage == 0)
        {
            //from slot+1 to end
            int moveSize = INTREAL_SLOT*(pageNumbers[pagePosition]-1-position);
            void *tmp = malloc(moveSize);
            memcpy(tmp, (char*)page[pagePosition]+(position+1)*INTREAL_SLOT ,moveSize);
            memcpy((char*)page[pagePosition]+position*INTREAL_SLOT , tmp, moveSize);
            pageNumbers[pagePosition] -= 1;
            memcpy( (char*)page[pagePosition] + PAGE_SIZE - 2*sizeof(int), &pageNumbers[pagePosition], sizeof(int));
            free(tmp);
            return pagePosition;
        }

        int currentPage = pagePosition;
        int currentPosition = position;
        while (nextPage != 0) {
            //get headData
            void *headData = malloc(INTREAL_SLOT);
            memset(headData, 0, INTREAL_SLOT);
            memcpy(headData, page[currentPage+1], INTREAL_SLOT);

            //pageNumbers[currentPage+1] -= 1;
            //memcpy((char*)page[currentPage+1] + PAGE_SIZE - sizeof(int),&pageNumbers[currentPage+1],sizeof(int));
            int intrealnumber = INTREAL_NUMBER;
            int moveSize = INTREAL_SLOT*(intrealnumber-1-currentPosition);
            void *tmp = malloc(moveSize);
            memcpy(tmp, (char*)page[currentPage]+(currentPosition+1)*INTREAL_SLOT ,moveSize);
            memcpy((char*)page[currentPage]+currentPosition*INTREAL_SLOT , tmp, moveSize);
            memcpy((char*)page[currentPage] + (intrealnumber-1)*INTREAL_SLOT, headData, INTREAL_SLOT);
            free(tmp);

            currentPage+=1;
            currentPosition = 0;
            memcpy(&nextPage, (char*)page[currentPage]+PAGE_SIZE-sizeof(int), sizeof(int));
        }

        int moveSize = INTREAL_SLOT*(pageNumbers[currentPage]-1-currentPosition);
        void *tmp = malloc(moveSize);
        memcpy(tmp, (char*)page[currentPage]+(currentPosition+1)*INTREAL_SLOT ,moveSize);
        memcpy((char*)page[currentPage]+currentPosition*INTREAL_SLOT , tmp, moveSize);
        pageNumbers[currentPage] -= 1;
        memcpy( (char*)page[currentPage] + PAGE_SIZE - 2*sizeof(int), &pageNumbers[currentPage], sizeof(int));
        if (pageNumbers[currentPage] == 0) {
            int n = 0;
            memcpy( (char*)page[currentPage-1] + PAGE_SIZE - sizeof(int),&n, sizeof(int));
        }
        free(tmp);
        return pagePosition;
    }
    else
    {
        //find the position of the page
        int pagePosition = 0;
        while(pageNumbers[pagePosition]>0 and position > pageNumbers[pagePosition]-1) {
            position -= pageNumbers[pagePosition];
            pagePosition += 1;
        }
        //delete From the fist page
        deleteFromCertianPage(position, page, pageNumbers, pagePosition);
        int nextPage;
        memcpy(&nextPage, (char*)page[pagePosition]+PAGE_SIZE-sizeof(int), sizeof(int));
        //move among pages
        int currentPage = pagePosition;
        int currentPosition = position;
        while (nextPage != 0) {
            //get headData
            void *headData = malloc(PAGE_SIZE);
            int tmplength = getVarcharSlot(headData,page[currentPage], 0);
            
            if (checkVarcharFull(1, tmplength, page[currentPage])) {
                break;
            }
            //insert headData to the end of the page
            else
            {
                //insert data
                int insertoffset = getVarcharOffset(page[currentPage], pageNumbers[currentPage]);
                memcpy((char*)page[currentPage]+insertoffset, headData, tmplength);
                //modify slot
                int lastoffset = insertoffset+tmplength;
                memcpy((char*)page[currentPage]+PAGE_SIZE-(pageNumbers[currentPage]+4)*sizeof(int), &lastoffset, sizeof(int));
                
                //add pagenumber
                pageNumbers[pagePosition] += 1;
                memcpy( (char*)page[pagePosition] + PAGE_SIZE - 2*sizeof(int), &pageNumbers[pagePosition], sizeof(int));
                
                deleteFromCertianPage(0, page, pageNumbers, currentPage+1);
                currentPage+=1;
                currentPosition = 0;
                memcpy(&nextPage, (char*)page[currentPage]+PAGE_SIZE-sizeof(int), sizeof(int));
            }
        }
        return pagePosition;
    }
}
void IndexManager::deleteFromCertianPage(int position, vector<void*> page, vector<int> &pageNumbers, int pagePosition)
{
    int startposition = getVarcharOffset(page[pagePosition], position+1);
    int endposition = getVarcharOffset(page[pagePosition], pageNumbers[pagePosition]);
    int moveSize = endposition-startposition;
    void *tmp = malloc(moveSize);
    memcpy(tmp, (char*)page[pagePosition]+startposition,moveSize);
    memcpy((char*)page[pagePosition]+ getVarcharOffset(page[pagePosition], position), tmp, moveSize);
    pageNumbers[pagePosition] -= 1;
    memcpy( (char*)page[pagePosition] + PAGE_SIZE - 2*sizeof(int), &pageNumbers[pagePosition], sizeof(int));
    //moveslot
    void *t = malloc(PAGE_SIZE);
    int slotlength = getVarcharSlot(t, page[pagePosition], position);
    moveVarChardirDelete(position, pageNumbers[pagePosition]-1,slotlength, page[pagePosition]);
    free(tmp);
    free(t);
}
int IndexManager::findDeletePosition(int start, const void *key, const RID &rid, const Attribute attribute, vector<void *> &pages, int totalNumber, vector<int> pageNumbers)
{
    if (attribute.type == 0)
    {
        int keyValue = *(int*)key;
        int head = start;
        int tail = 0;
        if (totalNumber > 0)
            tail = totalNumber-1;
        else
            return 0;
        //int insertPosion = 0;
        while (head<=tail) {
            int number = INTREAL_NUMBER;
            int middleNumber = (tail+head)/2 ;   //middleNumber th number in total
            int pageNumber = middleNumber/number;
            int offset = middleNumber%number;
            int targetValue;
            memcpy(&targetValue, (char*)pages[pageNumber]+offset*INTREAL_SLOT, sizeof(int));
            if (keyValue == targetValue)
            {
                if (offset > 0) {
                    offset -= 1;
                    memcpy(&targetValue, (char*)pages[pageNumber]+(offset)*INTREAL_SLOT, sizeof(int));
                    while (keyValue == targetValue and offset >= head) {
                        offset-=1;
                        memcpy(&targetValue, (char*)pages[pageNumber]+(offset)*INTREAL_SLOT, sizeof(int));
                    }
                    offset +=1;
                }
                int p;
                int s;
                memcpy(&p, (char*)pages[pageNumber]+offset*INTREAL_SLOT+sizeof(int), sizeof(int));
                memcpy(&s, (char*)pages[pageNumber]+offset*INTREAL_SLOT+2*sizeof(int), sizeof(int));
                if (p==rid.pageNum and s==rid.slotNum) {
                    return offset+pageNumber*number;
                }
                else
                {
                    return findDeletePosition(offset+1, key, rid, attribute, pages, totalNumber,pageNumbers);
                }
            }
            if (keyValue > targetValue)
                head = middleNumber +1;
            else
                tail = middleNumber -1;
            }
        return -1;
    }
    if (attribute.type == 1)
    {
        float keyValue = *(float*)key;
        int head = start;
        int tail = 0;
        if (totalNumber > 0)
            tail = totalNumber-1;
        else
            return 0;
        //int insertPosion = 0;
        while (head<=tail) {
            int number = INTREAL_NUMBER;
            int middleNumber = (tail+head)/2 ;   //middleNumber th number in total
            int pageNumber = middleNumber/number;
            int offset = middleNumber%number;
            float targetValue;
            memcpy(&targetValue, (char*)pages[pageNumber]+offset*INTREAL_SLOT, sizeof(int));
            if (keyValue == targetValue)
            {
                if (offset > 0) {
                        offset-=1;
                        memcpy(&targetValue, (char*)pages[pageNumber]+(offset)*INTREAL_SLOT, sizeof(int));
                    while (keyValue == targetValue and offset >= head) {
                        offset-=1;
                        memcpy(&targetValue, (char*)pages[pageNumber]+(offset)*INTREAL_SLOT, sizeof(int));
                    }
                    offset +=1;
                }
                int p;
                int s;
                memcpy(&p, (char*)pages[pageNumber]+offset*INTREAL_SLOT+sizeof(int), sizeof(int));
                memcpy(&s, (char*)pages[pageNumber]+offset*INTREAL_SLOT+2*sizeof(int), sizeof(int));
                if (p==rid.pageNum and s==rid.slotNum) {
                    return offset+pageNumber*number;
                }
                else
                {
                    return findDeletePosition(offset+1, key, rid, attribute, pages, totalNumber,pageNumbers);
                }
            }
            if (keyValue > targetValue)
                head = middleNumber +1;
            else
                tail = middleNumber -1;
            }
        return -1;
    }
    else
    {
        string keyValue = "";
        int keyLength = *(int*)key;
        keyValue.assign((char*)key+sizeof(int),(char*)key+sizeof(int)+keyLength);
        int head = start;
        int tail = 0;
        if (totalNumber > 0)
            tail = totalNumber-1;
        else
            return 0;
        //int insertPosion = 0;
        while (head<=tail) {
            int middleNumber = (tail+head)/2 ;   //middleNumber th number in total
            int offset = 0;
            int pageNumber = 0;
            middleTopage(middleNumber, pageNumbers, offset, pageNumber);
            string targetValue= getVarcharValue(pages[pageNumber], offset);
            if (keyValue == targetValue)
            {
                if (offset > 0) {
                    offset-=1;
                    int tmppagenumber;
                    int tmpoffset;
                    middleTopage(offset, pageNumbers, tmpoffset, tmppagenumber);
                    targetValue = getVarcharValue(pages[tmppagenumber], tmpoffset);
                    while (keyValue == targetValue and offset >= head) {
                        offset-=1;
                        int tmppagenumber;
                        int tmpoffset;
                        middleTopage(offset, pageNumbers, tmpoffset, tmppagenumber);
                        targetValue = getVarcharValue(pages[tmppagenumber], tmpoffset);
                    }
                    offset +=1;
                }
                RID tmprid;
                getVarcharRid(tmprid, pages[pageNumber], offset);
                if (tmprid.pageNum==rid.pageNum and tmprid.slotNum==rid.slotNum) {
                    // return offset;
                    int beforenumber=0;
                    for (int i=0; i<pageNumber; i++) {
                        beforenumber+=pageNumbers[i];
                    }
                    return offset+beforenumber;
                }
                else
                {
                    return findDeletePosition(offset+1, key, rid, attribute, pages, totalNumber,pageNumbers);
                }
            }
            if (keyValue > targetValue)
                head = middleNumber +1;
            else
                tail = middleNumber -1;
        }
        return -1;
    }
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key) {
    int level = attributeTable[attribute.name][0]; //level, next, bucket init
    int next = attributeTable[attribute.name][1];
    int bucketInit = attributeTable[attribute.name][2];
    int value;
    int result = 0;
    if (attribute.type == 0) {
        memcpy(&value, key, sizeof(int));
    } else {
        if (attribute.type == 1) {
            memcpy(&value, key, sizeof(float));
            //value = value & 0xfffff000;
            value = value >> 12;
        } else {
            int length;
            memcpy(&length, key, sizeof(int));
            char first;
            char last;
            memcpy(&first, key, sizeof(char));
            memcpy(&last, (char*) key + length - 1, sizeof(char));
            value = int(first) + int(last);
        }
    }
   result = value % (bucketInit<<level);
    if (result < next) {
        result = value % (bucketInit<<(level+1));
        //cout<<"????????????????"<<endl;
    }
    //cout<<"result:"<<result<<endl;
    return result;
}

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle,
                                          const Attribute &attribute, const unsigned &primaryPageNumber) {
    
    FileHandle fhMeta;
    FileHandle fhPrim;
    ixfileHandle.fetchFileHandle(fhMeta, fhPrim);
    //PreparePrint(fhPrim, fhMeta, attribute, primaryPageNumber);
    
    void *pagePrim = malloc(PAGE_SIZE);
    memset(pagePrim, 0, PAGE_SIZE);
    if (fhPrim.readPage(primaryPageNumber, pagePrim) == 0) {
        int PrimEntries;
        int OverflowEntries = 0;
        int OverflowPageNo = 0;
        
        
        memcpy(&PrimEntries, (char *) pagePrim + PAGE_SIZE- sizeof(int) * 2,
               sizeof(int));
        int totalEntries = PrimEntries;
        memcpy(&OverflowPageNo, (char *) pagePrim + PAGE_SIZE- sizeof(int),
               sizeof(int));
        
        while (OverflowPageNo != 0) {
            //cout<<"@@@OverflowPageNo:"<<OverflowPageNo<<endl;
            void *pageMeta = malloc(PAGE_SIZE);
            memset(pageMeta, 0, PAGE_SIZE);
            if (fhMeta.readPage(OverflowPageNo, pageMeta) == 0) {
                memcpy(&OverflowPageNo,
                       (char *) pageMeta + PAGE_SIZE - sizeof(int),
                       sizeof(int));
                memcpy(&OverflowEntries,
                       (char *) pageMeta + PAGE_SIZE- sizeof(int) * 2,
                       sizeof(int));
                totalEntries+=OverflowEntries;
                // cout<<"@@@OverflowEntries:"<<OverflowEntries<<endl;
            }
            
            free(pageMeta);
        }
        
        cout << "Number of total entries in the page (+ overflow pages) : "
        << totalEntries << endl;
        cout << "primary Page No." << primaryPageNumber << endl;
        cout << " a.# of entries : " << PrimEntries << endl;
        
        printEntriesInPrim(fhPrim, attribute, pagePrim);
        memcpy(&OverflowPageNo, (char *) pagePrim + PAGE_SIZE- sizeof(int),
               sizeof(int));
        while (OverflowPageNo != 0) {
            cout << "overflow Page No." << OverflowPageNo
            << " linked to [primary | overflow] page "
            << primaryPageNumber << endl;
            printEntriesInMeta(fhMeta, attribute, OverflowPageNo);
            
        }
        free(pagePrim);
        return 0;
    }
    free(pagePrim);
    return -1;
}

int IndexManager::printEntriesInPrim(FileHandle &fhPrim,
                                     const Attribute &attribute, void *pagePrim) {
    switch (attribute.type) {
        case 0:
            IntPrint(pagePrim);
            break;
        case 1:
            RealPrint(pagePrim);
            break;
        case 2:
            VarCharPrint(pagePrim);
            break;
    }
    return 0;
}

int IndexManager::printEntriesInMeta(FileHandle &fhMeta,const Attribute &attribute, int &OverflowPageNo) {
    
    void *pageMeta = malloc(PAGE_SIZE);
    memset(pageMeta, 0, PAGE_SIZE);
    if (fhMeta.readPage(OverflowPageNo, pageMeta) == 0) {
        memcpy(&OverflowPageNo, (char *) pageMeta + PAGE_SIZE- sizeof(int),
               sizeof(int));
        int OverflowEntries;
        memcpy(&OverflowEntries,
               (char *) pageMeta + PAGE_SIZE - sizeof(int) * 2, sizeof(int));
        cout << " a.# of entries : " << OverflowEntries << endl;
        
        int NextPage;
        
        switch (attribute.type) {
            case 0:
                NextPage = IntPrint(pageMeta);
                break;
            case 1:
                NextPage = RealPrint(pageMeta);
                break;
            case 2:
                NextPage = VarCharPrint(pageMeta);
                break;
                
        }
        free(pageMeta);
        return 0;
    }
    free(pageMeta);
    return -1;
}

int IndexManager::IntPrint(void *page) {
    int Entries;
    memcpy(&Entries, (char *) page + PAGE_SIZE - sizeof(int) * 2, sizeof(int));
    cout << " b. entries:" << endl;
    for (int i = 0; i < Entries; i++) {
        int offset = INTREAL_SLOT * i;
        int key;
        RID rid;
        //float key=*(float *) ((char *) page + offset);
        memcpy(&key, (char *) page + offset, sizeof(int));
        memcpy(&rid.pageNum, (char *) page + offset + sizeof(int), sizeof(int));
        memcpy(&rid.slotNum, (char *) page + offset + sizeof(int) * 2,
               sizeof(int));
        cout << "[" << key << "/" << rid.pageNum << "," << rid.slotNum << "] ";
    }
    int NextPage;
    memcpy(&NextPage, (char *) page + PAGE_SIZE - sizeof(int), sizeof(int));
    return NextPage;

}

int IndexManager::RealPrint(void *page) {
    int Entries;
    memcpy(&Entries, (char *) page + PAGE_SIZE - sizeof(int) * 2, sizeof(int));
    cout << " b. entries:" << endl;
    for (int i = 0; i < Entries; i++) {
        int offset = INTREAL_SLOT * i;
        //int key;
        RID rid;
        float key=*(float *) ((char *) page + offset);
        //memcpy(&key, (char *) page + offset, sizeof(int));
        memcpy(&rid.pageNum, (char *) page + offset + sizeof(int), sizeof(int));
        memcpy(&rid.slotNum, (char *) page + offset + sizeof(int) * 2,
               sizeof(int));
        cout << "[" << key << "/" << rid.pageNum << "," << rid.slotNum << "] ";
    }
    int NextPage;
    memcpy(&NextPage, (char *) page + PAGE_SIZE - sizeof(int), sizeof(int));
    return NextPage;

}

int IndexManager::VarCharPrint(void *page) {
    int Entries;
    memcpy(&Entries, (char *) page + PAGE_SIZE - sizeof(int) * 2, sizeof(int));
    cout << " b. entries:" << endl;
    for (int i = 0; i < Entries; i++) {
        //cout<<'Entries:'<<Entries<<endl;
        int PinDir = sizeof(int) * (i + 3);
        //int PinDir = sizeof(int)*3;
        int offset;
        memcpy(&offset, (char *) page + PAGE_SIZE - PinDir, sizeof(int));
        int length;
        
        RID rid;
        //<len+key,rid,next>
        memcpy(&length, (char *) page + offset, sizeof(int));
        
        char* key = new char[length + 1];
        memcpy(key, (char *) ((char*) page + offset + sizeof(int)), length);
        key[length] = '\0';
        
        memcpy(key, (char *) page + offset + sizeof(int), length);
        memcpy(&rid.pageNum, (char *) page + offset + sizeof(int) + length,
               sizeof(int));
        memcpy(&rid.slotNum, (char *) page + offset + sizeof(int) * 2 + length,
               sizeof(int));
        cout << "[" << key << "/" << rid.pageNum << "," << rid.slotNum << "] ";
    }
    int NextPage;
    memcpy(&NextPage, (char *) page + PAGE_SIZE - sizeof(int), sizeof(int));
    return NextPage;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle,
		unsigned &numberOfPrimaryPages) {
	numberOfPrimaryPages = ixfileHandle.getPrimPageNumber();
	return 0;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle,
		unsigned &numberOfAllPages) {
	numberOfAllPages = ixfileHandle.getPrimPageNumber()
			+ ixfileHandle.getMetaPageNumber();
	return 0;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator) {
    ix_ScanIterator.init(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
    return 0;
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}
void IX_ScanIterator::init(IXFileHandle &ixfileHandle, const Attribute &attribute,
          const void *lowKey, const void *highKey, bool lowKeyInclusive,
          bool highKeyInclusive)
{
    //ixfileHandle.fetchFileHandle(metaFile, primeFile);
    metaFile = &ixfileHandle.getMetaFile();
    primeFile = &ixfileHandle.getPrimFile();
    this->ixfileHandle = &ixfileHandle;
    this->attribute = attribute;
    this->lowKeyInclusive = lowKeyInclusive;
    this->highkeyInclusive = highKeyInclusive;
    this->data = malloc(PAGE_SIZE);
    primeFile->readPage(0, data);

    startPosition = 0;
    currentPage = 0;
    primaryPage = primeFile->getNumberOfPages();
    totalPage = metaFile->getNumberOfPages() + primaryPage -1;

    if (attribute.type!=2) {
        keySize = sizeof(int);
        if (lowKey == NULL) {
            this->lowKey = NULL;
        }
        else
        {
        this->lowKey = malloc(sizeof(int));
        memcpy(this->lowKey, lowKey, sizeof(int));
        }
        if (highKey==NULL) {
            this->highKey=NULL;
        }
        else
        {
        this->highKey = malloc(sizeof(int));
        memcpy(this->highKey, highKey , sizeof(int));
        }
    }
    else
    {
        keySize = 0;
        if (highKey==NULL) {
            this->highKey=NULL;
        }
        else
        {
        int length = *(int*)highKey;
        this->highKey = malloc(length+sizeof(int));
        memcpy(this->highKey, highKey, sizeof(int)+length);
        keySize = length + sizeof(int);
        }
        if (lowKey==NULL) {
            this->lowKey =NULL;
        }
        else
        {
        int length2 = *(int*)lowKey;
        this->lowKey = malloc(sizeof(int)+length2);
        memcpy(this->lowKey, lowKey, sizeof(int)+length2);
        keySize = length2 + sizeof(int);
        }
    }

    cout<<"return init"<<endl;
}
RC IX_ScanIterator::exactMatch(RID &rid, void *key)
{
    //check if there are changes after last match
    if (attribute.type == 0)
    {
        if (currentPage +1> totalPage) {
            return EOF;
        }

        //check if there are data in the slot
        int slotNumber;
        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
        if (slotNumber==0) {
            currentPage += 1;
            startPosition = 0;
            if (currentPage +1> totalPage) {
                return EOF;
            }
            if (currentPage+1 <= primaryPage) {
                primeFile->readPage(currentPage, data);
             //   //ixfileHandle->addread();
            }
            else
            {
                    metaFile->readPage(currentPage+1-primaryPage, data);
              //      //ixfileHandle->addread();
            }
        }
        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
        int keyValue = *(int*)key;
        int head = startPosition;
        int tail = slotNumber-1;
        while (head<=tail) {
            int middleNumber = (tail+head)/2 ;   //middleNumber th number in total
            //int pageNumber = middleNumber/INTREAL_NUMBER;
            int offset = middleNumber;
            int targetValue;
            memcpy(&targetValue, (char*)data+offset*INTREAL_SLOT, sizeof(int));
            if (keyValue == targetValue)
            {
                if (offset > 0) {
                    offset -= 1;
                    memcpy(&targetValue, (char*)data+(offset)*INTREAL_SLOT, sizeof(int));
                    while (keyValue == targetValue and offset >= head) {
                        offset-=1;
                        memcpy(&targetValue, (char*)data+(offset)*INTREAL_SLOT, sizeof(int));
                    }
                    offset +=1;
                }
                memcpy(&rid.pageNum, (char*)data+offset*INTREAL_SLOT+sizeof(int), sizeof(int));
                memcpy(&rid.slotNum, (char*)data+offset*INTREAL_SLOT+2*sizeof(int), sizeof(int));
                startPosition = offset + 1;
                if (startPosition+1>slotNumber) {
                    currentPage += 1;
                    startPosition = 0;
                    if (currentPage+1 <= primaryPage) {
                        primeFile->readPage(currentPage, data);
               //         //ixfileHandle->addread();
                    }
                    else
                    {
                        metaFile->readPage(currentPage+1-primaryPage, data);
               //         //ixfileHandle->addread();
                    }
                }
                return 0;
            }
            if (keyValue > targetValue)
                head = middleNumber +1;
            else
                tail = middleNumber -1;
        }
        currentPage += 1;
        startPosition = 0;
        if (currentPage > totalPage) {
            return EOF;
        }
        if (currentPage+1 <= primaryPage) {
            primeFile->readPage(currentPage, data);
            //ixfileHandle->addread();
        }
        else
        {
            metaFile->readPage(currentPage-primaryPage+1, data);
            //ixfileHandle->addread();
        }
        return exactMatch(rid, key);
    }
    if (attribute.type==1) {
        if (currentPage +1> totalPage) {
            return EOF;
        }

        //check if there are data in the slot
        int slotNumber;
        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
        if (slotNumber==0) {
            currentPage += 1;
            startPosition = 0;
            if (currentPage +1> totalPage) {
                return EOF;
            }
            if (currentPage+1 <= primaryPage) {
                primeFile->readPage(currentPage, data);
                //ixfileHandle->addread();
            }
            else
            {
                    metaFile->readPage(currentPage+1-primaryPage, data);
                    //ixfileHandle->addread();
            }
        }
        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
        float keyValue = *(float*)key;
        int head = startPosition;
        int tail = slotNumber-1;
        while (head<=tail) {
            int middleNumber = (tail+head)/2 ;   //middleNumber th number in total
            //int pageNumber = middleNumber/INTREAL_NUMBER;
            int offset = middleNumber;
            float targetValue;
            memcpy(&targetValue, (char*)data+offset*INTREAL_SLOT, sizeof(int));
            if (keyValue == targetValue)
            {
                if (offset > 0) {
                    offset -= 1;
                    memcpy(&targetValue, (char*)data+(offset)*INTREAL_SLOT, sizeof(int));
                    while (keyValue == targetValue and offset >= head) {
                        offset-=1;
                        memcpy(&targetValue, (char*)data+(offset)*INTREAL_SLOT, sizeof(int));
                    }
                    offset +=1;
                }
                memcpy(&rid.pageNum, (char*)data+offset*INTREAL_SLOT+sizeof(int), sizeof(int));
                memcpy(&rid.slotNum, (char*)data+offset*INTREAL_SLOT+2*sizeof(int), sizeof(int));
                startPosition = offset + 1;
                if (startPosition+1>slotNumber) {
                    currentPage += 1;
                    startPosition = 0;
                    if (currentPage+1 <= primaryPage) {
                        primeFile->readPage(currentPage, data);
                        //ixfileHandle->addread();
                    }
                    else
                    {
                        metaFile->readPage(currentPage+1-primaryPage, data);
                        //ixfileHandle->addread();
                    }
                }
                return 0;
            }
            if (keyValue > targetValue)
                head = middleNumber +1;
            else
                tail = middleNumber -1;
        }
        currentPage += 1;
        startPosition = 0;
        if (currentPage > totalPage) {
            return EOF;
        }
        if (currentPage+1 <= primaryPage) {
            primeFile->readPage(currentPage, data);
            //ixfileHandle->addread();
        }
        else
        {
            metaFile->readPage(currentPage-primaryPage+1, data);
            //ixfileHandle->addread();
        }
        return exactMatch(rid, key);
    
    }
    else
    {
        if (currentPage +1> totalPage) {
            return EOF;
        }
        
        //check if there are data in the slot
        int slotNumber;
        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
        if (slotNumber==0) {
            currentPage += 1;
            startPosition = 0;
            if (currentPage +1> totalPage) {
                return EOF;
            }
            if (currentPage+1 <= primaryPage) {
                primeFile->readPage(currentPage, data);
                //ixfileHandle->addread();
            }
            else
            {
                metaFile->readPage(currentPage+1-primaryPage, data);
                //ixfileHandle->addread();
            }
        }
        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
        string keyValue = "";
        getValueForVarchar(keyValue, key);
        int head = startPosition;
        int tail = slotNumber-1;
        while (head<=tail) {
            int middleNumber = (tail+head)/2 ;   //middleNumber th number in total
            //int pageNumber = middleNumber/INTREAL_NUMBER;
            int offset = middleNumber;
            string targetValue="";
            targetValue=getVarcharValue(data, offset);
            //memcpy(&targetValue, (char*)data+offset*INTREAL_SLOT, sizeof(int));
            if (keyValue == targetValue)
            {
                if (offset > 0) {
                    offset -= 1;
                    targetValue=getVarcharValue(data, offset);
                    while (keyValue == targetValue and offset >= head) {
                        offset-=1;
                        targetValue=getVarcharValue(data, offset);
                    }
                    offset +=1;
                }
                getVarcharRid(rid,data,offset);
                startPosition = offset + 1;
                if (startPosition+1>slotNumber) {
                    currentPage += 1;
                    startPosition = 0;
                    if (currentPage+1 <= primaryPage) {
                        primeFile->readPage(currentPage, data);
                        //ixfileHandle->addread();
                    }
                    else
                    {
                        metaFile->readPage(currentPage+1-primaryPage, data);
                        //ixfileHandle->addread();
                    }
                }
                return 0;
            }
            if (keyValue > targetValue)
                head = middleNumber +1;
            else
                tail = middleNumber -1;
        }
        currentPage += 1;
        startPosition = 0;
        if (currentPage > totalPage) {
            return EOF;
        }
        if (currentPage+1 <= primaryPage) {
            primeFile->readPage(currentPage, data);
            //ixfileHandle->addread();
        }
        else
        {
            metaFile->readPage(currentPage-primaryPage+1, data);
            //ixfileHandle->addread();
        }
        return exactMatch(rid, key);
    }
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    void *tmpData = malloc(PAGE_SIZE);
    memset(tmpData,0,PAGE_SIZE);
    if (currentPage +1> totalPage) {
        return EOF;
    }
    cout<<"currentPage:"<<currentPage<<endl;
    cout<<"primaryPage:"<<primaryPage<<endl;
    if (currentPage+1 <= primaryPage) {
        cout<<"rangeMatch0"<<endl;
        primeFile->readPage(currentPage, tmpData);

        //ixfileHandle->addread();
    }
    else
    {
        metaFile->readPage(currentPage+1-primaryPage, tmpData);
        //ixfileHandle->addread();
    }


    cout<<"rangeMatch1"<<endl;

    int Entries;
    memcpy(&Entries, (char *) tmpData + PAGE_SIZE - sizeof(int) * 2, sizeof(int));
    cout << "Entries:" << Entries <<endl;
    cout << " b. entries:" << endl;
    for (int i = 0; i < Entries; i++) {
        int offset = INTREAL_SLOT * i;
        //int key;
        RID rid;
        float key=*(float *) ((char *) tmpData + offset);
        //memcpy(&key, (char *) page + offset, sizeof(int));
        memcpy(&rid.pageNum, (char *) tmpData + offset + sizeof(int), sizeof(int));
        memcpy(&rid.slotNum, (char *) tmpData + offset + sizeof(int) * 2,
               sizeof(int));
        cout << "[" << key << "/" << rid.pageNum << "," << rid.slotNum << "] ";
    }
    int NextPage;
    memcpy(&NextPage, (char *) tmpData + PAGE_SIZE - sizeof(int), sizeof(int));



    cout<<"rangeMatch2"<<endl;
    if (memcmp(tmpData, data, PAGE_SIZE)!=0) {
        data = tmpData;
        startPosition = 0;
       // currentPage = 0;
    }
    cout<<"rangeMatch3"<<endl;
    if (highKey != NULL and lowKey != NULL) {
        if (memcmp(highKey,lowKey, keySize)==0){
            return exactMatch(rid, highKey);
        }
        else
            return rangeMatch(rid, key);
    }
    else
    {
        cout<<"rangeMatch4"<<endl;
        return rangeMatch(rid, key);
    }
}
RC IX_ScanIterator::rangeMatch(RID &rid, void *key)
{
    cout<<"attribute.type:"<<attribute.type<<endl;
    if (attribute.type==0) {
        //check if it comes to end
        if (currentPage +1> totalPage) {
            return EOF;
        }

        //check if there are data in the slot
        int slotNumber;
        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
        if (slotNumber==0) {
            currentPage += 1;
            if (currentPage +1> totalPage) {
                return EOF;
            }
            if (currentPage+1 <= primaryPage) {
                primeFile->readPage(currentPage, data);
                //ixfileHandle->addread();
            }
            else
            {
                metaFile->readPage(currentPage+1-primaryPage, data);
                //ixfileHandle->addread();
            }
            startPosition = 0;
            return rangeMatch(rid, key);
        }
        int targetData;
        memcpy(&targetData, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));

        //without any limitation
        if (highKey==NULL and lowKey==NULL) {
            memcpy(key, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
            memcpy(&rid.pageNum, (char*)data+startPosition*INTREAL_SLOT+sizeof(int), sizeof(int));
            memcpy(&rid.slotNum, (char*)data+startPosition*INTREAL_SLOT+2*sizeof(int), sizeof(int));
            startPosition += 1;
            if (startPosition+1>slotNumber) {
                currentPage += 1;
                startPosition = 0;
                if (currentPage+1 <= primaryPage) {
                    primeFile->readPage(currentPage, data);
                    //ixfileHandle->addread();
                }
                else
                {
                    metaFile->readPage(currentPage+1-primaryPage, data);
                    //ixfileHandle->addread();
                }
            }
            return 0;
        }
        //range search
        else{
            //without highKey
            if (highKey == NULL) {
                int low;
                memcpy(&low, lowKey, sizeof(int));
                if (lowKeyInclusive)
                {
                    while (targetData < low) {
                        startPosition += 1;
                        if (startPosition+1>slotNumber) {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                metaFile->readPage(currentPage+1-primaryPage, data);
                                //ixfileHandle->addread();
                            }
                            startPosition = 0;
                        }
                        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                        while(slotNumber==0)
                        {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                metaFile->readPage(currentPage+1-primaryPage, data);
                                //ixfileHandle->addread();
                            }
                            startPosition = 0;
                            memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                        }
                        memcpy(&targetData, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
                        }
                    memcpy(&key, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
                    memcpy(&rid.pageNum, (char*)data+startPosition*INTREAL_SLOT+sizeof(int), sizeof(int));
                    memcpy(&rid.slotNum, (char*)data+startPosition*INTREAL_SLOT+2*sizeof(int), sizeof(int));
                    startPosition += 1;
                    if (startPosition+1>slotNumber) {
                        currentPage += 1;
                        startPosition = 0;
                        if (currentPage+1 <= primaryPage) {
                            primeFile->readPage(currentPage, data);
                            //ixfileHandle->addread();
                        }
                        else
                        {
                            metaFile->readPage(currentPage+1-primaryPage, data);
                            //ixfileHandle->addread();
                        }
                    }
                    return 0;
                }
                else
                {
                    while (targetData<=low) {
                        startPosition += 1;
                        if (startPosition+1>slotNumber) {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                //ixfileHandle->addread();
                                metaFile->readPage(currentPage+1-primaryPage, data);
                            }
                            startPosition = 0;
                        }
                        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                        while(slotNumber==0)
                        {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                metaFile->readPage(currentPage+1-primaryPage, data);
                                //ixfileHandle->addread();
                            }
                            startPosition = 0;
                            memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                        }
                        memcpy(&targetData, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
                    }
                    memcpy(&rid.pageNum, (char*)data+startPosition*INTREAL_SLOT+sizeof(int), sizeof(int));
                    memcpy(&rid.slotNum, (char*)data+startPosition*INTREAL_SLOT+2*sizeof(int), sizeof(int));
                    startPosition += 1;
                    if (startPosition+1>slotNumber) {
                        currentPage += 1;
                        if (currentPage+1 > totalPage) {
                            return EOF;
                        }
                        if (currentPage+1 <= primaryPage) {
                            primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                        }
                        else
                        {
                            metaFile->readPage(currentPage+1-primaryPage, data);
                                //ixfileHandle->addread();
                        }
                        startPosition = 0;
                    //    return rangeMatch(rid, key);
                    }
                    return 0;
                }
            }
            if (lowKey==NULL) {
                int high;
                memcpy(&high, highKey, sizeof(int));
                //cout<<"high:"<<high<<endl;
                if (highkeyInclusive)
                {
                    while (targetData > high) {
                        startPosition += 1;
                        if (startPosition+1>slotNumber) {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                metaFile->readPage(currentPage+1-primaryPage, data);
                                //ixfileHandle->addread();
                            }
                            startPosition = 0;
                            //return rangeMatch(rid, key);
                        }
                        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                        while(slotNumber==0)
                        {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                metaFile->readPage(currentPage+1-primaryPage, data);
                                //ixfileHandle->addread();
                            }
                            startPosition = 0;
                            memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                        }
                        memcpy(&targetData, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
                    }
                    memcpy(&key, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
                    memcpy(&rid.pageNum, (char*)data+startPosition*INTREAL_SLOT+sizeof(int), sizeof(int));
                    memcpy(&rid.slotNum, (char*)data+startPosition*INTREAL_SLOT+2*sizeof(int), sizeof(int));
                    memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                    startPosition += 1;
                    if (startPosition+1>slotNumber) {
                        currentPage += 1;
                        startPosition = 0;
                        if (currentPage+1 <= primaryPage) {
                            primeFile->readPage(currentPage, data);
                            //ixfileHandle->addread();
                        }
                        else
                        {
                            metaFile->readPage(currentPage+1-primaryPage, data);
                            //ixfileHandle->addread();
                        }
                    }
                    return 0;
                }
                else
                {
                    while (targetData>=high) {
                        startPosition += 1;
                        if (startPosition+1>slotNumber) {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                //ixfileHandle->addread();
                                metaFile->readPage(currentPage+1-primaryPage, data);
                            }
                            startPosition = 0;
                            return rangeMatch(rid, key);
                        }
                        memcpy(&targetData, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
                    }
                    memcpy(&rid.pageNum, (char*)data+startPosition*INTREAL_SLOT+sizeof(int), sizeof(int));
                    memcpy(&rid.slotNum, (char*)data+startPosition*INTREAL_SLOT+2*sizeof(int), sizeof(int));
                    startPosition += 1;
                    if (startPosition+1>slotNumber) {
                        currentPage += 1;
                        if (currentPage+1 > totalPage) {
                            return EOF;
                        }
                        if (currentPage+1 <= primaryPage) {
                            primeFile->readPage(currentPage, data);
                            //ixfileHandle->addread();
                        }
                        else
                        {
                            metaFile->readPage(currentPage+1-primaryPage, data);
                            //ixfileHandle->addread();
                        }
                        startPosition = 0;
                        //    return rangeMatch(rid, key);
                    }
                    return 0;
                }

            }
            else{
            }
        }
    }
    if (attribute.type==1) {
        //check if it comes to end
        if (currentPage +1> totalPage) {
            return EOF;
        }
        



        //check if there are data in the slot
        int slotNumber;
        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
        cout<<"slotNumber:"<<slotNumber<<endl;
        if (slotNumber==0) {
            currentPage += 1;
            if (currentPage +1> totalPage) {
                return EOF;
            }
            if (currentPage+1 <= primaryPage) {
                primeFile->readPage(currentPage, data);
                //ixfileHandle->addread();
            }
            else
            {
                metaFile->readPage(currentPage+1-primaryPage, data);
                //ixfileHandle->addread();
            }
            startPosition = 0;
            return rangeMatch(rid, key);
        }
        
        float targetData;
        memcpy(&targetData, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
        
        //without any limitation
        if (highKey==NULL and lowKey==NULL) {
            memcpy(key, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
            memcpy(&rid.pageNum, (char*)data+startPosition*INTREAL_SLOT+sizeof(int), sizeof(int));
            memcpy(&rid.slotNum, (char*)data+startPosition*INTREAL_SLOT+2*sizeof(int), sizeof(int));
            startPosition += 1;
            if (startPosition+1>slotNumber) {
                currentPage += 1;
                startPosition = 0;
                if (currentPage+1 <= primaryPage) {
                    primeFile->readPage(currentPage, data);
                    //ixfileHandle->addread();
                }
                else
                {
                    metaFile->readPage(currentPage+1-primaryPage, data);
                    //ixfileHandle->addread();
                }
            }
            return 0;
        }
        //range search
        else{
            //without highKey
            if (highKey == NULL) {
                float low;
                memcpy(&low, lowKey, sizeof(int));
                //cout<<"low:"<<low<<endl;
                if (lowKeyInclusive)
                {
                    while (targetData < low) {
                        startPosition += 1;
                        if (startPosition+1>slotNumber) {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                metaFile->readPage(currentPage+1-primaryPage, data);
                                //ixfileHandle->addread();
                            }
                            startPosition = 0;
                        }
                        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                        while(slotNumber==0)
                        {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                metaFile->readPage(currentPage+1-primaryPage, data);
                                //ixfileHandle->addread();
                            }
                            startPosition = 0;
                            memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                        }
                        memcpy(&targetData, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
                    }
                    memcpy(&key, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
                    memcpy(&rid.pageNum, (char*)data+startPosition*INTREAL_SLOT+sizeof(int), sizeof(int));
                    memcpy(&rid.slotNum, (char*)data+startPosition*INTREAL_SLOT+2*sizeof(int), sizeof(int));
                    startPosition += 1;
                    if (startPosition+1>slotNumber) {
                        currentPage += 1;
                        startPosition = 0;
                        if (currentPage+1 <= primaryPage) {
                            primeFile->readPage(currentPage, data);
                            //ixfileHandle->addread();
                        }
                        else
                        {
                            metaFile->readPage(currentPage+1-primaryPage, data);
                            //ixfileHandle->addread();
                        }
                    }
                    return 0;
                }
                else
                {
                    while (targetData<=low) {
                        startPosition += 1;
                        if (startPosition+1>slotNumber) {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                //ixfileHandle->addread();
                                metaFile->readPage(currentPage+1-primaryPage, data);
                            }
                            startPosition = 0;
                        }
                        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                        while(slotNumber==0)
                        {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                metaFile->readPage(currentPage+1-primaryPage, data);
                                //ixfileHandle->addread();
                            }
                            startPosition = 0;
                            memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                        }
                        memcpy(&targetData, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
                    }
                    memcpy(&rid.pageNum, (char*)data+startPosition*INTREAL_SLOT+sizeof(int), sizeof(int));
                    memcpy(&rid.slotNum, (char*)data+startPosition*INTREAL_SLOT+2*sizeof(int), sizeof(int));
                    startPosition += 1;
                    if (startPosition+1>slotNumber) {
                        currentPage += 1;
                        if (currentPage+1 > totalPage) {
                            return EOF;
                        }
                        if (currentPage+1 <= primaryPage) {
                            primeFile->readPage(currentPage, data);
                            //ixfileHandle->addread();
                        }
                        else
                        {
                            metaFile->readPage(currentPage+1-primaryPage, data);
                            //ixfileHandle->addread();
                        }
                        startPosition = 0;
                        //    return rangeMatch(rid, key);
                    }
                    return 0;
                }
            }
            if (lowKey==NULL) {
                float high;
                memcpy(&high, highKey, sizeof(int));
                //cout<<"high:"<<high<<endl;
                if (highkeyInclusive)
                {
                    while (targetData > high) {
                        startPosition += 1;
                        if (startPosition+1>slotNumber) {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                metaFile->readPage(currentPage+1-primaryPage, data);
                                //ixfileHandle->addread();
                            }
                            startPosition = 0;
                            //return rangeMatch(rid, key);
                        }
                        memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                        while(slotNumber==0)
                        {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                metaFile->readPage(currentPage+1-primaryPage, data);
                                //ixfileHandle->addread();
                            }
                            startPosition = 0;
                            memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                        }
                        memcpy(&targetData, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
                    }
                    memcpy(&key, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
                    memcpy(&rid.pageNum, (char*)data+startPosition*INTREAL_SLOT+sizeof(int), sizeof(int));
                    memcpy(&rid.slotNum, (char*)data+startPosition*INTREAL_SLOT+2*sizeof(int), sizeof(int));
                    memcpy(&slotNumber, (char*)data+PAGE_SIZE-2*sizeof(int), sizeof(int));
                    startPosition += 1;
                    if (startPosition+1>slotNumber) {
                        currentPage += 1;
                        startPosition = 0;
                        if (currentPage+1 <= primaryPage) {
                            primeFile->readPage(currentPage, data);
                            //ixfileHandle->addread();
                        }
                        else
                        {
                            metaFile->readPage(currentPage+1-primaryPage, data);
                            //ixfileHandle->addread();
                        }
                    }
                    return 0;
                }
                else
                {
                    while (targetData>=high) {
                        startPosition += 1;
                        if (startPosition+1>slotNumber) {
                            currentPage += 1;
                            if (currentPage+1 > totalPage) {
                                return EOF;
                            }
                            if (currentPage+1 <= primaryPage) {
                                primeFile->readPage(currentPage, data);
                                //ixfileHandle->addread();
                            }
                            else
                            {
                                //ixfileHandle->addread();
                                metaFile->readPage(currentPage+1-primaryPage, data);
                            }
                            startPosition = 0;
                            return rangeMatch(rid, key);
                        }
                        memcpy(&targetData, (char*)data+startPosition*INTREAL_SLOT, sizeof(int));
                    }
                    memcpy(&rid.pageNum, (char*)data+startPosition*INTREAL_SLOT+sizeof(int), sizeof(int));
                    memcpy(&rid.slotNum, (char*)data+startPosition*INTREAL_SLOT+2*sizeof(int), sizeof(int));
                    startPosition += 1;
                    if (startPosition+1>slotNumber) {
                        currentPage += 1;
                        if (currentPage+1 > totalPage) {
                            return EOF;
                        }
                        if (currentPage+1 <= primaryPage) {
                            primeFile->readPage(currentPage, data);
                            //ixfileHandle->addread();
                        }
                        else
                        {
                            metaFile->readPage(currentPage+1-primaryPage, data);
                            //ixfileHandle->addread();
                        }
                        startPosition = 0;
                        //    return rangeMatch(rid, key);
                    }
                    return 0;
                }
            }
            else{
            }
        }
    }
	return -1;
}

RC IX_ScanIterator::close() {
    free(data);
	return 0;
}

void IX_PrintError (RC rc)
{
    switch(rc)
    {
        case SUCCESS:
            return;
            
        case SCAN_EOF:
            fprintf(stderr, "ERROR CODE: %d, ERROR MESSAGE: %s\n", rc, "Index scan out of range.\n");
            return;
            
        case CREATE_FILE_ERROR:
            fprintf(stderr, "ERROR CODE: %d, ERROR MESSAGE: %s\n", rc, "Cannot create this specific file.\n");
            return;
            
        case DESTROY_FILE_ERROR:
            fprintf(stderr, "ERROR CODE: %d, ERROR MESSAGE: %s\n", rc, "Cannot destroy this specific file.\n");
            return;
            
        case OPEN_FILE_ERROR:
            fprintf(stderr, "ERROR CODE: %d, ERROR MESSAGE: %s\n", rc, "Cannot open this specific file.\n");
            return;
            
        case CLOSE_FILE_ERROR:
            fprintf(stderr, "ERROR CODE: %d, ERROR MESSAGE: %s\n", rc, "Cannot close this specific file.\n");
            return;
            
        case ENTRY_NOT_EXIST:
            fprintf(stderr, "ERROR CODE: %d, ERROR MESSAGE: %s\n", rc, "Not such entry in the file.\n");
            return;
        case GET_ATTRIBUTES_ERROR:
            fprintf(stderr, "ERROR CODE: %d, ERROR MESSAGE: %s\n", rc,
                    "Cannot get attributes.\n");
            return;
        case WRONG_ATTRIBUTENAME:
            fprintf(stderr, "ERROR CODE: %d, ERROR MESSAGE: %s\n", rc,
                    "Not such attribute in the table.\n");
            return;
    }
}

IXFileHandle::IXFileHandle() {

}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::passFileHandle(FileHandle &FileHandle1,
		FileHandle &FileHandle2) {
	fh1 = FileHandle1;
	fh2 = FileHandle2;
	return 0;
}

RC IXFileHandle::fetchFileHandle(FileHandle &FileHandle1,
		FileHandle &FileHandle2) {
	FileHandle1 = fh1;
	FileHandle2 = fh2;
	return 0;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount,
		unsigned &writePageCount, unsigned &appendPageCount) {
    unsigned primaryReadPageCount, primaryWritePageCount, primaryAppendPageCount;
    fh2.collectCounterValues(primaryReadPageCount, primaryWritePageCount, primaryAppendPageCount);
    unsigned overflowReadPageCount, overflowWritePageCount, overflowAppendPageCount;
    fh1.collectCounterValues(overflowReadPageCount, overflowWritePageCount, overflowAppendPageCount);
    readPageCount = primaryReadPageCount + overflowReadPageCount;
    writePageCount = primaryWritePageCount + overflowWritePageCount;
    appendPageCount = primaryAppendPageCount + overflowAppendPageCount;
    return 0;
}

RC IXFileHandle::setLevel(int newLevel) {
	level = newLevel;
	return 0;
}
RC IXFileHandle::setNext(int newNext) {
	next = newNext;
	return 0;
}
RC IXFileHandle::setBucket(int newBucket) {
	bucket = newBucket;
	return 0;
}

RC IXFileHandle::setPrimPageNumber(int newNumberOfPrimPages) {
	NumberOfPrimPages = newNumberOfPrimPages;
	return 0;
}
RC IXFileHandle::setMetaPageNumber(int newNumberOfMetaPages) {
	NumberOfMetaPages = newNumberOfMetaPages;
	return 0;
}
void IXFileHandle::addRead() {
	readPageCounter++;
}
void IXFileHandle::addWrite() {
	writePageCounter++;
}
void IXFileHandle::addAppend() {
	appendPageCounter++;
}
FileHandle &IXFileHandle::getMetaFile() {
	return fh1;
}
FileHandle &IXFileHandle::getPrimFile() {
	return fh2;
}

RC IXFileHandle::getLevel() {
	return level;
}
RC IXFileHandle::getBucket() {
	return bucket;
}
RC IXFileHandle::getNext() {
	return next;
}
RC IXFileHandle::getPrimPageNumber() {
	return NumberOfPrimPages;
}
RC IXFileHandle::getMetaPageNumber() {
	return NumberOfMetaPages;
}

