#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>

#include "pfm.h"
//#include "../rbf/pfm.h"

using namespace std;


// Record ID
typedef struct
{
    unsigned pageNum;
    unsigned slotNum;
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal = 1, TypeVarChar = 2 } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0,  // =
    LT_OP,      // <
    GT_OP,      // >
    LE_OP,      // <=
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP      // no condition
} CompOp;



/****************************************************************************
 The scan iterator is NOT required to be implemented for part 1 of the project
 *****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();


class RBFM_ScanIterator {
public:
    RBFM_ScanIterator() {};
    ~RBFM_ScanIterator() {};
    void init(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames);
    
    
    // "data" follows the same format as RecordBasedFileManager::insertRecord()
    RC getNextRecord(RID &rid, void *data);
    bool checkValue(void *attrValue);
    RC selectAttrFromRecord(void *data, void* record);
    RC close()
    {
        PagedFileManager *pfm=PagedFileManager::instance();
        pfm->closeFile(fileHandle);
        //if(compOp!=NO_OP)
         //   free(value);
        return 0;
    };
    RC setRid(int pageNum, int slotNum);
private:
    RID currRid;
    FileHandle fileHandle;
    vector <Attribute> recordDescriptor;
    vector <string> attributeNames;
    const void *value;
    string conditionAttribute;
    CompOp compOp;
    int attrType;
};


class RecordBasedFileManager
{
public:
    static RecordBasedFileManager* instance();
    
    RC createFile(const string &fileName);
    
    RC destroyFile(const string &fileName);
    
    RC openFile(const string &fileName, FileHandle &fileHandle);
    
    RC closeFile(FileHandle &fileHandle);
    
    //  Format of the data passed into the function is the following:
    //  1) data is a concatenation of values of the attributes
    //  2) For int and real: use 4 bytes to store the value;
    //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
    RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);
    
    RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
    
    // This method will be mainly used for debugging/testing
    RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);
    
    /**************************************************************************************************************************************************************
     ***************************************************************************************************************************************************************
     IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
     ***************************************************************************************************************************************************************
     ***************************************************************************************************************************************************************/
    RC deleteRecords(FileHandle &fileHandle);
    
    RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);
    
    // Assume the rid does not change after update
    RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);
    
    RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data);
    
    RC reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber);
    
    // scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const vector<Attribute> &recordDescriptor,
            const string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const vector<string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);
    
    
    // Extra credit for part 2 of the project, please ignore for part 1 of the project
public:
    
    RC reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor);
    
    
protected:
    RecordBasedFileManager();
    ~RecordBasedFileManager();
    
private:
    static RecordBasedFileManager *_rbf_manager;
    PagedFileManager *pfm;
    unsigned int dataToRecord(const vector<Attribute> &recordDescription, const void *data, void *record);  //convert data to formatted record
    RC newPageInit(FileHandle &fileHandle, unsigned int pagenum);                                             //intial newPage
    unsigned int getValidPage(FileHandle &fileHandle, unsigned int recordLength);                          //choose page according to recordLength
    unsigned int getFreeSpace(FileHandle &fileHandle, unsigned int pageNum);                                 //get free space of pageNum
    unsigned int insertToPage(FileHandle &fileHandle, unsigned int pageNum, void *record, unsigned recordLength); //insert record to specific page
    unsigned int getValidPageForUpdate(FileHandle &fileHandle, unsigned int recordLength,unsigned pageNum);
};

#endif
