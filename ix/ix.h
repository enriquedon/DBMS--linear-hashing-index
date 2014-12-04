#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <map>
#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
#define INTREAL_SLOT sizeof(int)*3
#define INTREAL_NUMBER (PAGE_SIZE-2*sizeof(int))/(3*sizeof(int))

class IX_ScanIterator;
class IXFileHandle;

typedef enum 
{
  SUCCESS = 0,
  SCAN_EOF = -1,
  CREATE_FILE_ERROR = -10,
  DESTROY_FILE_ERROR = -11,
  OPEN_FILE_ERROR = -12,
  CLOSE_FILE_ERROR = -13,
  ENTRY_NOT_EXIST = -20,
  GET_ATTRIBUTES_ERROR = -21,
  WRONG_ATTRIBUTENAME = -22
} IXErrorCode;

class IndexManager {
public:
	static IndexManager* instance();

	// Create index file(s) to manage an index
	RC createFile(const string &fileName, const unsigned &numberOfPages);

	// Delete index file(s)
	RC destroyFile(const string &fileName);

	// Open an index and returns an IXFileHandle
	RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

	// Close an IXFileHandle.
	RC closeFile(IXFileHandle &ixfileHandle);

	// The following functions  are using the following format for the passed key value.
	//  1) data is a concatenation of values of the attributes
	//  2) For INT and REAL: use 4 bytes to store the value;
	//     For VarChar: use 4 bytes to store the length of characters, then store the actual characters.

	// Insert an entry to the given index that is indicated by the given IXFileHandle
	RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
			const void *key, const RID &rid);

	// Delete an entry from the given index that is indicated by the given IXFileHandle
	RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
			const void *key, const RID &rid);

	// scan() returns an iterator to allow the caller to go through the results
	// one by one in the range(lowKey, highKey).
	// For the format of "lowKey" and "highKey", please see insertEntry()
	// If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
	// should be included in the scan
	// If lowKey is null, then the range is -infinity to highKey
	// If highKey is null, then the range is lowKey to +infinity

	// Initialize and IX_ScanIterator to supports a range search
	RC scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
			const void *lowKey, const void *highKey, bool lowKeyInclusive,
			bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator);

	// Generate and return the hash value (unsigned) for the given key
	unsigned hash(const Attribute &attribute, const void *key);

	// Print all index entries in a primary page including associated overflow pages
	// Format should be:
	// Number of total entries in the page (+ overflow pages) : ??
	// primary Page No.??
	// # of entries : ??
	// entries: [xx] [xx] [xx] [xx] [xx] [xx]
	// overflow Page No.?? liked to [primary | overflow] page No.??
	// # of entries : ??
	// entries: [xx] [xx] [xx] [xx] [xx]
	// where [xx] shows each entry.
	RC printIndexEntriesInAPage(IXFileHandle &ixfileHandle,
			const Attribute &attribute, const unsigned &primaryPageNumber);

	// Get the number of primary pages
	RC getNumberOfPrimaryPages(IXFileHandle &ixfileHandle,
			unsigned &numberOfPrimaryPages);

	// Get the number of all pages (primary + overflow)
	RC getNumberOfAllPages(IXFileHandle &ixfileHandle,
			unsigned &numberOfAllPages);

	int PreparePrint(FileHandle &fhPrim, FileHandle &fhMeta,
			const Attribute &attribute, const unsigned &primaryPageNumber);
	int printEntriesInPrim(FileHandle &fhPrim, const Attribute &attribute,
			void *page);
	int printEntriesInMeta(FileHandle &fhMeta, const Attribute &attribute,
			int &overflowPageNo);
	//int IntRealPrint(FileHandle &fhPrim, void *page);
	int IntRealPrint(void *page);
	int VarCharPrint(void *page);
protected:
	IndexManager();                            // Constructor
	~IndexManager();                            // Destructor

private:
	static IndexManager *_index_manager;

	//unsigned h0num;
	RC initiateMeta(string metaFile, const unsigned &numberOfPages);
	RC initiatePrim(string primFile, const unsigned &numberOfPages);
	RC getLevelNext(IXFileHandle &ixfileHandle);
	map<string, vector<int> > attributeTable;   //level, next, hashBits

	int findInsertPosition(const void *key, const Attribute attribute,
			vector<void *> &page, int totalNumber);
	int findDeletePosition(int start, const void *key, const RID &rid,
			const Attribute attribute, vector<void *> &page, int totalNumber);
	int insertToPage(int position, vector<int> &pageNumbers, const void *key,
			const Attribute attribute, vector<void *> &page, const RID &rid);
	int writeToPages(IXFileHandle &ixfileHandle, FileHandle &metaFile,
			FileHandle &primFile, int startPage, vector<void*> &page,
			vector<int> &pagePosition, int nextPageNumber);
	int deleteFromPages(int position, vector<int> &pageNumbers, const void *key,
			const Attribute attribute, vector<void *> &page, const RID &rid);
    string getVarcharValue(void* data, int offset);
    int getVarcharSlot(void *slotData, void* data, int offset);
    bool checkVarcharFull(int datasize, int length, void *data);
    int getVarcharOffset(void* data, int offset);
    void moveVarChardir(int start, int end, int slotlength, void *data);
    void insertTailTopage(void *tailData,vector<int> &tailLengths,int taillength,int currentPage,vector<void*> &page,vector<int> &pageNumbers);
    int getVarcharTailData(void *pagedata, int length, vector<int> &tailLengths, void* tailData);



	RC split(const Attribute &attribute, string attributeName,
			FileHandle &metaFileHandle, FileHandle &primaryFileHandle);

	RC reinsertData(const Attribute &attribute, vector<void *> &oldPages,
			FileHandle &primaryFileHandle, FileHandle &metaFileHandle,vector<int> usedOFnumber);
	RC splitGetEntry(void *entry, const Attribute &attribute, void *oldPage,
			int i);
	RC splitInsertEntry(const Attribute &attribute, void *entry, void *oldPage,
			vector<void *> &newPages1, vector<void *> &newPages2);
	RC splitInsertOneIntReal(const Attribute &attribute, void *entry,
			vector<void *> &newPages1, vector<void *> &newPages2);
	RC splitInsertVarChar(const Attribute &attribute, void *entry,
			 vector<void *> &newPages1, vector<void *> &newPages2);
	unsigned remainsForVarchar(void* page, unsigned &freePostion,
			unsigned &Entries);
	RC writeRehashedPages(const Attribute &attribute, FileHandle &primaryFileHandle, FileHandle &metaFileHandle, vector<void *> &newPages1,
			vector<void *> &newPages2,vector<int> usedOFnumber);
	RC writeOverflow(FileHandle &metaFileHandle,vector<void*> &newPages2,int &numOfOverflowPages,vector<int> &usedOFnumber,int &used);

	RC updateMetaAfterSplit(string attributeName, int level, int next);
	RC merge(const Attribute &attribute, string attributeName,
			FileHandle &primaryFileHandle, FileHandle &metaFileHandle);
	RC prepareMerge(FileHandle &metaFileHandle, vector<void *> &oldPages,
			void *pagePrim,vector<int> &usedOFnumber);
	RC sortMerge(const Attribute attribute, vector<void *> &oldPages1,
			vector<void *> &oldPages2, vector<void *> &newpages);
	RC mergeGetEntry(void *entry1, const Attribute &attribute, void* oldPage1,
			int i);
	RC mergeCompareEntry(const Attribute &attribute, void *entry1,
			void *entry2);
	RC mergeInsertWinner(const Attribute &attribute, void * entry,
			vector<void*> &newPages);
	RC mergeInsertOneIntReal(const Attribute &attribute, void * entry,
			vector<void*> &newPages);
	RC mergeInsertVarChar(const Attribute &attribute, void * entry,
			vector<void*> &newPages);
	RC appendRemainPages(const Attribute attribute, vector<void *> &oldPages, vector<void *> &newpages,int j,int size);
	RC writeMergedPages(vector<void *> &newPages, FileHandle &primaryFileHandle, FileHandle &metaFileHandle , int hashvalue1,vector<int> &usedOFnumber);
	RC updateMetaAfterMerge(string attributeName, int level, int next);

	RC freeVector(vector<void *> &newPages);

};

class IX_ScanIterator {
public:
	IX_ScanIterator();  							// Constructor
	~IX_ScanIterator(); 							// Destructor
    void init(IXFileHandle &ixfileHandle, const Attribute &attribute,
              const void *lowKey, const void *highKey, bool lowKeyInclusive,
              bool highKeyInclusive);

	RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
    
    RC close();             						// Terminate index scan
private:
    RC exactMatch(RID &rid, void *key);
    RC rangeMatch(RID &rid, void *key);
    IXFileHandle *ixfileHandle;
    FileHandle metaFile;
    FileHandle primeFile;
    Attribute attribute;
    int keySize;
    void *lowKey;
    void *highKey;
    bool lowKeyInclusive;
    bool highkeyInclusive;
    int startPosition;
    void *data;
    int currentPage;
    int primaryPage;
    int totalPage;
};

class IXFileHandle {
public:
	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
			unsigned &appendPageCount);
	RC passFileHandle(FileHandle &FileHandle1, FileHandle &FileHandle2);
	RC fetchFileHandle(FileHandle &FileHandle1, FileHandle &FileHandle2);

    FileHandle &getMetaFile();
    FileHandle &getPrimFile();
	RC setLevel(int level);
	RC setNext(int next);
	RC setBucket(int bucket);
	RC setPrimPageNumber(int NumberOfPrimPages);
	RC setMetaPageNumber(int NumberOfMetaPages);

	RC getLevel();
	RC getNext();
	RC getBucket();
	RC getPrimPageNumber();
	RC getMetaPageNumber();

	void addRead();
	void addWrite();
	void addAppend();

	IXFileHandle();  							// Constructor
	~IXFileHandle(); 							// Destructor

private:
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;

	FileHandle fh1;
	FileHandle fh2;
	int level;
	int next;
	int bucket;
	int NumberOfPrimPages;
	int NumberOfMetaPages;
};

// print out the error message for a given return code
void IX_PrintError(RC rc);

#endif

