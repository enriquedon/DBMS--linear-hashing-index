#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <map>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator {
public:
	RM_ScanIterator() {
	}
	;
	~RM_ScanIterator() {
	}
	;

	// "data" follows the same format as RelationManager::insertTuple()
	RC getNextTuple(RID &rid, void *data) {
		return rbfmscaner.getNextRecord(rid, data);
	}
	;
	RC close() {
		return rbfmscaner.close();
	}
	RBFM_ScanIterator rbfmscaner;
};

class RM_IndexScanIterator {
public:
	RM_IndexScanIterator() {
	}
	;  	// Constructor
	~RM_IndexScanIterator() {
	}
	; 	// Destructor

	// "key" follows the same format as in IndexManager::insertEntry()
	RC getNextEntry(RID &rid, void *key) {
		cout<<"key:"<<*(float*)key<<endl;
		return rmindexscaner.getNextEntry(rid, key);
	}
	;  	// Get next matching entry
	RC close() {
		return rmindexscaner.close();
	}
	;
	// Terminate index scan
	IX_ScanIterator rmindexscaner;
};

// Relation Manager
class RelationManager {
public:
	static RelationManager* instance();
	//static IndexManager* instance();

	RC createTable(const string &tableName, const vector<Attribute> &attrs);

	RC deleteTable(const string &tableName);

	RC getAttributes(const string &tableName, vector<Attribute> &attrs);

	RC insertTuple(const string &tableName, const void *data, RID &rid);

	RC deleteTuples(const string &tableName);

	RC deleteTuple(const string &tableName, const RID &rid);

	// Assume the rid does not change after update
	RC updateTuple(const string &tableName, const void *data, const RID &rid);

	RC readTuple(const string &tableName, const RID &rid, void *data);

	RC readAttribute(const string &tableName, const RID &rid,
			const string &attributeName, void *data);

	RC reorganizePage(const string &tableName, const unsigned pageNumber);

	// scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(const string &tableName, const string &conditionAttribute,
			const CompOp compOp,         // comparision type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RM_ScanIterator &rm_ScanIterator);

// Extra credit
public:
	RC dropAttribute(const string &tableName, const string &attributeName);

	RC addAttribute(const string &tableName, const Attribute &attr);

	RC reorganizeTable(const string &tableName);

	RC createIndex(const string &tableName, const string &attributeName);

	RC destroyIndex(const string &tableName, const string &attributeName);

	// indexScan returns an iterator to allow the caller to go through qualified entries in index
	RC indexScan(const string &tableName, const string &attributeName,
			const void *lowKey, const void *highKey, bool lowKeyInclusive,
			bool highKeyInclusive, RM_IndexScanIterator &rm_IndexScanIterator);

protected:
	RelationManager();
	~RelationManager();

private:
	map<string, vector<Attribute> > attrMap;
	static RelationManager *_rm;
	IndexManager *ix;
	RecordBasedFileManager *rbfm;
	PagedFileManager *pfm; 
	string catalog;
	string column;
	string IndexCatalog;
	vector<Attribute> catalog_v;
	vector<Attribute> column_v;
	vector<Attribute> IndexTable_v;
	void getMaxId(FileHandle &fileHandle, unsigned &MaxId);
	unsigned int MaxTableId;
	unsigned int MaxIndexTableId;
	RC UpdateCatalogTable(FileHandle &fileHandle, const void *data,
			const string tableName, unsigned &MaxId);
	RC UpdateColumnTable(FileHandle &fileHandle, const vector<Attribute> &attrs,
			unsigned &MaxId, vector<Attribute> column_vector);
	RC clearTableInColumn(void * TableId);
	RC clearTableInIndex(const string &tableName);
	RC getAllIndex(const string &tableName, map<string, string> &index);
	RC updateOffsetForAttrs(Attribute attr, unsigned &offset, const void *data);
	RC DeleteInsertIndexes(vector<Attribute> attrs, map<string, string> index,
			const string &tableName, const void *data, const RID &rid, bool DI);
	RC DelInsIndex(Attribute attr, string indexFileName, unsigned &offset,
			const void *data, const RID rid, bool DI);
	RC MakeIndex(const string &tableName,const string &indexTableName);
};

#endif
