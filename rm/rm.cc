#include "rm.h"


RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance() {
	if (!_rm)
		_rm = new RelationManager();

	return _rm;
}

RelationManager::RelationManager() {

	pfm = PagedFileManager::instance();
	rbfm = RecordBasedFileManager::instance();
	ix = IndexManager::instance();
	MaxTableId = 0;
	MaxIndexTableId = 0;
	catalog = "Tables";
	column = "Columns";
	IndexCatalog = "IndexTables";

	Attribute attr;
	attr.name = "TableId";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	catalog_v.push_back(attr);

	attr.name = "TableName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 30;
	catalog_v.push_back(attr);

	attr.name = "FileName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 30;
	catalog_v.push_back(attr);

	attr.name = "TableId";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	column_v.push_back(attr);

	attr.name = "ColumnName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 30;
	column_v.push_back(attr);

	attr.name = "ColumnType";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	column_v.push_back(attr);

	attr.name = "ColumnLength";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	column_v.push_back(attr);

	attr.name = "Position";
	attr.type = TypeInt;
	attr.length = (AttrLength) 4;
	column_v.push_back(attr);

	attr.name = "indexFileName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 60;
	IndexTable_v.push_back(attr);

	attr.name = "attributeName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 30;
	IndexTable_v.push_back(attr);

	attr.name = "TableName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength) 30;
	IndexTable_v.push_back(attr);

}

RelationManager::~RelationManager() {
}

void RelationManager::getMaxId(FileHandle &fileHandle, unsigned &MaxId) {
	unsigned int pageNum = fileHandle.getNumberOfPages();
	MaxId = 0;
	if (pageNum != 0) {
		for (unsigned i = 0; i < pageNum; i++) {
			void* Page = malloc(PAGE_SIZE);
			//fileHandle.readPage(pageNum - 1, Page);
			fileHandle.readPage(i, Page);
			int tempNum;
			memcpy(&tempNum, (char*) Page + PAGE_SIZE - 2 * sizeof(int),
					sizeof(int));
			MaxId += tempNum;
			free(Page);
		}
	}
}

RC RelationManager::UpdateCatalogTable(FileHandle &fileHandle,
		const void *CatalogPage, const string tableName, unsigned &MaxId) {

	RID rid;
	getMaxId(fileHandle, MaxId);
	unsigned int offset = 0;
	memcpy((char*) CatalogPage, &MaxId, sizeof(int));
	offset += sizeof(int);

	unsigned int tableNameLength = (unsigned int) tableName.length();
	memcpy((char*) CatalogPage + offset, &tableNameLength, sizeof(int));
	offset += sizeof(int);

	memcpy((char*) CatalogPage + offset, tableName.c_str(),
			sizeof(char) * tableNameLength);
	offset += tableNameLength;

	unsigned int fileNameLength = (unsigned int) tableName.length();
	memcpy((char*) CatalogPage + offset, &fileNameLength, sizeof(int));
	offset += sizeof(int);

	memcpy((char*) CatalogPage + offset, tableName.c_str(),
			sizeof(char) * fileNameLength);
	offset += tableNameLength;

	rbfm->insertRecord(fileHandle, catalog_v, CatalogPage, rid);

	return 0;
}

RC RelationManager::UpdateColumnTable(FileHandle &fileHandle,
		const vector<Attribute> &attrs, unsigned &MaxId,
		vector<Attribute> column_vector) {
	RID rid;
	for (int i = 0; i < (int) attrs.size(); i++) {
		unsigned int offset = 0;
		void *ColumnPage = malloc(PAGE_SIZE);
		memcpy((char*) ColumnPage, &MaxId, sizeof(int));
		offset += sizeof(int);

		unsigned int length = (unsigned int) attrs[i].name.length();
		memcpy((char *) ColumnPage + offset, &length, sizeof(int));
		offset += sizeof(int);

		memcpy((char *) ColumnPage + offset, attrs[i].name.c_str(), length);
		offset += length;

		memcpy((char *) ColumnPage + offset, &attrs[i].type, sizeof(int));
		offset += sizeof(int);
		memcpy((char *) ColumnPage + offset, &attrs[i].length, sizeof(int));
		offset += sizeof(int);

		memcpy((char *) ColumnPage + offset, &i, sizeof(int));
		rbfm->insertRecord(fileHandle, column_vector, ColumnPage, rid);
		free(ColumnPage);

	}

	return 0;
}

RC RelationManager::createTable(const string &tableName,
		const vector<Attribute> &attrs) {

	RBFM_ScanIterator rbfm_ScanIterator;

	FileHandle fileHandle;
	int rc = rbfm->createFile(catalog);
	rbfm->openFile(catalog, fileHandle);
	if (rc == 0) { //initialize Tables and Columns
		void *tmp = malloc(111);
		UpdateCatalogTable(fileHandle, tmp, catalog, MaxTableId);
		rbfm->createFile(column);
		UpdateCatalogTable(fileHandle, tmp, column, MaxTableId);
		FileHandle fileHandle2;
		rbfm->openFile(column, fileHandle2);
		MaxTableId = 0;
		UpdateColumnTable(fileHandle2, catalog_v, MaxTableId, column_v);
		MaxTableId = 1;
		UpdateColumnTable(fileHandle2, column_v, MaxTableId, column_v);
		rbfm->closeFile(fileHandle2);
		free(tmp);
	}
	void *CatalogPage = malloc(PAGE_SIZE);
	void *tableInfo = malloc(sizeof(int) + tableName.size());
	*(int *) tableInfo = tableName.size();
	memcpy((char *) tableInfo + sizeof(int), tableName.c_str(),
			tableName.size());
	vector<string> attributes;
	attributes.push_back(string("TableName"));
	rbfm->scan(fileHandle, catalog_v, string("TableName"), EQ_OP, tableInfo,
			attributes, rbfm_ScanIterator);

	RID rid;
	if (rbfm_ScanIterator.getNextRecord(rid, CatalogPage) != -1) { //find such tablename
		free(tableInfo);
		free(CatalogPage);
		rbfm_ScanIterator.close();
		return -1;
	}
	UpdateCatalogTable(fileHandle, CatalogPage, tableName, MaxTableId);
	FileHandle fileHandle2;
	rbfm->createFile(column);
	rbfm->openFile(column, fileHandle2);
	UpdateColumnTable(fileHandle2, attrs, MaxTableId, column_v);
	rbfm->createFile(tableName);
	rbfm_ScanIterator.close();
	free(tableInfo);
	free(CatalogPage);
	rbfm->closeFile(fileHandle2);
	attrMap.insert(pair<string, vector<Attribute> >(tableName, attrs));
	return 0;

}

RC RelationManager::deleteTable(const string &tableName) {

	FileHandle fileHandle;
	RID rid;
	RBFM_ScanIterator rbfm_ScanIterator;
	void *CatalogPage = malloc(PAGE_SIZE);
	rbfm->openFile(catalog, fileHandle);
	void *tableInfo = malloc(sizeof(int) + tableName.size());
	*(int *) tableInfo = tableName.size();
	memcpy((char *) tableInfo + sizeof(int), tableName.c_str(),
			tableName.size());
	vector<string> attributes;
	attributes.push_back(string("TableName"));
	rbfm->scan(fileHandle, catalog_v, string("TableName"), EQ_OP, tableInfo,
			attributes, rbfm_ScanIterator);
	if (rbfm_ScanIterator.getNextRecord(rid, CatalogPage) != -1) {
		void * TableId = malloc(sizeof(int));
		rbfm->readAttribute(fileHandle, catalog_v, rid, "TableId", TableId);
		rbfm->deleteRecord(fileHandle, catalog_v, rid);

		clearTableInColumn(TableId);
		clearTableInIndex(tableName);

		attributes.clear();
		free(tableInfo);
		free(CatalogPage);

		rbfm_ScanIterator.close();
		rbfm->destroyFile(tableName.c_str());
		return 0;
	}
	attributes.clear();
	free(tableInfo);
	free(CatalogPage);
	rbfm_ScanIterator.close();
	return -1;
}

RC RelationManager::clearTableInIndex(const string &tableName) {
	RID Irid;
	FileHandle fileHandle;

	if (rbfm->openFile(IndexCatalog, fileHandle) == 0) {
		void *tableInfo = malloc(sizeof(int) + tableName.size());
		*(int *) tableInfo = tableName.size();
		memcpy((char *) tableInfo + sizeof(int), tableName.c_str(),
				tableName.size());
		void *ColumnPage = malloc(PAGE_SIZE);
		RBFM_ScanIterator rbfm_ScanIterator;
		vector<string> attributes;
		attributes.push_back(string("TableName"));
		rbfm->scan(fileHandle, IndexTable_v, string("TableName"), EQ_OP,
				tableInfo, attributes, rbfm_ScanIterator);
		while (rbfm_ScanIterator.getNextRecord(Irid, ColumnPage) != -1) {
			void *record = malloc(PAGE_SIZE);
			rbfm->readAttribute(fileHandle, IndexTable_v, Irid, "IndexFileName",
					record);
			string IndexFileName;
			int length1;
			memcpy(&length1, (char*) record, sizeof(int));
			memcpy(&IndexFileName, (char*) record + sizeof(int), length1);
			destroyIndex(tableName, IndexFileName);
		}
		free(ColumnPage);
		//rbfm->closeFile(fileHandle);
		attributes.clear();
		rbfm_ScanIterator.close();
		return 0;
	}
	return -1;
}

RC RelationManager::clearTableInColumn(void * TableId) {
	RID rid;
	FileHandle fileHandle;

	if (rbfm->openFile(column, fileHandle)) {
		void *ColumnPage = malloc(PAGE_SIZE);
		RBFM_ScanIterator rbfm_ScanIterator;
		vector<string> attributes;
		attributes.push_back(string("TableName"));
		rbfm->scan(fileHandle, column_v, string("TableId"), EQ_OP, TableId,
				attributes, rbfm_ScanIterator);
		while (rbfm_ScanIterator.getNextRecord(rid, ColumnPage) != -1) {
			rbfm->deleteRecord(fileHandle, column_v, rid);
		}
		free(ColumnPage);
		attributes.clear();
		rbfm_ScanIterator.close();
		return 0;
	}
	return -1;
}

RC RelationManager::getAttributes(const string &tableName,
		vector<Attribute> &attrs) {
	if (attrMap.find(tableName) != attrMap.end()) {
		attrs = attrMap.at(tableName);
		return 0;
	}
	FileHandle fileHandle;
	RID rid;
	RBFM_ScanIterator rbfm_ScanIterator;
	void *CatalogPage = malloc(PAGE_SIZE); //hope the name not exceeding that size
	rbfm->openFile(catalog, fileHandle);
	void *tableInfo = malloc(sizeof(int) + tableName.size());
	*(int *) tableInfo = tableName.size();
	memcpy((char *) tableInfo + sizeof(int), tableName.c_str(),
			tableName.size());
	vector<string> selectAttributes;
	selectAttributes.push_back(string("TableId"));
	rbfm->scan(fileHandle, catalog_v, string("TableName"), EQ_OP, tableInfo,
			selectAttributes, rbfm_ScanIterator);
	if (rbfm_ScanIterator.getNextRecord(rid, CatalogPage) != 0) { //does TableName exist?
		free(CatalogPage);
		free(tableInfo);
		rbfm_ScanIterator.close();
		return -1;
	}
	free(CatalogPage);
	free(tableInfo);
	void * TableId = malloc(sizeof(int));
	rbfm->readAttribute(fileHandle, catalog_v, rid, "TableId", TableId);

	FileHandle fileHandle2;
	rbfm->openFile(column, fileHandle2);
	void *ColumnRecord = malloc(PAGE_SIZE);
	RBFM_ScanIterator rbfm_ScanIterator2;
	vector<string> selectColumn;
	selectColumn.push_back(string("ColumnName"));
	selectColumn.push_back(string("ColumnType"));
	selectColumn.push_back(string("ColumnLength"));
	rbfm->scan(fileHandle2, column_v, string("TableId"), EQ_OP, TableId,
			selectColumn, rbfm_ScanIterator2);
	while (rbfm_ScanIterator2.getNextRecord(rid, ColumnRecord) != -1) {
		Attribute column;
		int nameLength;
		int offset = 0;
		memcpy(&nameLength, (char*) ColumnRecord + offset, sizeof(int));
		offset += sizeof(int);
		column.name.assign((char*) ColumnRecord + offset,
				(char*) ColumnRecord + offset + nameLength);
		offset += nameLength;
		memcpy(&column.type, (char*) ColumnRecord + offset, sizeof(int));
		offset += sizeof(int);
		memcpy(&column.length, (char*) ColumnRecord + offset, sizeof(int));
		attrs.push_back(column);
	}

	free(ColumnRecord);
	rbfm_ScanIterator.close();
	rbfm_ScanIterator2.close();
	attrMap.insert(pair<string, vector<Attribute> >(tableName, attrs));
	return 0;
}

RC RelationManager::getAllIndex(const string &tableName,
		map<string, string> &index) {

	FileHandle fileHandle;
	RC success = rbfm->openFile(IndexCatalog, fileHandle);
	if (success == -1) {
		//IX_PrintError(OPEN_FILE_ERROR);
		return -1;
	}
	RID Irid;
	RBFM_ScanIterator rbfm_ScanIterator;
	void *CatalogPage = malloc(PAGE_SIZE);
	//string indexTableName = tableName;
	void *tableInfo = malloc(sizeof(int) + tableName.size());
	*(int *) tableInfo = tableName.size();
	memcpy((char *) tableInfo + sizeof(int), tableName.c_str(),
			tableName.size());
	vector<string> attributes;
	attributes.push_back(string("indexFileName"));
	attributes.push_back(string("attributeName"));
	attributes.push_back(string("TableName"));

	rbfm->scan(fileHandle, IndexTable_v, string("TableName"), EQ_OP, tableInfo,
			attributes, rbfm_ScanIterator);
	RC hasIndex = -1;
	while (rbfm_ScanIterator.getNextRecord(Irid, CatalogPage) != -1) {
		hasIndex = 0;
		//void *record = malloc(PAGE_SIZE);
		//rbfm->readAttribute(fileHandle, IndexTable_v, Irid, "IndexFileName",
		//		record);
		
		
		int offset = 0;
		int length1;
		memcpy(&length1, (char*) CatalogPage + offset, sizeof(int));
		offset += sizeof(int);

		string indexFileName = "";
		indexFileName.assign((char*)CatalogPage+offset,(char*)CatalogPage+offset+length1);
		offset += length1;
		int length2;
		memcpy(&length2, (char*) CatalogPage + offset, sizeof(int));
		offset += sizeof(int);

		string attributeName = "";
		attributeName.assign((char*)CatalogPage+offset,(char*)CatalogPage+offset+length2);
		index.insert(pair<string, string>(attributeName, indexFileName));
	}
	free(tableInfo);
	free(CatalogPage);
	rbfm->closeFile(fileHandle);
	return hasIndex;
}

RC RelationManager::updateOffsetForAttrs(Attribute attr, unsigned &offset,
		const void *data) {
	if (attr.type < 2) {
		offset += sizeof(int);
	} else {
		int length;
		memcpy(&length, (char*) data + offset, sizeof(int));
		offset += sizeof(int);
		offset += length;
	}
	return 0;
}

RC RelationManager::DeleteInsertIndexes(vector<Attribute> attrs,
		map<string, string> index, const string &tableName, const void *data,
		const RID &rid, bool DI) {
	unsigned offset = 0;
	for (unsigned i = 0; i < attrs.size(); i++) {
		map<string, string>::iterator it = index.find(attrs[i].name);
		if ((it != index.end())) {
			string indexFileName = it->second;
			RC success = DelInsIndex(attrs[i], indexFileName, offset, data, rid,
					DI);
			if (success == -1) {
				//IX_PrintError(OPEN_FILE_ERROR);
				return -1;
			}
			updateOffsetForAttrs(attrs[i], offset, data);
		} else {
			updateOffsetForAttrs(attrs[i], offset, data);
		}
	}
	return 0;
}

RC RelationManager::DelInsIndex(Attribute attr, string indexFileName,
		unsigned &offset, const void *data, const RID rid, bool DI) {

	IXFileHandle ixfileHandle;
	if (ix->openFile(indexFileName, ixfileHandle) == 0) {
		if (attr.type < 2) {
			void *key = malloc(sizeof(int));
			memcpy(key, (char*)data + offset, sizeof(int));
			if (DI) {
				ix->insertEntry(ixfileHandle, attr, key, rid);
			} else {
				ix->deleteEntry(ixfileHandle, attr, key, rid);
			}
			free(key);
		} else {
			unsigned tempLength;
			memcpy(&tempLength, (char*)data + offset, sizeof(int));
			void *key = malloc(PAGE_SIZE);
			memcpy(key, (char*)data + offset, sizeof(int) + tempLength);
			if (DI) {
				ix->insertEntry(ixfileHandle, attr, key, rid);
			} else {
				ix->deleteEntry(ixfileHandle, attr, key, rid);
			}
			free(key);
		}
		return 0;
	} else {
		return -1;
	}

}

RC RelationManager::insertTuple(const string &tableName, const void *data,
		RID &rid) {
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) {
		attrs.clear();
		//IX_PrintError(GET_ATTRIBUTES_ERROR);
		return -1;
	}

	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0) {
		attrs.clear();
		//IX_PrintError(OPEN_FILE_ERROR);
		return -1;
	}
	rbfm->insertRecord(fileHandle, attrs, data, rid);

	//vector<string> index;
	//rbfm->openFile(IndexCatalog, fileHandle);
	map<string, string> index;
	if (getAllIndex(tableName, index) == 0) {
		DeleteInsertIndexes(attrs, index, tableName, data, rid, true);
	}

	index.clear();
	attrs.clear();
	rbfm->closeFile(fileHandle);
	return 0;
}

RC RelationManager::deleteTuples(const string &tableName) {
	vector<Attribute> attrs = *(new vector<Attribute>());
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0) {
		attrs.clear();
		//IX_PrintError(OPEN_FILE_ERROR);
		return -1;
	}
	if (getAttributes(tableName, attrs) != 0) {
		attrs.clear();
		//IX_PrintError(GET_ATTRIBUTES_ERROR);
		return -1;
	}
	attrs.clear();
	rbfm->deleteRecords(fileHandle);

	clearTableInIndex(tableName);

	rbfm->closeFile(fileHandle);
	return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
	vector<Attribute> attrs = *(new vector<Attribute>());
	if (getAttributes(tableName, attrs) != 0) {
		attrs.clear();
		//IX_PrintError(GET_ATTRIBUTES_ERROR);
		return -1;
	}
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0) {
		attrs.clear();
		//IX_PrintError(OPEN_FILE_ERROR);
		return -1;
	}

	//
	map<string, string> index;
	if (getAllIndex(tableName, index) == 0) {
		void *data = malloc(PAGE_SIZE);
		rbfm->readRecord(fileHandle, attrs, rid, data);
		DeleteInsertIndexes(attrs, index, tableName, data, rid, false);
		free(data);
	}

	if (rbfm->deleteRecord(fileHandle, attrs, rid) != 0) {
		rbfm->closeFile(fileHandle);
		attrs.clear();
		return -3;
	}
	rbfm->closeFile(fileHandle);
	attrs.clear();
	return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data,
		const RID &rid) {
	vector<Attribute> attrs = *(new vector<Attribute>());
	if (getAttributes(tableName, attrs) != 0) {
		attrs.clear();
		return -1;
	}
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0) {
		attrs.clear();
		return -2;
	}

	map<string, string> index;
	if (getAllIndex(tableName, index) == 0) {
		void *record = malloc(PAGE_SIZE);
		rbfm->readRecord(fileHandle, attrs, rid, record);
		DeleteInsertIndexes(attrs, index, tableName, record, rid, false);
		free(record);
		DeleteInsertIndexes(attrs, index, tableName, data, rid, true);
	}

	if (rbfm->updateRecord(fileHandle, attrs, data, rid) != 0) {
		rbfm->closeFile(fileHandle);
		attrs.clear();
		return -3;
	}
	rbfm->closeFile(fileHandle);
	attrs.clear();
	return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
		void *data) {
	vector<Attribute> attrs;
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0) {
		attrs.clear();
		return -1;
	}
	if (getAttributes(tableName, attrs) != 0) {
		rbfm->closeFile(fileHandle);
		attrs.clear();
		return -2;
	}
	if (rbfm->readRecord(fileHandle, attrs, rid, data) != 0) {
		rbfm->closeFile(fileHandle);
		attrs.clear();
		return -3;
	}
	rbfm->closeFile(fileHandle);
	attrs.clear();
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
		const string &attributeName, void *data) {
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) {
		attrs.clear();
		return -1;
	}
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0) {
		attrs.clear();
		return -1;
	}
	if (rbfm->readAttribute(fileHandle, attrs, rid, attributeName, data) != 0) {
		attrs.clear();
		rbfm->closeFile(fileHandle);
		return -1;
	}
	rbfm->closeFile(fileHandle);
	attrs.clear();
	return 0;
}

RC RelationManager::reorganizePage(const string &tableName,
		const unsigned pageNumber) {
	vector<Attribute> attrs;
	if (getAttributes(tableName, attrs) != 0) {
		attrs.clear();
		return -1;
	}
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != 0) {
		attrs.clear();
		return -1;
	}
	if (rbfm->reorganizePage(fileHandle, attrs, pageNumber) != 0) {
		rbfm->closeFile(fileHandle);
		attrs.clear();
		return -1;
	}
	rbfm->closeFile(fileHandle);
	attrs.clear();
	return 0;
}

RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute, const CompOp compOp,
		const void *value, const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator) {
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	FileHandle fh;
	rbfm->openFile(tableName, fh);
	return rbfm->scan(fh, attrs, conditionAttribute, compOp, value,
			attributeNames, rm_ScanIterator.rbfmscaner);
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName,
		const string &attributeName) {
	FileHandle fileHandle;
	RID rid;
	RBFM_ScanIterator rbfm_ScanIterator;
	void *CatalogPage = malloc(PAGE_SIZE); //hope the name not exceeding that size
	rbfm->openFile(catalog, fileHandle);
	void *tableInfo = malloc(sizeof(int) + tableName.size());
	*(int *) tableInfo = tableName.size();
	memcpy((char *) tableInfo + sizeof(int), tableName.c_str(),
			tableName.size());
	vector<string> selectAttributes;
	selectAttributes.push_back(string("TableId"));
	rbfm->scan(fileHandle, catalog_v, string("TableName"), EQ_OP, tableInfo,
			selectAttributes, rbfm_ScanIterator);
	if (rbfm_ScanIterator.getNextRecord(rid, CatalogPage) != 0) { //does TableName exist?
		free(CatalogPage);
		free(tableInfo);
		rbfm_ScanIterator.close();
		return -1;
	}
	free(CatalogPage);
	free(tableInfo);
	void * TableId = malloc(sizeof(int));
	rbfm->readAttribute(fileHandle, catalog_v, rid, "TableId", TableId);
	rbfm_ScanIterator.close();

	FileHandle fileHandle2;
	RID rid2;
	RBFM_ScanIterator rbfm_ScanIterator2;
	void *ColumnPage = malloc(PAGE_SIZE);
	rbfm->openFile(column, fileHandle2);
	void *attributeInfo = malloc(sizeof(int) + attributeName.size());
	*(int *) attributeInfo = attributeName.size();
	memcpy((char *) attributeInfo + sizeof(int), attributeName.c_str(),
			attributeName.size());
	vector<string> selectColumn;
	selectColumn.push_back(string("TableId"));
	rbfm->scan(fileHandle2, column_v, string("ColumnName"), EQ_OP,
			attributeInfo, selectColumn, rbfm_ScanIterator2);
	if (rbfm_ScanIterator2.getNextRecord(rid2, ColumnPage) != 0) {
		free(ColumnPage);
		free(attributeInfo);
		rbfm_ScanIterator2.close();
		return -1;
	}
	void * TableId2 = malloc(sizeof(int));
//void *TableRecord = malloc(11);
	rbfm->readAttribute(fileHandle2, column_v, rid2, "TableId", TableId2);
	int id1;
	int id2;
	memcpy(&id1, (char*) TableId, sizeof(int));
	memcpy(&id2, (char*) TableId2, sizeof(int));
	//cout << id1 << " " << id2 << endl;
	while (id1 != id2) {
		if (rbfm_ScanIterator2.getNextRecord(rid2, ColumnPage) != 0) {
			free(ColumnPage);
			free(attributeInfo);
			rbfm_ScanIterator2.close();
			return -1;
		}
		rbfm->readAttribute(fileHandle2, column_v, rid2, "TableId", TableId2);
		memcpy(&id2, (char*) TableId2, sizeof(int));
	}
	free(ColumnPage);
	free(attributeInfo);
	rbfm_ScanIterator2.close();

	FileHandle fh;
	rbfm->openFile(column, fh);
	void* record = malloc(PAGE_SIZE);
	rbfm->readRecord(fh, column_v, rid2, record);
	int offset = 0;
	offset += sizeof(int);  // skip tableid
	offset += sizeof(int);  // skip name
	int namelen;
	memcpy(&namelen, (char*) record + sizeof(int), sizeof(int));
	offset += namelen;
	offset += sizeof(int); //skip type
	int length = -1;
	memcpy((char*) record + offset, &length, sizeof(int));  //set lenght -1
	rbfm->updateRecord(fh, column_v, record, rid2);
	map<string, vector<Attribute> >::iterator iter = attrMap.find(tableName);
	attrMap.erase(iter);
	return 0;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName,
		const Attribute &attr) {
	FileHandle fh;
	if (rbfm->openFile(column, fh) != 0) {
		return -1;
	}
	vector<Attribute> attributes;
	attributes.push_back(attr);
	if (UpdateColumnTable(fh, attributes, MaxTableId, column_v) != 0) {
		return -2;
	}
	map<string, vector<Attribute> >::iterator iter = attrMap.find(tableName);
	attrMap.erase(iter);
	return 0;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName) {
	FileHandle fh;
	rbfm->openFile(tableName, fh);
	vector<Attribute> a;
	rbfm->reorganizeFile(fh, a);
	rbfm->closeFile(fh);
	return 0;
}

RC RelationManager::createIndex(const string &tableName,
		const string &attributeName) {
	string indexTableName = tableName + "." + attributeName;
	FileHandle fileHandle;

	if (rbfm->openFile(indexTableName, fileHandle) == 0) {//index exists
		rbfm->closeFile(fileHandle);
		return -1;
	}
	ix->createFile(indexTableName,1);
	rbfm->createFile(IndexCatalog);
	rbfm->openFile(IndexCatalog, fileHandle);
//tablename , attriname
	void *CatalogPage = malloc(PAGE_SIZE);
	memset(CatalogPage,0,PAGE_SIZE);
	unsigned int offset = 0;

	unsigned int indexTableNameLength = (unsigned int) attributeName.length()
			+ (unsigned int) tableName.length();
	memcpy((char*) CatalogPage + offset, &indexTableNameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char*) CatalogPage + offset, indexTableName.c_str(),
			sizeof(char) * indexTableNameLength);
	offset += indexTableNameLength;

	unsigned int attributeNameLength = (unsigned int) attributeName.length();
	memcpy((char*) CatalogPage + offset, &attributeNameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char*) CatalogPage + offset, attributeName.c_str(),
			sizeof(char) * attributeNameLength);
	offset += attributeNameLength;

	unsigned int tableNameLength = (unsigned int) tableName.length();
	memcpy((char*) CatalogPage + offset, &tableNameLength, sizeof(int));
	offset += sizeof(int);
	memcpy((char*) CatalogPage + offset, tableName.c_str(),
			sizeof(char) * tableNameLength);
	//offset += tableNameLength;
	RID rid;
	rbfm->insertRecord(fileHandle, IndexTable_v, CatalogPage, rid);

	//cout<<"indexTableNameLength:"<<indexTableNameLength<<endl;
	//cout<<"tableNameLength:"<<tableNameLength<<endl;
	//cout<<"attributeNameLength:"<<attributeNameLength<<endl;
	//cout<<"00000000000000"<<endl;
	MakeIndex(tableName,attributeName);

	free(CatalogPage);
	rbfm->closeFile(fileHandle);
	return 0;
}

RC RelationManager::MakeIndex(const string &tableName,const string &attributeName){
	
	FileHandle fileHandle;
	RBFM_ScanIterator rbfm_ScanIterator;
	string indexTableName = tableName + "." + attributeName;


	vector<Attribute> Attributes;
	getAttributes(tableName, Attributes);
	vector<Attribute> *name=new vector<Attribute>();
	unsigned i=0;
	for(;i<Attributes.size();i++)
	{
		//cout<<"Attributes[i].name:"<<Attributes[i].name<<endl;
		if(Attributes[i].name==attributeName)
		{
			break;
		}
	}

	rbfm->openFile(tableName,fileHandle);
	vector<string> givenAttr;
	givenAttr.push_back(attributeName);
	
	if(rbfm->scan(fileHandle, Attributes, "", NO_OP, NULL, givenAttr, rbfm_ScanIterator)==0)
	{
		//cout<<"222222222222222"<<endl;
		RID rid;
		void *returnedData=malloc(PAGE_SIZE);
		memset(returnedData,0,PAGE_SIZE);
		IXFileHandle ixfileHandle;
		ix->openFile(indexTableName,ixfileHandle);
		while(rbfm_ScanIterator.getNextRecord(rid, returnedData) != RM_EOF)
		{

			//float in=*(float*) returnedData;
            //cout<<"in:"<<in<<endl;
			//cout<<"rid1:"<<rid.slotNum<<endl;
			//cout<<"rid2:"<<rid.pageNum<<endl;
			ix->insertEntry(ixfileHandle,Attributes[i],returnedData,rid);
			//free(attr);
		}
		ix->printIndexEntriesInAPage(ixfileHandle,Attributes[i], 0) ;

		ix->closeFile(ixfileHandle);

		free(returnedData);
	}


	return 0;
}


RC RelationManager::destroyIndex(const string &tableName,
		const string &attributeName) {
	string indexTableName = tableName + "." + attributeName;
	rbfm->destroyFile(indexTableName+ "_primary"); 
	rbfm->destroyFile(indexTableName+ "_meta");  //delete file

	RID Irid;
	FileHandle fileHandle;
	RBFM_ScanIterator rbfm_ScanIterator;
	void *CatalogPage = malloc(PAGE_SIZE);
	if (rbfm->openFile(IndexCatalog, fileHandle) == -1) {
		return -1;
	}
	void *tableInfo = malloc(sizeof(int) + indexTableName.size());
	*(int *) tableInfo = indexTableName.size();
	memcpy((char *) tableInfo + sizeof(int), indexTableName.c_str(),
			indexTableName.size());
	vector<string> attributes;
	attributes.push_back(string("indexFileName"));
	rbfm->scan(fileHandle, IndexTable_v, string("indexFileName"), EQ_OP,
			tableInfo, attributes, rbfm_ScanIterator);
	if (rbfm_ScanIterator.getNextRecord(Irid, CatalogPage) != -1) {
		rbfm->deleteRecord(fileHandle, IndexTable_v, Irid);
		attributes.clear();
		free(tableInfo);
		free(CatalogPage);
		rbfm_ScanIterator.close();
		return 0;
	}
	attributes.clear();
	free(tableInfo);
	free(CatalogPage);
	rbfm_ScanIterator.close();
	return 0;
}

RC RelationManager::indexScan(const string &tableName,
		const string &attributeName, const void *lowKey, const void *highKey,
		bool lowKeyInclusive, bool highKeyInclusive,
		RM_IndexScanIterator &rm_IndexScanIterator) {
    //cout<<"lowKey:"<<*(float*)lowKey<<endl;
    //cout<<"highKey:"<<*(int*)highKey<<endl;
	IXFileHandle ixfileHandle;
	Attribute attribute;
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	for (unsigned i = 0; i < attrs.size(); i++) {
			
		if (tableName+"."+attrs[i].name == attributeName) {
			attribute = attrs[i];
			///attribute.name=tableName+"."+attribute.name;
			break;
		}
	}
	ix->openFile(attributeName, ixfileHandle);
    
	//float low;
   // memcpy(&low, lowKey, sizeof(int));
    //cout<<"low:"<<low<<endl;
	//float high;
    //memcpy(&high, highKey, sizeof(int));
    //cout<<"high:"<<high<<endl;

	ix->scan(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive,
                    highKeyInclusive, rm_IndexScanIterator.rmindexscaner);
	cout<<"get out of scan"<<endl;
	return 0;
}

