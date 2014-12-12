#ifndef _qe_h_
#define _qe_h_

#include <vector>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

# define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN = 0, MAX, SUM, AVG, COUNT } AggregateOp;


// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by
//                 the characters

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
                attrNames.push_back(attrs[i].name);
            }

            // Call rm scan to get iterator
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
                tmp += attrs[i].name;
                attrs[i].name = tmp;
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
            cout<<"lowKey:"<<*(float*)lowKey<<endl;
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            cout<<"indexcsan getNextTuple"<<endl;
            int rc = iter->getNextEntry(rid, key);
            cout<<"indexcsan getNextTuple key:"<<*(float*)key<<endl;
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
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};



class Filter : public Iterator {
    // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter(){};
    
        bool checkCondition(void *data,Condition cond,Iterator *initer);
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const
        {
            initer->getAttributes(attrs);
        }
    private:
        Iterator *initer;
        Condition cond;
    
    
};


class Project : public Iterator {
    // Projection operator
    public:
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames);   // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
    private:
        Iterator *initer;
        vector <string> projAttrs;
};

class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      );
      ~GHJoin(){
      };

    RC getNextTuple(void *data);
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const
    {
        attrs.clear();
        for(int i=0;i<leftdes.size();i++)
            attrs.push_back(leftdes[i]);
        for(int i=0;i<rightdes.size();i++)
            attrs.push_back(rightdes[i]);

    };
    unsigned hash(unsigned numPartitions, Attribute &attribute, void *key);
    private:
    void partition();
    void getMap();
    RecordBasedFileManager *rbfm;
    Iterator *leftIn;
    Iterator *rightIn;
    Condition cond;
    Attribute rightAttr;
    Attribute leftAttr;
    vector <Attribute> rightdes,leftdes;
    
    unsigned numPart;
    vector<FileHandle> leftFile;
    vector<FileHandle> rightFile;
    
    vector<RBFM_ScanIterator> leftScan;
    vector<RBFM_ScanIterator> rightScan;
    
    map <int, void* > intmap;
    map <float, void* > floatmap;
    map <string, void* > stringmap;
    
    unsigned currentPart;
    void *leftdata;

};


class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numRecords     // # of records can be loaded into memory, i.e., memory block size (decided by the optimizer)
        );
        ~BNLJoin(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            for(int i=0;i<leftdes.size();i++)
                attrs.push_back(leftdes[i]);
            for(int i=0;i<rightdes.size();i++)
                attrs.push_back(rightdes[i]);
        };
    private:
        Iterator *left;
        TableScan *right;
        Condition cond;
        Attribute rightAttr;
        Attribute leftAttr;

        unsigned numRec;
        void *leftdata;
        vector<void*> records;
        vector <Attribute> rightdes,leftdes;
    
};

class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        ){};
        ~INLJoin(){};

        RC getNextTuple(void *data){return QE_EOF;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{};
};


class Aggregate : public Iterator {
    // Aggregation operator
    public:
        // Mandatory for graduate teams only
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone. 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op,              // Aggregate operation
                  const unsigned numPartitions // Number of partitions for input (decided by the optimizer)
        );
        ~Aggregate(){
            for(int i=0;i<intvector.size();i++)
                free(intvector[i].second.first);
            for(int i=0;i<floatvector.size();i++)
                free(floatvector[i].second.first);
            for(int i=0;i<stringvector.size();i++)
                free(stringvector[i].second.first);
        };

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void calculate(void *record,void * tmp,AggregateOp op,Attribute agg);
        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            if(grouped)
            {
                attrs.push_back(grAttr);
            }
            Attribute tmp;
            tmp.length = sizeof(int);
            if(op==COUNT)
            {
                tmp.name="COUNT";
                tmp.type=TypeInt;
                attrs.push_back(tmp);
            }
            if(op==AVG)
            {
                tmp.name="AVG";
                tmp.type=TypeReal;
                attrs.push_back(tmp);
            }
            if(op==SUM||op==MAX||op==MIN)
            {
                if(op==SUM)
                    tmp.name="SUM";
                if(op==MAX)
                    tmp.name="MAX";
                if(op==MIN)
                    tmp.name="MIN";
                tmp.type=agg.type;
                attrs.push_back(tmp);
            }
        };
    private:
    
        Iterator *initer;
        Attribute agg;
        bool grouped;
        Attribute grAttr;
        AggregateOp op;
        int currentPosition;
        map <int, pair<void *, int> > intmap;
        map <float,pair<void *, int> > floatmap;
        map <string, pair<void *, int> > stringmap;
        vector <pair<int, pair<void *, int> > > intvector;
        vector <pair<float,pair<void *, int> > > floatvector;
        vector <pair<string, pair<void *, int> > > stringvector;

    
};

#endif
