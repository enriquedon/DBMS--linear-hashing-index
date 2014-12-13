
#include "qe.h"
#include <iostream>
using namespace std;

Filter::Filter(Iterator* input, const Condition &condition)
{
    cond=condition;
    initer=input;
}
void freeVector(vector<void *> &newPages) {
    for (unsigned i =0; i<newPages.size(); i++) {
        free(newPages[i]);
    }
}
int printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
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
template <class T>
bool check(T a,T b,CompOp compOp)
{
    if(compOp==EQ_OP)
        return a==b;
    if(compOp==LT_OP)
        return a<b;
    if(compOp==GT_OP)
        return a>b;
    if(compOp==LE_OP)
        return a<=b;
    if(compOp==GE_OP)
        return a>=b;
    if(compOp==NE_OP)
        return a!=b;
    if(compOp==NO_OP)
        return true;
    return false;
}

bool ifSatisfy(CompOp compOp,void *value,void *attrValue,int attrType)
{
    if(compOp==NO_OP)
        return true;
    if(attrType==0)
    {
        return check(*(int*)attrValue, *(int*)value,compOp);
    }
    if(attrType==1)
    {
        return check<float>(*(float*)attrValue,*(float*)value,compOp);
    }
    else
    {
        string attr="", target="";
        int attrLength;
        int targetLength;
        memcpy(&targetLength, (char*)value, sizeof(int));
        memcpy(&attrLength, (char*)attrValue, sizeof(int));
        attr.assign((char*)attrValue+sizeof(int), (char*)attrValue+sizeof(int) + attrLength);
        target.assign((char*)value+sizeof(int), (char*)value+sizeof(int) + targetLength);
        return check<string>(attr,target,compOp);
    }
}
int getAttrPosition(void *data,const vector <Attribute> &recordDescriptor,int position)
{
    unsigned length=0;
    for(unsigned i=0;i<position;i++)
    {
        if(recordDescriptor[i].type==2)
        {
            int tmp;
            tmp=*(int *)((char *)data+length);
            length+=sizeof(int)+tmp;
        }
        else
        {
            length+=sizeof(int);
        }
    }
    return length;
}
int getAttrFromRecord(const vector<Attribute> &recordDescriptors,const string attrName,
                       void *attrData, void *recordData)
{
    int attrPosition;
    for(attrPosition=0;attrPosition<recordDescriptors.size();attrPosition++)
    {
        if(recordDescriptors[attrPosition].name==attrName)
            break;
    }
    if(attrPosition==recordDescriptors.size())
        return -1;
    int pointer = getAttrPosition(recordData, recordDescriptors, attrPosition);
    if (recordDescriptors[attrPosition].type==2) {
        memcpy((char *)attrData,(char *)recordData+pointer,sizeof(int));
        memcpy((char *)attrData+sizeof(int),(char *)recordData+pointer+sizeof(int),*(int *)attrData);
        return *((int *)attrData)+sizeof(int);
    }
    else
    {
        memcpy((char *)attrData,(char *)recordData+pointer,sizeof(int));
        return sizeof(int);
    }
}
bool Filter::checkCondition(void *data,Condition cond,Iterator *initer)
{
    //pick up attribute value
    vector <Attribute> recordDescriptor;
    initer->getAttributes(recordDescriptor);
    int leftpos=0,rightpos=0;
    for(leftpos=0;leftpos<recordDescriptor.size();leftpos++)
    {
        if(recordDescriptor[leftpos].name==cond.lhsAttr)
            break;
    }
    for(rightpos=0;rightpos<recordDescriptor.size();rightpos++)
    {
        if(recordDescriptor[rightpos].name==cond.rhsAttr)
            break;
    }
    void *leftAttr=malloc(PAGE_SIZE);
    void *rightAttr=malloc(PAGE_SIZE);
    getAttrFromRecord(recordDescriptor,cond.lhsAttr,leftAttr,data);
    if(cond.bRhsIsAttr)
        getAttrFromRecord(recordDescriptor,cond.rhsAttr,rightAttr,data);
    else
    {
        if(recordDescriptor[leftpos].type==2)
        {
            int tmp=*(int *)cond.rhsValue.data;
            memcpy(rightAttr,cond.rhsValue.data,tmp+sizeof(int));
        }
        else
        {
            memcpy(rightAttr,cond.rhsValue.data,sizeof(int));
        }
    }
    bool result= ifSatisfy(cond.op,rightAttr,leftAttr,recordDescriptor[leftpos].type);
    free(leftAttr);
    free(rightAttr);
    return result;
}
Project::Project(Iterator *input,const vector<string> &attrNames)
{
    initer=input;
    projAttrs=attrNames;
}

RC Filter::getNextTuple(void *data)
{
    while(initer->getNextTuple(data)!=-1)
    {
        if(checkCondition(data,cond,initer))
            return 0;
    }
    return -1;
};

RC Project::getNextTuple(void *data)
{
    void *record=malloc(PAGE_SIZE);
    if(initer->getNextTuple(record)==-1)
    {
        free(record);
        return -1;
    }
    vector <Attribute> recordDescriptor;
    initer->getAttributes(recordDescriptor);
    int currLength=0;
    for(int i=0;i<projAttrs.size();i++)
    {
        currLength=currLength+getAttrFromRecord(recordDescriptor,projAttrs[i],(void *)((char *)data+currLength),record);
    }
    free(record);
    return 0;
}
void Project::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    vector <Attribute> recordDescriptor;
    initer->getAttributes(recordDescriptor);
    for(int i=0;i<projAttrs.size();i++)
    {
        for(int j=0;j<recordDescriptor.size();j++)
        {
            if(recordDescriptor[j].name==projAttrs[i])
            {
                attrs.push_back(recordDescriptor[j]);
                break;
            }
        }
    }
};

Aggregate::Aggregate(Iterator *input,Attribute aggAttr,AggregateOp op)
{
    initer=input;
    agg=aggAttr;
    this->op=op;
    grouped=false;
}

RC Aggregate::getNextTuple(void *data)
{
    if(!grouped)
    {
        void *record=malloc(PAGE_SIZE);
        void *tmp=malloc(sizeof(int));
        vector <Attribute> recordDescriptor;
        initer->getAttributes(recordDescriptor);
        if(initer->getNextTuple(record)==-1)
        {
            free(record);
            free(tmp);
            return -1;
        }
        //get the value of the attribute
        getAttrFromRecord(recordDescriptor,agg.name,tmp,record);
        memcpy(data,tmp,sizeof(int));
        int count=1;
        while(initer->getNextTuple(record)!=-1)
        {
            getAttrFromRecord(recordDescriptor,agg.name,tmp,record);
            calculate(data,tmp,op,agg);
            count++;
        }
        if(op==AVG)
        {
            if (agg.type==1) {
                *(float *)data=(*(float *)data)/count;
            }
            else
            {
                float tmp = (float)*(int*)data;
                *(float*)data = tmp/count;
            }
        }
        if(op==COUNT)
        {
            *(int *)data=count;
        }
        free(record);
        free(tmp);
        return 0;
    }
    else
    {
        
        if(grAttr.type==0)
        {
            if(currentPosition<intvector.size())
            {
                memcpy(data,&(intvector[currentPosition].first),sizeof(int));
                if(op==AVG)
                {
                    float tmp;
                    if (agg.type==1) {
                        tmp = (float)*(float *)intvector[currentPosition].second.first/intvector[currentPosition].second.second;
                    }
                    else
                    {
                        tmp = (float)*(int*)intvector[currentPosition].second.first/intvector[currentPosition].second.second;
                    }
                    memcpy((char*)data+sizeof(int),&tmp,sizeof(int));
                }
                else if(op==COUNT)
                {
                    memcpy((char*)data+sizeof(int),(char*)intvector[currentPosition].second.second,sizeof(int));
                }
                else
                {
                    memcpy((char*)data+sizeof(int),(char*)intvector[currentPosition].second.first,sizeof(int));
                }
                currentPosition++;
                return 0;
            }
            return -1;
        }
        if(grAttr.type==1)
        {
            
            if(currentPosition<floatvector.size())
            {
                memcpy(data,&(floatvector[currentPosition].first),sizeof(int));
                if(op==AVG)
                {
                    float tmp;
                    if (agg.type==1) {
                        tmp = (float)*(float *)floatvector[currentPosition].second.first/floatvector[currentPosition].second.second;
                    }
                    else
                    {
                        tmp = (float)*(int*)floatvector[currentPosition].second.first/floatvector[currentPosition].second.second;
                    }
                    memcpy((char*)data+sizeof(int),&tmp,sizeof(int));
                }
                else if(op==COUNT)
                {
                    memcpy((char*)data+sizeof(int),(char*)floatvector[currentPosition].second.second,sizeof(int));
                }
                else
                {
                    memcpy((char*)data+sizeof(int),(char*)floatvector[currentPosition].second.first,sizeof(int));
                }
                currentPosition++;
                return 0;
            }
            return -1;
             
        }
        else
        {
            if(currentPosition<stringvector.size())
            {
                int stringsize=stringvector[currentPosition].first.size();
                const char * str = stringvector[currentPosition].first.c_str();
                memcpy(data,&stringsize,sizeof(int));
                memcpy((char*)data+sizeof(int),str,stringsize);
                if(op==AVG)
                {
                    float tmp;
                    if (agg.type==1) {
                        tmp = (float)*(float *)stringvector[currentPosition].second.first/stringvector[currentPosition].second.second;
                    }
                    else
                    {

                        tmp = (float)*(int*)stringvector[currentPosition].second.first/stringvector[currentPosition].second.second;

                    }
                    memcpy((char*)data+sizeof(int)+stringsize,&tmp,sizeof(int));
                }
                else if(op==COUNT)
                {
                    memcpy((char*)data+sizeof(int)+stringsize,(char*)stringvector[currentPosition].second.second,sizeof(int));
                }
                else
                {
                    memcpy((char*)data+sizeof(int)+stringsize,(char*)stringvector[currentPosition].second.first,sizeof(int));
                }
                currentPosition++;
                return 0;
            }
            return -1;
        }
    }
}
void Aggregate::calculate(void *record,void * tmp,AggregateOp op,Attribute agg)
{
    if(op==MIN)
    {
        if(agg.type==0)
            *(int *)record=min(*(int *)record,*(int *)tmp);
        if(agg.type==1)
            *(float *)record=min(*(float *)record,*(float *)tmp);
    }
    if(op==MAX)
    {
        if(agg.type==0)
            *(int *)record=max(*(int *)record,*(int *)tmp);
        if(agg.type==1)
            *(float *)record=max(*(float *)record,*(float *)tmp);
    }
    if(op==SUM or op==AVG)
    {
        if(agg.type==0)
            *(int *)record+=*(int *)tmp;
        if(agg.type==1)
            *(float *)record+=*(float *)tmp;
    }
}
Aggregate::Aggregate(Iterator *input,Attribute aggAttr,Attribute gAttr,AggregateOp op, const unsigned numPartitions)
{
    currentPosition = 0;
    initer=input;
    agg=aggAttr;
    grouped=true;
    this->op=op;
    grAttr=gAttr;
    vector <Attribute> recordDescriptor;
    initer->getAttributes(recordDescriptor);
    void *group=malloc(PAGE_SIZE);
    void *record=malloc(PAGE_SIZE);
    void *value=malloc(sizeof(int));
    while(initer->getNextTuple(record)!=-1)
    {
        getAttrFromRecord(recordDescriptor,gAttr.name,group,record);
        getAttrFromRecord(recordDescriptor,agg.name,value,record);
        if(grAttr.type==0)
        {
            if(intmap.find(*(int *)group)==intmap.end())
            {
                void *tmp = malloc(sizeof(int));
                memcpy(tmp, value, sizeof(int));
                pair<void*, int> tmppair = make_pair(tmp, 1);
                intmap[*(int *)group]=tmppair;
            }
            else
            {
                    intmap[*(int*)group].second++;
                    calculate(intmap[*(int*)group].first,value,op,agg);

            }
        }
        if(grAttr.type==1)
        {
            if(floatmap.find(*(float *)group)==floatmap.end())
            {
                void *tmp = malloc(sizeof(int));
                memcpy(tmp, value, sizeof(int));
                pair<void*, int> tmppair = make_pair(tmp, 1);
                floatmap[*(float *)group]=tmppair;
            }
            else
            {
                    floatmap[*(float*)group].second++;
                    calculate(floatmap[*(float*)group].first,value,op,agg);
            }
        }
        else
        {
            string groupstring="";
            int stringLength;
            memcpy(&stringLength, (char*)group, sizeof(int));
            groupstring.assign((char*)group+sizeof(int), (char*)group+sizeof(int) + stringLength);
            if(stringmap.find(groupstring)==stringmap.end())
            {
                void *tmp = malloc(sizeof(int));
                memcpy(tmp, value, sizeof(int));
                pair<void*, int> tmppair = make_pair(tmp, 1);
                stringmap[groupstring] = tmppair;
            }
            else
            {
                stringmap[groupstring].second++;
                calculate(stringmap[groupstring].first,value,op,agg);
            }
        }
    }
    intvector=vector <pair<int, pair<void *, int> > >(intmap.begin(),intmap.end());
    floatvector=vector <pair<float, pair<void *, int> > >(floatmap.begin(),floatmap.end());
    stringvector=vector <pair<string, pair<void *, int> > >(stringmap.begin(),stringmap.end());
    free(record);
    free(group);
   // free(value);
}

int getRecordLength(const vector<Attribute> &recordDescription, const void *data)
{
    unsigned size = recordDescription.size();
    unsigned offset = 0;
    for (int i = 0; i < size; i++) {
        if (recordDescription[i].length == -1) {
            continue;
        }
        if (recordDescription[i].type == 0) {
            offset += sizeof(int);
        } else if (recordDescription[i].type == 1) {
            offset += sizeof(float);
        } else {
            int varLength;
            memcpy(&varLength, (char *) data + offset, sizeof(int));
            offset = offset + sizeof(int) + varLength;
        }
    }
    return offset;
}

bool checkSat(vector<Attribute> leftAttrdes, vector<Attribute> rightAttrdes, void* leftrecord, void* rightrecord, Condition cond)
{
    int leftpos=0,rightpos=0;
    for(leftpos=0;leftpos<leftAttrdes.size();leftpos++)
    {
        if(leftAttrdes[leftpos].name==cond.lhsAttr)
            break;
    }
    for(rightpos=0;rightpos<rightAttrdes.size();rightpos++)
    {
        if(rightAttrdes[rightpos].name==cond.rhsAttr)
            break;
    }
    void *leftAttr=malloc(PAGE_SIZE);
    void *rightAttr=malloc(PAGE_SIZE);
    getAttrFromRecord(leftAttrdes,cond.lhsAttr,leftAttr,leftrecord);
    getAttrFromRecord(rightAttrdes,cond.rhsAttr,rightAttr,rightrecord);
    bool result= ifSatisfy(cond.op,rightAttr,leftAttr,leftAttrdes[leftpos].type);
    free(leftAttr);
    free(rightAttr);
    return result;
}

BNLJoin::BNLJoin(Iterator *leftIn,TableScan *rightIn,const Condition &condition, const unsigned numRecords)
{
    left=leftIn;
    right=rightIn;
    cond=condition;
    numRec = numRecords;
    leftIn->getAttributes(leftdes);
    rightIn->getAttributes(rightdes);
    leftdata=malloc(PAGE_SIZE);
    void *tmprecord=malloc(PAGE_SIZE);
    while(right->getNextTuple(tmprecord)!=-1)
    {
        int tmplength =getRecordLength(rightdes, tmprecord);
        void *rightrecord = malloc(tmplength);
        memcpy(rightrecord, tmprecord, tmplength);
        records.push_back(rightrecord);
    }
    free(tmprecord);
}
RC BNLJoin::getNextTuple(void *data)
{
    if (left->getNextTuple(leftdata)==-1) {
        return -1;
    }
    do
    {
        for (int rightrecord=0; rightrecord<records.size(); rightrecord++) {
            if(checkSat(leftdes, rightdes, leftdata, records[rightrecord], cond))
            {
                int leftLength= getRecordLength(leftdes, leftdata);
                memcpy(data,leftdata,leftLength);
                int rightLength= getRecordLength(rightdes, records[rightrecord]);
                memcpy((char *)data+leftLength,records[rightrecord],rightLength);
                return 0;
            }
        }
    }
    while(left->getNextTuple(leftdata)!=-1);
    freeVector(records);
    return -1;
}

INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        ){
    cout<<"000000000000"<<endl;
    leftInput=leftIn;
    rightInput=rightIn;
    cond=condition;
    leftIn->getAttributes(leftdes);
    rightIn->getAttributes(rightdes);
    leftdata=malloc(PAGE_SIZE);
    rightdata=malloc(PAGE_SIZE);

}

RC INLJoin::getNextTuple(void *data){
    if (leftInput->getNextTuple(leftdata)==-1) {
        return QE_EOF;
    }
    do
    {
        memset(leftdata,0,PAGE_SIZE);       
        memset(rightdata,0,PAGE_SIZE);
        while(rightInput->getNextTuple(rightdata)!=-1); {
            if(checkSat(leftdes, rightdes, leftdata, rightdata, cond))
            {
                int leftLength= getRecordLength(leftdes, leftdata);
                memcpy(data,leftdata,leftLength);
                int rightLength= getRecordLength(rightdes, rightdata);
                memcpy((char *)data+leftLength,rightdata,rightLength);
                return 0;
            }
        }
    }while(leftInput->getNextTuple(leftdata)!=-1);
    return QE_EOF;
};


// ... the rest of your implementations go here
