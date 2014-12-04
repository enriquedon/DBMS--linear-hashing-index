
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition)
{
    cond=condition;
    initer=input;
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
            func(data,tmp,op,agg);
            count++;
        }
        if(op==AVG)
        {
            *(float *)data=(*(float *)data)/count;
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
        return -1;
}
void Aggregate::func(void *record,void * tmp,AggregateOp op,Attribute agg)
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


// ... the rest of your implementations go here
