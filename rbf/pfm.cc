#include "pfm.h"
#include <unistd.h>
#include <sys/types.h>

#include <iostream>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const char *fileName)
{
    try
    {   FILE *file = fopen(fileName, "r");
        if(file != NULL)
        {
            fclose(file);
            return 1;
        }
        else
        {
            file = fopen(fileName, "wb");
            fclose(file);
            return 0;
        }
    }
    catch(int e)
    {
        return e;
    }
}


RC PagedFileManager::destroyFile(const char *fileName)
{
    int status = remove(fileName);
    if(status == 0)
    {
        return 0;
    }
    else
    {
        return status;
    }
}


RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
    try
    {
        FILE *file = fopen(fileName,"r+");
        if (file == NULL) {
            return -1;
        }
        fileHandle.setFile(file);
        fseek(fileHandle.getFile(), 0, SEEK_END);
        fileHandle.setPageNumber(ftell(fileHandle.getFile())/PAGE_SIZE);
 //       fileHandle.setPageNumber(ftell(fileHandle.getFile())/PAGE_SIZE);
        fseek(fileHandle.getFile(), 0, SEEK_SET);

        return 0;
    }
    catch(int e)
    {
        return e;
    }
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    try
    {
        fclose(fileHandle.getFile());
        return 0;
    }
    catch(int e)
    {
        return e;
    }
}




FileHandle::FileHandle()
{
    file = NULL;
    pageNumber = 0;
    writePageCounter=0;
    readPageCounter=0;
    appendPageCounter=0;
}

FileHandle::~FileHandle()
{
}
FILE* FileHandle::getFile()
{
    return file;
}

void FileHandle::setFile(FILE *filestream)
{
    file = filestream;
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
    try
    {   //if(pageNum > pageNumber)
        if(pageNum >= pageNumber)
        {
            throw 2;
        }
        fseek(file, (pageNum)*PAGE_SIZE, SEEK_SET);
      //  fseek(file, (pageNum-1)*PAGE_SIZE, SEEK_SET);
        fread(data, PAGE_SIZE,1,file);
        readPageCounter++;
        return 0;
    }
    catch(int e)
    {
        return e;
    }
}

void FileHandle::setPageNumber(unsigned number)
{
    pageNumber = number;
}

RC FileHandle::readMeta(PageNum pageNum, void *data)
{
    try
    {   if(pageNum >= pageNumber)
        //if(pageNum > pageNumber)
    {
        throw 2;
    }
       // fseek(file, (pageNum)*PAGE_SIZE-2*sizeof(int), SEEK_SET);
        fseek(file, (pageNum+1)*PAGE_SIZE-2*sizeof(int), SEEK_SET);
        fread(data, 2*sizeof(int), 1, file);
       // fflush(file);
        return 0;
    }
    catch(int e)
    {
        return e;
    }
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    try
    {
        if(pageNum >= pageNumber)
      //  if(pageNum > pageNumber)
        {
            throw 2;
        }
      //  fseek(file, (pageNum-1)*PAGE_SIZE, SEEK_SET);
        fseek(file, (pageNum)*PAGE_SIZE, SEEK_SET);
        fwrite(data,PAGE_SIZE,1,file);
        fflush(file);
        writePageCounter++;
        return 0;
    }
    catch(int e)
    {
        return e;
    }

}


RC FileHandle::appendPage(const void *data)
{
    try
    {
        fseek(file, 0, SEEK_END);
        fwrite(data, PAGE_SIZE, 1, file);
        fflush(file);
        pageNumber++;
        appendPageCounter++;
        return 0;
    }
 catch(int e)
 {
     return e;
 }
}

unsigned FileHandle::getNumberOfPages()
{
    return pageNumber;
}
RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}


