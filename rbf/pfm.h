#ifndef _pfm_h_
#define _pfm_h_
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
typedef int RC;
typedef unsigned PageNum;

using namespace std;



#define PAGE_SIZE 4096

class FileHandle;


class PagedFileManager
{
public:
    static PagedFileManager * instance();                  // Access to the _pf_manager instance
    RC createFile    (const char *fileName);                         // Create a new file
    RC destroyFile   (const char *fileName);             // Destroy a file
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle) ;                     // Close a file
    
protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
    FileHandle();
    ~FileHandle();                                                   // Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                  // Append a specific page
    
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    RC readMeta(PageNum pageNum, void *data);                           // Get data about free space

    void setFile(FILE *filestream);                             //set file pointer
    FILE* getFile();                                            //get file pointer
    void setPageNumber(unsigned number);                        //set pageNumber
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
    const char *filen;
private:
    FILE *file;

    unsigned pageNumber;
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
 };

 #endif
