#ifndef _pfm_h_
#define _pfm_h_

#include<cstdio>
#include<map>
#include<string>
#include<stdlib.h>

typedef int RC;
typedef unsigned PageNum;

#define PAGE_SIZE 4096

class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance

    RC createFile    (const char *fileName);                         // Create a new file
    RC destroyFile   (const char *fileName);                         // Destroy a file
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                       // Close a file

    RC decrementFileCount(std::string fileName);   					// decrement the filehandle count in the directory
    unsigned numOfFileHandle(std::string fileName);                      // how many fileHandle is handling this file

protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
    std::map<std::string, unsigned> fileDirectory;
};


class FileHandle
{
public:
    FileHandle();                                                    // Default constructor
    ~FileHandle();                                                   // Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file

    FILE * getFile();
    void setFile(FILE *file);
    void clearFile();
    void setFileName(const char *fileName);
    std::string getFileName();


private:
    FILE *file;														// ptr points to the file under handling
    std::string fileName;
};

#endif
