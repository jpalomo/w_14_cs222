#include "pfm.h"

using namespace std;

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
	_pf_manager = NULL;
}

/*
 * This method creates a paged file called fileName. The file should not already exist.
 */
RC PagedFileManager::createFile(const char *fileName)
{
	if (fileName != NULL) {
		FILE *file;

		//check if the file exists
		//fopen should be used as binary mode
		file = fopen(fileName, "rb");
		if (file != NULL) {
			//file with name passed already exists
			fclose(file);
			return -1;
		}

		//create a new file and then close it
		file = fopen(fileName, "wb");
		if (file != NULL) {
			fclose(file);
			return 0;
		}
	}

	return -1;
}

/*
 * This method destroys the paged file whose name is fileName. The file should exist.
 */
RC PagedFileManager::destroyFile(const char *fileName)
{
	string name(fileName);
	if (fileName != NULL && fileDirectory.find(name) == fileDirectory.end()) {
		return remove(fileName);
	}
	return -1;
}

/*
 * This method opens the paged file whose name is fileName.
 * The file must already exist and it must have been created using the CreateFile method.
 * If the method is successful, the fileHandle object whose address is passed as a parameter becomes a "handle" for the open file.
 * The file handle is used to manipulate the pages of the file.
 * It is an error if fileHandle is already a handle for an open file when it is passed to the OpenFile method.
 * It is not an error to open the same file more than once if desired, using a different fileHandle object each time.
 * Each call to the OpenFile method creates a new "instance" of the open file.
 *
 * Warning: Opening a file more than once for data modification is not prevented by the PF component,
 * but doing so is likely to corrupt the file structure and may crash the PF component. Opening a file more than once for reading is no problem.
 */
RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
	//check if file exists
	FILE *file = fopen(fileName, "rb+");
	if (file == NULL) {
		perror("File not exists!");
		fclose(file);
		return -1;
	}

	//check if fileHandle is a handle for another file
	if (fileHandle.getFile() != 0) {
		perror("FileHandle is handling another file!");
		fclose(file);
		return -1;
	}


	string name(fileName);
	if (fileDirectory.find(name) != fileDirectory.end()) {
		//if file name exists
		fileDirectory[name]++;
	}
	else {
		//increment the fileHandle counts on the file
		fileDirectory[name] = 1;
	}

	fileHandle.setFile(file);
	fileHandle.setFileName(fileName);

	return 0;
}

/*
 * This method closes the open file instance referred to by fileHandle. The file must have been opened using the OpenFile method.
 * All of the file's pages are flushed to disk when the file is closed.
 */
RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    //check if fileHandle is handling file
	if (fileHandle.getFile() == NULL) {
		perror("FileHandle is not handling any file!");
		return -1;
	}

	int result = fclose(fileHandle.getFile());
	decrementFileCount(fileHandle.getFileName());
	fileHandle.clearFile();
	return result;
}

RC PagedFileManager::decrementFileCount(string fileName)
{
	//make sure the map has entries and the file name exists
    if (fileDirectory.find(fileName) != fileDirectory.end()){
    	unsigned count = fileDirectory[fileName];
    	count -= 1;

    	if(count == 0){
    		fileDirectory.erase(fileName);
    		return 0;
    	}

    	fileDirectory[fileName] = count;
    }
    return -1;
}

unsigned PagedFileManager::numOfFileHandle(string fileName) {
	if (fileDirectory.find(fileName) == fileDirectory.end())
		return 0;

	return fileDirectory[fileName];
}


FileHandle::FileHandle() : file(NULL)
{
}


FileHandle::~FileHandle()
{
	delete file;
}

/*
 * This method reads the page into the memory block pointed by data. The page should exist. Note the page number starts from 0.
 */
RC FileHandle::readPage(PageNum pageNum, void *data)
{
    //check if this page exists
	if (pageNum >= getNumberOfPages())
		return -1;

	//set file to the beginning of the page
	if (fseek(file, PAGE_SIZE * pageNum, SEEK_SET) != 0)
		return -1;

	size_t result = fread(data, 1, PAGE_SIZE, file);
	return result == PAGE_SIZE ? 0 : -1;
}

/*
 * This method writes the data into a page specified by the pageNum. The page should exist. Note the page number starts from 0.
 */
RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	//check if this page exists
	if (pageNum >= getNumberOfPages())
		return -1;

	//set file to the beginning of the page
	if (fseek(file, PAGE_SIZE * pageNum, SEEK_SET) != 0)
		return -1;

	size_t result = fwrite(data, 1, PAGE_SIZE, file);
	return result == PAGE_SIZE ? 0 : -1;
}

/*
 * This method appends a new page to the file, and writes the data into the new allocated page.
 */
RC FileHandle::appendPage(const void *data)
{
    //set file to end
	fseek(file, 0, SEEK_END);
    int result = fwrite(data, 1, PAGE_SIZE, file);
    return result == PAGE_SIZE ? 0 : -1;
}

/*
 * This method returns the total number of pages in the file.
 */
unsigned FileHandle::getNumberOfPages()
{
	fseek(file, 0, SEEK_END);
	long size = ftell(file);
	return (unsigned) (size / PAGE_SIZE);
}

FILE * FileHandle::getFile() {
	return file;
}

void FileHandle::setFile(FILE *file) {
	this->file = file;
}

void FileHandle::setFileName(const char *fileName) {
	this->fileName.assign(fileName);
}

std::string FileHandle::getFileName() {
	return fileName;
}

void FileHandle::clearFile() {
	fileName.clear();
	file = NULL;
}


