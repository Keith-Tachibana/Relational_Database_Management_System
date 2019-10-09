#include "pfm.h"
#include <cstring>
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
	if(_pf_manager){
		delete _pf_manager;
	}
}

bool fileExists(const string &fileName){
	//Helper function to see if file already exists
	FILE* file = fopen(fileName.c_str(), "rb");
	if(!file){
		return false;
	}
	fclose(file);
	return true;
}


RC PagedFileManager::createFile(const string &fileName)
{
	//Checks if file already exists
	if(fileExists(fileName)){
		return -1;
	}
	//Creates file
	FILE* file = fopen(fileName.c_str(), "wb");
	//Checks if file creation is successful
	if(!file){
		return -1;
	}
	createHiddenPage(file);
	fclose(file);
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	//Error in deleting file
	if(remove(fileName.c_str()) != 0){
		return -1;
	}
    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	FILE* file = fopen(fileName.c_str(), "rb+");
	if(!file){
		return -1;
	}
	fileHandle.file = file;
	fileHandle.readCounters();
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	if(!fileHandle.file){
		return -1;
	}
	fileHandle.writeCounters();
	fclose(fileHandle.file);
	fileHandle.file = 0;
    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    file = 0;
    tableId = 0;
}


FileHandle::~FileHandle()
{
	if(file){
//		fclose(file);
		file = 0;
	}
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	//If file or data don't exist then return -1
	if(!file && !data){
		return -1;
	}
	//If asked for a page number greater than the number of pages in file
	if(pageNum + 1 > getNumberOfPages()){
		return -1;
	}
	//Tries to seek to the page position in file and returns -1 if fails
	if(fseek(file, (pageNum + 1) * PAGE_SIZE, SEEK_SET) != 0){
		return -1;
	}
	fread(data, PAGE_SIZE, 1, file);
    ++readPageCounter;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if(!file && !data){
		return -1;
	}
	if(pageNum + 1 > getNumberOfPages()){
		return -1;
	}
	//Tries to seek to the page position in file and returns -1 if fails
	if(fseek(file, (pageNum + 1) * PAGE_SIZE, SEEK_SET) != 0){
		return -1;
	}
	fwrite(data, PAGE_SIZE, 1, file);
	++writePageCounter;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
	if(!file && !data){
		return -1;
	}
	//Tries to seek to last page position in file and returns -1 if fails
	if(fseek(file, 0, SEEK_END) != 0){
		return -1;
	}
	fwrite(data, PAGE_SIZE, 1, file);
	++appendPageCounter;
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    return appendPageCounter;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
    return 0;
}

RC FileHandle::readCounters(){
	if(!file){
		return -1;
	}
	void *hiddenPage = malloc(PAGE_SIZE);
	int counters[4] = {0, 0, 0, 0};
	fseek(file, 0, SEEK_SET);
	fread(hiddenPage, PAGE_SIZE, 1, file);
	std::memcpy(&counters, hiddenPage, sizeof(counters));
	readPageCounter = counters[0];
	writePageCounter = counters[1];
	appendPageCounter = counters[2];
	tableId = counters[3];
	free(hiddenPage);
	return 0;
}

RC FileHandle::writeCounters(){
	void *hiddenPage = malloc(PAGE_SIZE);
    memset(hiddenPage, 0, PAGE_SIZE);
	int counters[4] = {readPageCounter, writePageCounter, appendPageCounter, tableId};
	std::memcpy(hiddenPage, &counters, sizeof(counters));
	fseek(file, 0, SEEK_SET);
	fwrite(hiddenPage, PAGE_SIZE, 1, file);
	free(hiddenPage);
	return 0;
}

RC FileHandle::getTableId(unsigned &tid){
	tid = tableId;
	return 0;
}

RC FileHandle::setTableId(const unsigned &tid){
	tableId = tid;
	return 0;
}

RC FileHandle::incrementTableId(){
	++tableId;
	return 0;
}

void createHiddenPage(FILE *file){
	void *hiddenPage = malloc(PAGE_SIZE);
    memset(hiddenPage, 0, PAGE_SIZE);
	int initialValues[4]= {0, 0, 0, 0};
	std::memcpy(hiddenPage, &initialValues, sizeof(initialValues));
	fseek(file, 0, SEEK_SET);
	fwrite(hiddenPage, PAGE_SIZE, 1, file);
	free(hiddenPage);
}
