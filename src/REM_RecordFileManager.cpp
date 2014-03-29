#include <iostream>
#include "REM_RecordFileManager.h"

REM_RecordFileManager::REM_RecordFileManager(STORM_StorageManager * sm) {
	this->sm = sm;
}

REM_RecordFileManager::~REM_RecordFileManager(void) {}

t_rc REM_RecordFileManager::CreateRecordFile(const char * fname, int rs) {
	t_rc rc;
	STORM_FileHandle fh;
	STORM_PageHandle ph;
	char * pData;
	int pageID;
	RecordFileInfo rfi = {0,rs,(PAGE_SIZE - sizeof(RecordPageInfo))/rs};
	
	errno = 0;
	rc = this->sm->CreateFile(fname);
	if (rc != OK) return rc;

	rc = this->sm->OpenFile(fname,fh);
	if (rc != OK) return rc;

	rc = fh.ReservePage(ph);
	if (rc != OK) return rc;

	ph.GetPageID(pageID);
	if (rc != OK) return rc;

	rc = ph.GetDataPtr(&pData);
	if (rc != OK) return rc;

	memcpy(&pData[0],&rfi,sizeof(RecordFileInfo));

	rc = fh.MarkPageDirty(pageID);
	if (rc != OK) return rc;

	rc = fh.FlushPage(pageID);
	if (rc != OK) return rc;

	rc = fh.UnpinPage(pageID);
	if (rc != OK) return rc;

	rc = this->sm->CloseFile(fh);
	if (rc != OK) return rc;

	return (OK);
}

t_rc REM_RecordFileManager::DestroyRecordFile(const char * fname) {
	t_rc rc;
	
	rc = this->sm->DestroyFile(fname);

	return rc;
}

t_rc REM_RecordFileManager::OpenRecordFile(const char * fname, REM_RecordFileHandle &rfh) {
	t_rc rc;
	STORM_PageHandle ph;
	char * pData;

	if (rfh.isOpen) this->CloseRecordFile(rfh);

	rc = this->sm->OpenFile(fname, rfh.fh);
	if (rc != OK) return rc;

	rc = rfh.fh.GetFirstPage(ph);
	if (rc != OK) return rc;

	rc = ph.GetPageID(rfh.fInfoPageID);
	if (rc != OK) return rc;

	rc = ph.GetDataPtr(&pData);
	if (rc != OK) return rc;

	memcpy(&rfh.fInfo,&pData[0],sizeof(RecordFileInfo));

	rfh.isOpen = true;

	return (OK);
}

t_rc REM_RecordFileManager::CloseRecordFile(REM_RecordFileHandle &rfh) {

	if (!rfh.isOpen) return (REM_FILEISNOTOPEN);

	STORM_PageHandle ph;
	char * pData;
	t_rc rc;

	rc = rfh.fh.GetPage(rfh.fInfoPageID,ph);
	if (rc != OK) return rc;

	rc = ph.GetDataPtr(&pData);
	if (rc != OK) return rc;

	memcpy(&pData[0],&rfh.fInfo,sizeof(RecordFileInfo));

	rc = rfh.fh.MarkPageDirty(rfh.fInfoPageID);
	if (rc != OK) return rc;

	rc = rfh.fh.FlushAllPages();
	if (rc != OK) return rc;

	rc = rfh.fh.UnpinPage(rfh.fInfoPageID);
	if (rc != OK) return rc;

	rc = this->sm->CloseFile(rfh.fh);
	if (rc != OK) return rc;

	rfh.isOpen = false;

	return (OK);
}