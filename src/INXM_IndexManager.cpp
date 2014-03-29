#include "INXM_IndexManager.h"

INXM_IndexManager::INXM_IndexManager(STORM_StorageManager *sm){
	this->sm = sm;
}

INXM_IndexManager::~INXM_IndexManager(){
}



t_rc INXM_IndexManager::CreateIndex(const char *fname, int indexNo, t_attrType attrType, int attrLength){

	std::string indx_Name("");
	indx_Name.append(fname);
	indx_Name.append(".");
	char indx_No_str[4];
	indx_Name.append(_itoa(indexNo,indx_No_str,10));

	errno = 0;
	t_rc rc = this->sm->CreateFile(indx_Name.c_str());
	if (rc != OK) { return rc; }

	STORM_FileHandle stormFileHandle;
	rc = this->sm->OpenFile(indx_Name.c_str(), stormFileHandle);
	if (rc != OK) { return rc; } 

	STORM_PageHandle stormPageHandle;
	rc = stormFileHandle.ReservePage(stormPageHandle);
	if (rc != OK) { return rc; }

	rc = stormPageHandle.GetPageID(this->fileHeaderPageID);
	if (rc != OK) { return rc; }

	char * pData;

	rc = stormPageHandle.GetDataPtr(&pData);
	if (rc != OK) { return rc; }	

	IndexFileHeader fileHeader = { 0, 0, 0, attrType, attrLength };
	memcpy(&pData[0], &fileHeader, sizeof(IndexFileHeader));

	rc = stormFileHandle.MarkPageDirty(fileHeaderPageID);
	if (rc != OK) { return rc; }

	rc = stormFileHandle.FlushPage(fileHeaderPageID);
	if (rc != OK) { return rc; }

	rc = stormFileHandle.UnpinPage(fileHeaderPageID);
	if (rc != OK) { return rc; }

	rc = this->sm->CloseFile(stormFileHandle);
	if (rc != OK) { return rc; }

	return (OK);
}

t_rc INXM_IndexManager::DestroyIndex(const char *fname, int indexNo) {

	std::string indx_Name("");
	indx_Name.append(fname);
	indx_Name.append(".");

	char inx_No_str[4];
	indx_Name.append(_itoa(indexNo, inx_No_str,10));

	return this->sm->DestroyFile(indx_Name.c_str());
	return (OK);


}

t_rc INXM_IndexManager::OpenIndex(const char *fname, int indexNo, INXM_IndexHandle &ih){
	
	std::string buffer("");
	buffer.append(fname);
	buffer.append(".");

	char indx_No_str[4];
	buffer.append(_itoa(indexNo,indx_No_str,10));
	
	t_rc rc = this->sm->OpenFile(buffer.c_str(), ih.fh);
	if (rc != OK) { return rc; }
	
	
	STORM_PageHandle pageHandle;
	rc = ih.fh.GetFirstPage(pageHandle);
	if (rc != OK) { return rc; }
	
	rc = pageHandle.GetPageID(ih.fileHeaderPageID);
	if (rc != OK) return rc;

	/* Get data from first page. Those first data are the INXM File Header. */
	char *headerData;
	rc = pageHandle.GetDataPtr(&headerData);
	if (rc != OK) { return rc; }

	memcpy(&ih.fileHeader, &headerData[0], sizeof(IndexFileHeader));
		
	ih.isOpen = true;
	
	return(OK);
}

t_rc INXM_IndexManager::CloseIndex(INXM_IndexHandle &ih){
	
	if(!ih.isOpen){
		return INXM_INDEXHANDLECLOSED;
	}

	STORM_PageHandle ph;
	char * pData;
	t_rc rc;

	rc = ih.fh.GetPage(ih.fileHeaderPageID,ph);
	if (rc != OK) return rc;

	rc = ph.GetDataPtr(&pData);
	if (rc != OK) return rc;

	memcpy(&pData[0], &ih.fileHeader, sizeof(IndexFileHeader));

	rc = ih.fh.MarkPageDirty(ih.fileHeaderPageID);
	if (rc != OK) return rc;

	rc = ih.fh.FlushAllPages();
	if (rc != OK) return rc;

	rc = ih.fh.UnpinPage(ih.fileHeaderPageID);
	if (rc != OK) return rc;

	rc = this->sm->CloseFile(ih.fh);
	if (rc != OK) return rc;

	ih.isOpen = false;
	
	return(OK);
}
