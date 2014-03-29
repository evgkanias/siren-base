#include <iostream>
#include "REM_RecordFileHandle.h"

REM_RecordFileHandle::REM_RecordFileHandle() {
	this->isOpen = false;
}

REM_RecordFileHandle::~REM_RecordFileHandle() {}

t_rc REM_RecordFileHandle::ReadRecord (const REM_RecordID &rid, REM_RecordHandle &rh) const {

	if (!isOpen) return (REM_FILEISNOTOPEN);

	t_rc rc;
	int pageID, slot;
	STORM_PageHandle ph;

	rc = rid.GetPageID(pageID);
	if (rc != OK) return rc;

	rc = rid.GetSlot(slot);
	if (rc != OK) return rc;

	STORM_FileHandle * sfh = new STORM_FileHandle();
	* sfh = this->fh;
	rc = sfh->GetPage(pageID, ph);
	if (rc != OK) return rc;
	
	char * data;

	rc = ph.GetDataPtr(&data);
	if (rc != OK) return rc;
	
	RecordPageInfo pInfo;
	memcpy(&pInfo, &data[0], sizeof(RecordPageInfo));
	if (!pInfo.sStatus.at(slot-1)) return (REM_NOTVALIDRECORDTOREAD);
	
	rh.rid = rid;

	rh.pData = new char[fInfo.rs];
	memcpy(rh.pData, &data[sizeof(RecordPageInfo) + fInfo.rs * (slot - 1) + 1], fInfo.rs);

	return (OK);
}

t_rc REM_RecordFileHandle::InsertRecord (const char * pData, REM_RecordID &rid) {

	if(!isOpen) return (REM_FILEISNOTOPEN);

	t_rc rc;
	STORM_PageHandle ph;
	int pageID, slot;
	
	bool done = false;
	RecordPageInfo pInfo;
	int currentPageID = this->fInfoPageID;

	char * data;
	while ((rc = this->fh.GetNextPage(currentPageID,ph)) == OK) {
		currentPageID++;

		rc = ph.GetDataPtr(&data);
		if (rc != OK) return (REM_GETDATAERROR);

		rc = ph.GetPageID(pageID);
		if (rc != OK) return (REM_PAGEIDENTIFIERRETURNERROR);

		memcpy(&pInfo,&data[0],sizeof(RecordPageInfo));

		if (pInfo.alocRecs < pInfo.recs) {
			for (int i = 0; i < pInfo.recs; i++) {
				if (pInfo.sStatus.at(i) == false) {
					rc = rid.SetPageID(pageID);
					if (rc != OK) return (REM_PAGEIDENTIFIERSETERROR);

					rc = rid.SetSlot(i + 1);
					if (rc != OK) return (REM_SLOTSETERROR);

					pInfo.sStatus.set(i, true);
					pInfo.alocRecs++;
					memcpy(&data[0],&pInfo,sizeof(RecordPageInfo));
					
					rc = this->fh.MarkPageDirty(pageID);
					if (rc != OK) { return rc; }

					done = true;
					break;
				}
			}
		} else {
			rc = this->fh.UnpinPage(pageID);
			if (rc != OK) return rc;
		}
		if (done)
			break;
	}

	if (!done) {
		rc = this->fh.ReservePage(ph);
		if (rc != OK) return (REM_RESERVEPAGEERROR);

		pInfo = this->InitRecordPageInfo(fInfo.rpp);
		pInfo.alocRecs++;
		pInfo.sStatus.set(0, true);

		rc = ph.GetDataPtr(&data);
		if (rc != OK) return (REM_GETDATAERROR);

		memcpy(&data[0],&pInfo,sizeof(RecordPageInfo));
		
		rc = ph.GetPageID(pageID);
		if (rc != OK) return (REM_PAGEIDENTIFIERRETURNERROR);

		rc = rid.SetPageID(pageID);
		if (rc != OK) return (REM_PAGEIDENTIFIERSETERROR);

		rc = rid.SetSlot(1);
		if (rc != OK) return (REM_SLOTSETERROR);

		rc = this->fh.MarkPageDirty(pageID);
		if (rc != OK) return rc;

		done = true;
	}

	rc = rid.GetPageID(pageID);
	if (rc != OK) return (REM_PAGEIDENTIFIERRETURNERROR);

	rc = rid.GetSlot(slot);
	if (rc != OK) return (REM_SLOTRETURNERROR);

	rc = this->fh.GetPage(pageID,ph);
	if (rc != OK) return (REM_GETPAGEERROR);

	char * nData;
	rc = ph.GetDataPtr(&nData);
	if (rc != OK) return (REM_GETDATAERROR);

	memcpy(&nData[sizeof(RecordPageInfo) + (slot - 1) * fInfo.rs + 1], pData, fInfo.rs);
	this->fInfo.nr++;

	rc = this->fh.MarkPageDirty(pageID);
	if (rc != OK) return rc;

	rc = this->fh.FlushPage(pageID);
	if (rc != OK) return rc;

	return (OK);
}

t_rc REM_RecordFileHandle::DeleteRecord (const REM_RecordID &rid) {
	t_rc rc;
	STORM_PageHandle ph;
	int pageID, slot;

	rc = rid.GetPageID(pageID);
	if (rc != OK) return rc;

	rc = rid.GetSlot(slot);
	if (rc != OK) return rc;

	rc = this->fh.GetPage(pageID,ph);
	if (rc != OK) return rc;

	char * pData;
	rc = ph.GetDataPtr(&pData);
	if (rc != OK) return rc;

	RecordPageInfo pInfo;
	memcpy(&pInfo,pData,sizeof(RecordPageInfo));

	pInfo.alocRecs--;
	pInfo.sStatus.set(slot-1, false);
	this->fInfo.nr--;

	if (pInfo.alocRecs > 0) {
		memcpy(&pData[0],&pInfo,sizeof(RecordPageInfo));

		rc = this->fh.MarkPageDirty(pageID);
		if (rc != OK) return rc;

		rc = this->fh.FlushPage(pageID);
		if (rc != OK) return rc;
	} else {
		rc = this->fh.UnpinPage(pageID);
		if (rc != OK) return rc;

		rc = this->fh.ReleasePage(pageID);
		if (rc != OK) return rc;
	}

	return (OK);
}

t_rc REM_RecordFileHandle::UpdateRecord (const REM_RecordHandle &rh) {
	t_rc rc;
	REM_RecordID rid;
	int pageID, slot;

	rc = rh.GetRecordID(rid);
	if (rc != OK) return rc;

	rc = rid.GetPageID(pageID);
	if (rc != OK) return rc;

	rc = rid.GetSlot(slot);
	if (rc != OK) return rc;

	STORM_PageHandle ph;
	rc = this->fh.GetPage(pageID,ph);
	if (rc != OK) return rc;

	char * pData;
	rc = ph.GetDataPtr(&pData);
	if (rc != OK) return rc;

	memcpy(&pData[sizeof(RecordPageInfo) + (slot - 1) * fInfo.rs + 1],rh.pData,fInfo.rs);

	rc = this->fh.MarkPageDirty(pageID);
	if (rc != OK) return rc;

	rc = this->fh.FlushPage(pageID);
	if (rc != OK) return rc;

	rc = this->fh.UnpinPage(pageID);
	if (rc != OK) return rc;

	return (OK);
}

t_rc REM_RecordFileHandle::FlushPages () const {
	t_rc rc;
	rc = this->fh.FlushAllPages();
	if (rc != OK) return rc;

	return (OK);
}

RecordPageInfo REM_RecordFileHandle::InitRecordPageInfo(int recs) {
	RecordPageInfo pInfo;
	pInfo.recs = recs;
	pInfo.alocRecs = 0;

	for (int i = 0; i < recs; i++)
		pInfo.sStatus.set(i,false);

	return pInfo;
}