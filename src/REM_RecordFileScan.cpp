#include "REM_RecordFileScan.h"

REM_RecordFileScan::REM_RecordFileScan() {
	this->isOpen = false;
	this->compOp = NO_OP;
	this->value = NULL;
	this->rfh = NULL;
}

REM_RecordFileScan::~REM_RecordFileScan() {}

t_rc REM_RecordFileScan::OpenRecordScan(REM_RecordFileHandle &rfh, t_attrType attrType,
	int	attrLength, int	attrOffset, t_compOp compOp, void * value) {
		
		t_rc rc;
		if (this->isOpen) return (REM_FILESCANISOPEN);

		this->rfh = &rfh;
		this->attrType = attrType;
		this->attrLength = attrLength;
		this->attrOffset = attrOffset;
		this->compOp = compOp;
		this->value = malloc(attrLength);
		memcpy(this->value,value,attrLength);

		this->isOpen = true;

		STORM_PageHandle ph;
		rc = this->rfh->fh.GetNextPage(this->rfh->fInfoPageID,ph);
		if (rc != OK) return rc;
		
		int pageID;
		rc = ph.GetPageID(pageID);
		if (rc != OK) return rc;

		bool done = false;
		for (int pid = pageID; ph.GetPageID(pid) == OK; this->rfh->fh.GetNextPage(pid,ph)) {
			char * pData;
			rc = ph.GetDataPtr(&pData);
			if (rc != OK) return rc;
			RecordPageInfo pInfo;
			memcpy(&pInfo,pData,sizeof(RecordPageInfo));
			
			for (int slot = 1; slot <= pInfo.recs; slot++) {
				if (pInfo.sStatus.at(slot-1)) {
					this->cRecord.SetPageID(pid);
					this->cRecord.SetSlot(slot);
					
					done = true;
					break;
				}
			}
			if (done) break;
			this->rfh->fh.GetNextPage(pid,ph);
		}

		return (OK);
}

t_rc REM_RecordFileScan::GetNextRecord(REM_RecordHandle &rh) {
	
	t_rc rc;
	if (!this->isOpen) return (REM_FILESCANISNOTOPEN);
	if ((rc = this->rfh->ReadRecord(this->cRecord,rh)) != OK) 
		if ((rc != REM_NOTVALIDRECORDTOREAD)) return rc;
	
	int fPage, fSlot;
	this->cRecord.GetPageID(fPage);
	this->cRecord.GetSlot(fSlot);

	bool done = false;
	STORM_PageHandle ph;
	rc = this->rfh->fh.GetPage(fPage,ph);
	if (rc != OK) return rc;

	int int1, int2;
	memcpy(&int2,this->value,sizeof(int));
	char * str1 = new char[this->attrLength];
	char * str2 = new char[this->attrLength];
	memcpy(str2,this->value,this->attrLength);

	int curPage = fPage;
	do {
		
		char * pData;
		rc = ph.GetPageID(curPage);
		if (rc != OK) return rc;

		rc = ph.GetDataPtr(&pData);
		if (rc != OK) return rc;
		
		RecordPageInfo pInfo;
		memcpy(&pInfo,&pData[0],sizeof(RecordPageInfo));

		if (pInfo.alocRecs == 0) continue;

		for (int slot = (curPage == fPage) * (fSlot - 1) + 1; slot <= pInfo.recs; slot++) {
						// Αν δεν έχουμε αλλάξει σελίδα ξεκινάμε από το slot που σταματήσαμε
			this->cRecord.SetPageID(curPage);
			this->cRecord.SetSlot(slot);
			if (pInfo.sStatus.at(slot-1)) {


				rc = this->rfh->ReadRecord(cRecord,rh);
				if (rc != OK) return rc;

				char * rData;			// Το περιεχόμενο της εγγραφής
				rc = rh.GetData(rData);
				if (rc != OK) return rc;

				switch (this->compOp) {
				case EQ_OP:				// equal
					switch (this->attrType) {
					case TYPE_INT:
						memcpy(&int1,&rData[this->attrOffset],sizeof(int));

						done = (int1 == int2);
						break;
					case TYPE_STRING:
						memcpy(str1,&rData[this->attrOffset],this->attrLength);
						
						done = (strncmp(str1,str2,this->attrLength) == 0);
						break;
					default:
						return (REM_WRONGTYPE);
						break;
					}
					break;
				case LT_OP:				// less than
					switch (this->attrType) {
					case TYPE_INT:
						memcpy(&int1,&rData[this->attrOffset],sizeof(int));

						done = (int1 < int2);
						break;
					case TYPE_STRING:
						memcpy(str1,&rData[this->attrOffset],this->attrLength);

						done = (strncmp(str1,str2,this->attrLength) < 0);
						break;
					default:
						return (REM_WRONGTYPE);
						break;
					}
					break;
				case GT_OP:				//greater than
					switch (this->attrType) {
					case TYPE_INT:
						memcpy(&int1,&rData[this->attrOffset],sizeof(int));

						done = (int1 > int2);
						break;
					case TYPE_STRING:
						memcpy(str1,&rData[this->attrOffset],this->attrLength);

						done = (strncmp(str1,str2,this->attrLength) > 0);
						break;
					default:
						return (REM_WRONGTYPE);
						break;
					}
					break;
				case NE_OP:				// not equal
					switch (this->attrType) {
					case TYPE_INT:
						memcpy(&int1,&rData[this->attrOffset],sizeof(int));

						done = !(int1 == int2);
						break;
					case TYPE_STRING:
						memcpy(str1,&rData[this->attrOffset],this->attrLength);

						done = !(strncmp(str1,str2,this->attrLength) == 0);
						break;
					default:
						return (REM_WRONGTYPE);
						break;
					}
					break;
				case LE_OP:				// less than or equal
					switch (this->attrType) {
					case TYPE_INT:
						memcpy(&int1,&rData[this->attrOffset],sizeof(int));

						done = (int1 <= int2);
						break;
					case TYPE_STRING:
						memcpy(str1,&rData[this->attrOffset],this->attrLength);

						done = (strncmp(str1,str2,this->attrLength) <= 0);
						break;
					default:
						return (REM_WRONGTYPE);
						break;
					}
					break;
				case GE_OP:				// greater than or equal
					switch (this->attrType) {
					case TYPE_INT:
						memcpy(&int1,&rData[this->attrOffset],sizeof(int));

						done = (int1 >= int2);
						break;
					case TYPE_STRING:
						memcpy(str1,&rData[this->attrOffset],this->attrLength);

						done = (strncmp(str1,str2,this->attrLength) >= 0);
						break;
					default:
						return (REM_WRONGTYPE);
						break;
					}
					break;
				case NO_OP:				// no comparison. It's used when value is null
					done = true;
					break;
				default:
					return (REM_WRONGTYPE);
					break;
				}
				
			}
			if (done) break;
		}
		if (done) break;

	} while (this->rfh->fh.GetNextPage(curPage,ph) == OK);

	int nPageID, nSlot;
	this->cRecord.GetPageID(nPageID);
	this->cRecord.GetSlot(nSlot);

	nSlot++;
	if (nSlot <= this->rfh->fInfo.rpp) this->cRecord.SetSlot(nSlot);
	else { 
		rc = this->rfh->fh.GetNextPage(nPageID,ph);
		if (rc != OK) return rc;

		rc = ph.GetPageID(nPageID);
		if (rc != OK) return rc;

		this->cRecord.SetPageID(nPageID);
		this->cRecord.SetSlot(1);
	}

	if (!done) return (REM_FILESCANNOTFINDRESULTS);

	return (OK);
}

t_rc REM_RecordFileScan::CloseRecordScan() {
	
	if (!this->isOpen) return (REM_FILESCANISNOTOPEN);

	this->attrLength = 0;
	this->attrOffset = 0;
	this->compOp = NO_OP;
	this->rfh = NULL;
	this->value = NULL;

	this->isOpen = false;

	return (OK);
}