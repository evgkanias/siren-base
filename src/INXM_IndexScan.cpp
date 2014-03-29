#include "INXM_IndexScan.h"

INXM_IndexScan::INXM_IndexScan(){
	this->isOpen = false;
	this->compOp = NO_OP;
	this->value = NULL;
	this->ih = NULL;
}

INXM_IndexScan::~INXM_IndexScan(){ }

t_rc INXM_IndexScan::OpenIndexScan(INXM_IndexHandle &ih, t_compOp compOp, void * value){
	t_rc rc;
	if (this->isOpen) return (INXM_FILESCANISOPEN);

	this->ih = &ih;
	this->fh = &ih.fh;
	this->compOp = compOp;
	this->fileHeader = ih.fileHeader;
	
	//this->value = value;
	int length = this->fileHeader.attrLength;
	this->value = malloc(length);
	memcpy(this->value, value, length);

	this->isOpen = true;

	// Βρίσκουμε την σελίδα/φύλλο που βρίσκεται η τιμή value
	int leafPageID = this->ih->searchRightPage( fileHeader.rootPage , this->value);

	// Διαβάζουμε τον header αυτής της σελίδας
	STORM_PageHandle leafPageHandle;
	char* hData;

	rc = this->ih->fh.GetPage(leafPageID, leafPageHandle);	
	if (rc != OK) return rc;

	rc = leafPageHandle.GetDataPtr(&hData);
	if (rc != OK) return rc;

	NodeHeader leafPageHeader;
	memcpy(&leafPageHeader, &hData[0], sizeof(NodeHeader));		// o header διαβάστηκε

	// Ανάτρεξη στα φύλλα του δέντρου για να βρώ την ακριβή θέση της τιμής value
	// ή της αμέσως μεγαλύτερης
	bool found = false;
	int slot;

	for(slot = 1; slot <= leafPageHeader.numberOfItems; slot++){
		 
		char* currentData = new char[this->ih->getLeafItemSize()];
		memcpy(currentData, &hData[sizeof(NodeHeader) + (slot-1)*this->ih->getLeafItemSize()], this->ih->getLeafItemSize());
		
		LeafItem currentItem;
		currentItem = this->ih->getLeafItemStruct(currentData);

		if (this->ih->keyCompare( currentItem.key , this->value) == 0) {
			this->cRecord.SetPageID(leafPageID);
			this->cRecord.SetSlot(slot);
			found = true;
			this->occasion = 1;
			break;
		} else if (this->ih->keyCompare( currentItem.key, this->value) > 0){
			this->cRecord.SetPageID(leafPageID);
			this->cRecord.SetSlot(slot);
			found = true;
			this->occasion = 2;
			break;
		} else {
			this->cRecord.SetPageID(leafPageID);
			this->cRecord.SetSlot(slot);
			found = true;
			this->occasion = 4;
		}
	}

	if(found){
		return (OK);
	} else {
		return (INXM_EOF);
	}
	
}

t_rc INXM_IndexScan::CloseIndexScan() {

	if (!this->isOpen)  return INXM_FILESCANISNOTOPEN;
	
	this->compOp = NO_OP;
	this->ih = NULL;
	this->value = NULL;

	this->isOpen = false;

	return(OK);
}

t_rc INXM_IndexScan::GetNextEntry(REM_RecordID &rid){
	t_rc rc;
	if (!this->isOpen) return (INXM_FILESCANISNOTOPEN);

	char * pData = new char[this->ih->getLeafItemSize()];
	LeafItem item;

	// To rid που είναι είτε ακριβώς ίσο με το value, είτε είναι το αμέσως μεγαλύτερο
	int fPage, fSlot;
	this->cRecord.GetPageID(fPage);
	this->cRecord.GetSlot(fSlot);

	// Διαβάζω τη σελίδα που βρέθηκε
	STORM_PageHandle ph;
	rc = this->ih->fh.GetPage(fPage,ph);
	if (rc != OK) return rc;

	// Διαβάζω τον header της
	NodeHeader fPageHeader;
	char* hData;

	rc = ph.GetDataPtr(&hData);
	if (rc != OK) return rc;

	memcpy(&fPageHeader, &hData[0], sizeof(NodeHeader));
	int curPage = fPage;

	switch (this->compOp) {
		case GE_OP:
			for (int slot = fSlot; slot <=  fPageHeader.numberOfItems;) {				
				if(this->occasion == 4) {
					return (INXM_NOMOREVALUES);
				} else if(this->occasion == 3){					
					if ((slot+1) > fPageHeader.numberOfItems ) { return (INXM_EOF); }
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot+1);

					memcpy(pData,&hData[sizeof(NodeHeader) + fSlot*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					return (OK);
				} else if(this->occasion == 1){
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot);
					
					memcpy(pData,&hData[sizeof(NodeHeader) + (fSlot-1)*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					this->occasion = 3;
					return (OK);
				} else if (this->occasion == 2) {
					if ((slot+1) > fPageHeader.numberOfItems ) { return (INXM_EOF); }
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot);
		
					memcpy(pData,&hData[sizeof(NodeHeader) + (fSlot-1)*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					this->occasion = 3;
					return (OK);
				}
				break;
			}
			break;
		case LE_OP:
			for (int slot = fSlot; slot >= 1;) {				
				if(this->occasion == 4) {
					return (INXM_NOMOREVALUES);
				} else if(this->occasion == 3){					
					if ((slot-1) == 0 ) { return (INXM_EOF); }
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot-1);

					memcpy(pData,&hData[sizeof(NodeHeader) + (fSlot-2)*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					return (OK);
				} else if(this->occasion == 1){
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot);
					
					memcpy(pData,&hData[sizeof(NodeHeader) + (fSlot-1)*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					this->occasion = 3;
					return (OK);
				} else if (this->occasion == 2) {
					if ((slot-1) == 0 ) { return (INXM_EOF); }
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot-1);
					
					memcpy(pData,&hData[sizeof(NodeHeader) + (fSlot-2)*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					this->occasion = 3;
					return (OK);
				}
				break;
			}
			break;
		case GT_OP:
			for (int slot = fSlot; slot <=  fPageHeader.numberOfItems;) {				
				if(this->occasion == 4) {
					return (INXM_NOMOREVALUES);
				} else if(this->occasion == 3){					
					if ((slot+1) > fPageHeader.numberOfItems ) { return (INXM_EOF); }
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot+1);

					memcpy(pData,&hData[sizeof(NodeHeader) + fSlot*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					return (OK);
				} else if(this->occasion == 1){
					if ((slot+1) > fPageHeader.numberOfItems ) { return (INXM_EOF); }
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot+1);
					
					memcpy(pData,&hData[sizeof(NodeHeader) + fSlot*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					this->occasion = 3;
					return (OK);
				} else if (this->occasion == 2) {
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot);
		
					memcpy(pData,&hData[sizeof(NodeHeader) + (fSlot-1)*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					this->occasion = 3;
					return (OK);
				}
				break;
			}
			break;
///////////////////////////////////////////////////////////////////////////////////////////////////////
		case LT_OP:
			for (int slot = fSlot; slot >= 1;) {				
				if(this->occasion == 4) {
					if ((slot-1) == 0 ) { return (INXM_EOF); }
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot);

					memcpy(pData,&hData[sizeof(NodeHeader) + (fSlot-1)*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					this->occasion = 3;
					return (OK);
				} else if(this->occasion == 3){					
					if ((slot-1) == 0 ) { return (INXM_EOF); }
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot-1);

					memcpy(pData,&hData[sizeof(NodeHeader) + (fSlot-2)*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					return (OK);
				} else if(this->occasion == 1){
					if ((slot-1) == 0 ) { return (INXM_EOF); }
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot-1);
					
					memcpy(pData,&hData[sizeof(NodeHeader) + (fSlot-2)*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					this->occasion = 3;
					return (OK);
				} else if (this->occasion == 2) {
					if ((slot-1) == 0 ) { return (INXM_EOF); }
					this->cRecord.SetPageID(fPage);
					this->cRecord.SetSlot(fSlot-1);
		
					memcpy(pData,&hData[sizeof(NodeHeader) + (fSlot-2)*(this->ih->getLeafItemSize())],this->ih->getLeafItemSize());
					item = this->ih->getLeafItemStruct(pData);

					int t_pid, t_slot;

					rc = item.rid.GetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = item.rid.GetSlot(t_slot);
					if (rc != OK) return rc;

					rc = rid.SetPageID(t_pid);
					if (rc != OK) return rc;
					
					rc = rid.SetSlot(t_slot);
					if (rc != OK) return rc;

					this->occasion = 3;
					return (OK);
				}
				break;
			}
			break;
		}
}
