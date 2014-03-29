#include "INXM_IndexHandle.h"

INXM_IndexHandle::INXM_IndexHandle() {
	this->isOpen = false;
}

INXM_IndexHandle::~INXM_IndexHandle() {}

int INXM_IndexHandle::keyCompare(void *key1,void *key2) {
	
	if (this->fileHeader.attrType == TYPE_STRING) {
		return strcmp((char *)key1, (char *)key2);
	} else {
		int kleidi1 = *(int *)key1;
		int kleidi2 = *(int *)key2;

		return kleidi1 - kleidi2;
	}
}

// ���������� �� ������� ��� ����� InterItem
int INXM_IndexHandle::getInterItemSize(){
	return fileHeader.attrLength + 2*sizeof(int);
}

// ���������� �� ������� ��� ����� LeafItem
int INXM_IndexHandle::getLeafItemSize(){
	return fileHeader.attrLength + sizeof(REM_RecordID);
}

char* INXM_IndexHandle::getInterItemData(InterItem iItem){
	char* iData = new char[this->getInterItemSize()];
	int pos = 0;

	memcpy(&iData[pos], iItem.key, fileHeader.attrLength);
	pos += fileHeader.attrLength;
	memcpy(&iData[pos], &iItem.leftChild, sizeof(int));
	pos += sizeof(int);
	memcpy(&iData[pos], &iItem.rightChild, sizeof(int));

	return iData;
}

char* INXM_IndexHandle::getLeafItemData(LeafItem lItem){
	char* iData = new char[this->getLeafItemSize()];
	int pos = 0;

	memcpy(&iData[pos], lItem.key, fileHeader.attrLength);
	pos += fileHeader.attrLength;
	memcpy(&iData[pos], &lItem.rid, sizeof(REM_RecordID));

	return iData;
}

// �������� �� struct ���� Inter Item
InterItem INXM_IndexHandle::getInterItemStruct(char* data) {
	InterItem item;
	int pos = 0;

	item.key = new char [fileHeader.attrLength];
	memcpy(item.key, &data[pos], fileHeader.attrLength);
	pos += fileHeader.attrLength;
	memcpy(&item.leftChild,  &data[pos], sizeof(int));
	pos += sizeof(int);
	memcpy(&item.rightChild, &data[pos], sizeof(int));

	return item;
}

// �������� �� struct ���� Leaf Item
LeafItem INXM_IndexHandle::getLeafItemStruct(char* data) {
	LeafItem item;
	int pos = 0;
	
	item.key = new char [fileHeader.attrLength];
	memcpy(item.key, &data[pos], fileHeader.attrLength);
	pos += fileHeader.attrLength;
	memcpy(&item.rid, &data[pos], sizeof(REM_RecordID));

	return item;
}

bool INXM_IndexHandle::checkInterNodeForSpace(NodeHeader nh){
	if ( ( PAGE_SIZE - (sizeof(NodeHeader) + nh.numberOfItems * this->getInterItemSize())) > this->getInterItemSize() ) {
		return true;
	} else {
		return false;
	}
}

bool INXM_IndexHandle::checkLeafNodeForSpace(NodeHeader nh){
	if ( ( PAGE_SIZE - (sizeof(NodeHeader) + nh.numberOfItems * this->getLeafItemSize())) > this->getLeafItemSize() ) {
		return true;
	} else {
		return false;
	}
}


// ����������� ������ (������) � ������ ���� �������
// ���������� ���� �� id ��� ������� ��� ������
t_rc INXM_IndexHandle::splitLeafNode(int pageToSplitID){
	t_rc rc;
	
	// ������� ��� Node Header ��� �������
	STORM_PageHandle pageHandle;
	char* data;

	rc = fh.GetPage(pageToSplitID, pageHandle);
	if (rc != OK) return rc;

	rc = pageHandle.GetDataPtr(&data);
	if (rc != OK) return rc;

	NodeHeader nh;		// o header ��� ������� ���� ��������
	memcpy(&nh, &data[0], sizeof(NodeHeader));

	// ���������� ���� leaf node
	// �������� �������
	this->fh.ReservePage(pageHandle);
	if (rc != OK) return rc;

	int newLeafPageID;
	rc = pageHandle.GetPageID(newLeafPageID);
	if (rc != OK) { return rc; }
	
	char* newLeafData;
	rc = pageHandle.GetDataPtr(&newLeafData);
	if (rc != OK) { return rc; }
	
	// ������������ ��� ���� node header
	NodeHeader nhNew;
	nhNew.leaf = true;
	nhNew.parent = nh.parent;
	for (int i = 0; i < 1200; i++)
		nhNew.sStatus.set(i,false);
	
	// ���������� ��� ����� ������������ ��� ��� ������ �����
	// ���� ���������
	for (int i = nh.numberOfItems/2; i < nh.numberOfItems; i++) {
		memcpy(
			&newLeafData[sizeof(NodeHeader)+(i-nh.numberOfItems/2)*this->getLeafItemSize()],
			&data[sizeof(NodeHeader)+i*this->getLeafItemSize()],
			this->getLeafItemSize());
		
		// ������ ������� ������������ ���� ���� �����
		nh.numberOfItems--;
		nh.sStatus.set(i, false);

		// ������ ������� ������������ ���� ���� �����
		nhNew.numberOfItems++;
		nhNew.sStatus.set(i-nh.numberOfItems/2, true);
		
	}	
	// ������������
	nhNew.right = nh.right;
	nh.right = newLeafPageID;
	nhNew.left = pageToSplitID;
	

	// ������������ ��� ������ ���������� ������
	NodeHeader nhParent;

	// ������� �� ����� item ��� �� ��� ����� ����� ��� ����� �����������
	char* firstItemData = new char[this->getLeafItemSize()];
	memcpy(&firstItemData, &newLeafData[sizeof(NodeHeader)], this->getLeafItemSize());

	LeafItem firstLeftItem = this->getLeafItemStruct(firstItemData);

	// ���������� ��� item ��� �� ���������� ���� ����� ������
	InterItem iItemNew;
	iItemNew.key = firstLeftItem.key;
	iItemNew.rightChild = newLeafPageID;
	iItemNew.leftChild = pageToSplitID;

	if ( nh.parent < 0) {		// ��������� ��� � ���� �������� ������ ��� ���� ������, ��� ����� ����
		// ���������� inter node
		// �������� �������
		this->fh.ReservePage(pageHandle);
		if (rc != OK) return rc;

		int interPageID;
		rc = pageHandle.GetPageID(interPageID);
		if (rc != OK) { return rc; }
	
		char* interData;
		rc = pageHandle.GetDataPtr(&interData);
		if (rc != OK) { return rc; }

		nh.parent = interPageID;
		nhNew.parent = interPageID;
		for (int i = 0; i < 1200; i++)
			nhParent.sStatus.set(i,false);
		
		nhParent.parent = -1;
		nhParent.sStatus.set(0,true);
		nhParent.numberOfItems++;
		nhParent.leaf = false;
		nhParent.left = -1;
		nhParent.right = -1;

		// ������� ��� interItem
		memcpy(&interData[sizeof(NodeHeader)], this->getInterItemData(iItemNew), this->getInterItemSize());

		// ��������� ��� header
		memcpy(&interData[0], &nhParent, sizeof(NodeHeader));

		rc = this->fh.MarkPageDirty(interPageID);
		if (rc != OK) return rc;

		rc = this->fh.FlushPage(interPageID);
		if (rc != OK) return rc;

		rc = this->fh.UnpinPage(interPageID);
		if (rc != OK) return rc;

	} else {		// ��������� ��� � ���� �������� ������ ���� ������, ��� ��� ����� ����
		
		// ������� ��� header ��� ��� ��������� ������
		int parentPageID = nh.parent;	// ���� ������� ��� �������
		char* parentData;

		rc = fh.GetPage(parentPageID, pageHandle);
		if (rc != OK) return rc;

		rc = pageHandle.GetDataPtr(&parentData);
		if (rc != OK) return rc;

		memcpy(&nhParent, &parentData[0], sizeof(NodeHeader));

		if(checkInterNodeForSpace(nhParent)){		// ��������� ��� � ������� ���� ����
			bool posFound = false;
			InterItem tempItem1 = iItemNew;
			InterItem tempItem2;
			int i;

			// ��������� ��� ������ item ��� ������ �������
			// ���� ������
			for (i = 0; i < nhParent.numberOfItems+1; i++) {
				
				char* currentData = new char[this->getInterItemSize()];
				InterItem currentItem;

				memcpy(currentData, &parentData[sizeof(NodeHeader)+i*this->getInterItemSize()], this->getInterItemSize());
				currentItem = this->getInterItemStruct(currentData);

				if (keyCompare(iItemNew.key, currentItem.key) < 0 ) posFound = true;

				// ������� � ����� ���� ���������� ���� ��� ������ item
				// ��� ���� �����
				if (posFound) {
					tempItem2 = currentItem;
					currentItem = tempItem1;
					tempItem1 = tempItem2;

					memcpy(&parentData[sizeof(NodeHeader)+i*this->getInterItemSize()],this->getInterItemData(currentItem),this->getInterItemSize());
				}
			}

			nhParent.numberOfItems++;
			nhParent.sStatus.set(i-1,true);

		} else {	// ��������� ��� � ������� ��� ���� ����
			 splitInterNode(parentPageID); 

		}

		// ������ ��������� ��� header ��� ������
		memcpy(&parentData[0], &nhParent, sizeof(NodeHeader));

		rc = this->fh.MarkPageDirty(parentPageID);
		if (rc != OK) return rc;

		rc = this->fh.FlushPage(parentPageID);
		if (rc != OK) return rc;

		rc = this->fh.UnpinPage(parentPageID);
		if (rc != OK) return rc;

		return (OK);
	
	}
	memcpy(&data[0], &nh, sizeof(NodeHeader));
	memcpy(&newLeafData[0], &nhNew, sizeof(NodeHeader));

	return (OK);
}

// ����������� ���������� ������ � ������ ���� �������
// ���������� ���� �� id ��� ������� ��� ������
t_rc INXM_IndexHandle::splitInterNode(int pageToSplitID){
	t_rc rc;
	
	// ������� ��� Inter Node Header ��� �������
	STORM_PageHandle pageHandle;
	char* data;

	rc = fh.GetPage(pageToSplitID, pageHandle);
	if (rc != OK) return rc;

	rc = pageHandle.GetDataPtr(&data);
	if (rc != OK) return rc;

	NodeHeader nh;		// o header ��� ������� ���� ��������
	memcpy(&nh, &data[0], sizeof(NodeHeader));

	// ���������� ���� inter node
	// �������� �������
	this->fh.ReservePage(pageHandle);
	if (rc != OK) return rc;

	int newInterPageID;
	rc = pageHandle.GetPageID(newInterPageID);
	if (rc != OK) { return rc; }
	
	char* newInterData;
	rc = pageHandle.GetDataPtr(&newInterData);
	if (rc != OK) { return rc; }
	
	// ������������ ��� ���� inter node header
	NodeHeader nhNew;
	nhNew.leaf = false;
	nhNew.parent = nh.parent;
	nhNew.right = -1;
	nhNew.left = -1;
	for (int i = 0; i < 1200; i++)
		nhNew.sStatus.set(i,false);

	// ���������� ��� ����� ������������ ��� ��� ������ �����
	// ���� ���������
	for (int i = nh.numberOfItems/2; i < nh.numberOfItems; i++) {
		memcpy(
			&newInterData[sizeof(NodeHeader)+(i-nh.numberOfItems/2)*this->getInterItemSize()],
			&data[sizeof(NodeHeader)+i*this->getInterItemSize()],
			this->getInterItemSize());
		
		// ������ ������� ������������ ���� ���� �����
		nh.numberOfItems--;
		nh.sStatus.set(i, false);

		// ������ ������� ������������ ���� ���� �����
		nhNew.numberOfItems++;
		nhNew.sStatus.set(i-nh.numberOfItems/2, true);
	}

	

	// ���������� ��� ������ ���������� ������
	// ���������� ��� header
	NodeHeader nhParent;

	// ������� �� ����� item ��� �� ��� ��������� ����� ��� ����� �����������
	char* firstItemData = new char[this->getInterItemSize()];
	memcpy(&firstItemData, &newInterData[sizeof(NodeHeader)], this->getInterItemSize());

	InterItem firstLeftItem = this->getInterItemStruct(firstItemData);

	// ������������ ��� item ��� �� ���� ���� ����� ������
	InterItem iItemNew;
	iItemNew.key = firstLeftItem.key;
	iItemNew.rightChild = newInterPageID;
	iItemNew.leftChild = pageToSplitID;

	if ( nh.parent > 0) {		// ��������� ��� � ���� �������� ������ ���� ������, ��� ��� ����� ����
		int parentPageID = nh.parent;
		char* parentData;

		rc = fh.GetPage(parentPageID, pageHandle);
		if (rc != OK) return rc;

		rc = pageHandle.GetDataPtr(&parentData);
		if (rc != OK) return rc;

		memcpy(&nhParent, &parentData[0], sizeof(NodeHeader));

		if(checkInterNodeForSpace(nhParent)){		// ��������� ��� � ������� ���� ����
			bool posFound = false;
			InterItem tempItem1 = iItemNew;
			InterItem tempItem2;
			int i;

			// ��������� ��� ������ item ��� ������ �������
			// ���� ������
			for (i = 0; i < nhParent.numberOfItems+1; i++) {
				
				char* currentData = new char[this->getInterItemSize()];
				InterItem currentItem;

				memcpy(currentData, &parentData[sizeof(NodeHeader)+i*this->getInterItemSize()], this->getInterItemSize());
				currentItem = this->getInterItemStruct(currentData);

				if (keyCompare(iItemNew.key, currentItem.key) < 0 ) posFound = true;

				// ������� � ����� ���� ���������� ���� ��� ������ item
				// ��� ���� �����
				if (posFound) {
					tempItem2 = currentItem;
					currentItem = tempItem1;
					tempItem1 = tempItem2;

					memcpy(&parentData[sizeof(NodeHeader)+i*this->getInterItemSize()],this->getInterItemData(currentItem),this->getInterItemSize());
				}
			}

			nhParent.numberOfItems++;
			nhParent.sStatus.set(i-1,true);

		} else {	// ��������� ��� � ������� ��� ���� ����
			// **
			// Anadromika splitInterNode(parentPageID); 
			// **
		}

	} else {		// ��������� ��� � ���� �������� ������ ��� ���� ������, ��� ����� ����
		// ���������� inter node
		// �������� �������
		this->fh.ReservePage(pageHandle);
		if (rc != OK) return rc;

		int interPageID;
		rc = pageHandle.GetPageID(interPageID);
		if (rc != OK) { return rc; }
	
		char* interData;
		rc = pageHandle.GetDataPtr(&interData);
		if (rc != OK) { return rc; }

		nh.parent = interPageID;
		nhNew.parent = interPageID;
		for (int i = 0; i < 1200; i++)
			nhParent.sStatus.set(i,false);
		
		nhParent.parent = -1;
		nhParent.leaf = false;
		nhParent.sStatus.set(0,true);
		nhParent.numberOfItems++;
		nhParent.left = -1;
		nhParent.right = -1;

		// ������� ��� interItem
		memcpy(&interData[sizeof(NodeHeader)], this->getInterItemData(iItemNew), this->getInterItemSize());

		// ��������� ��� header
		memcpy(&interData[0], &nhParent, sizeof(NodeHeader));

		rc = this->fh.MarkPageDirty(interPageID);
		if (rc != OK) return rc;

		rc = this->fh.FlushPage(interPageID);
		if (rc != OK) return rc;

		rc = this->fh.UnpinPage(interPageID);
		if (rc != OK) return rc;
	}

	memcpy(&data[0], &nh, sizeof(NodeHeader));
	memcpy(&newInterData[0], &nhNew, sizeof(NodeHeader));

	return (OK);

}

// ���������� �� id ��� ������� ��� �� ������ �� ����� �
// �������� ��� ���� ���������
int INXM_IndexHandle::searchRightPage(int pageID, void* key){

	STORM_PageHandle pageHandle;
	char* data;

	// ���������� ��� header ��� �������
	fh.GetPage(pageID, pageHandle);	
	pageHandle.GetDataPtr(&data);

	NodeHeader currentHeader;
	memcpy(&currentHeader, &data[0], sizeof(NodeHeader));

	if( currentHeader.leaf == true ){
		// � ������ ��� ����� ������� ����� �����, ��������� � ���������
		return pageID;
	} else {
		// � ������ ��� ����� ������� ����� ����������, ������ ��� �����
		
		for(int i = 0; i < currentHeader.numberOfItems; i++){	
			InterItem currentItem, nextItem;
			char* currentData = new char[this->getInterItemSize()];

			memcpy(currentData, &data[sizeof(NodeHeader)+i*this->getInterItemSize()], this->getInterItemSize());
			currentItem = this->getInterItemStruct(currentData);

			memcpy(currentData, &data[sizeof(NodeHeader)+(i+1)*this->getInterItemSize()], this->getInterItemSize());
			nextItem = this->getInterItemStruct(currentData);

			if(keyCompare(key, currentItem.key) < 0 ){
				// akoloutha aristero paidi
				searchRightPage(currentItem.leftChild, key);
			} else if ( (keyCompare(key, currentItem.key) > 0) && ((keyCompare(key, nextItem.key) == 0) || (keyCompare(key, nextItem.key) < 0))) {
				// akoloutha aristero paidi
				searchRightPage(nextItem.leftChild, key);
			} else if ((keyCompare(key, currentItem.key) > 0) || (keyCompare(key, currentItem.key) == 0)){
				//akoloutha deksi paidi
				searchRightPage(currentItem.leftChild, key);
			}

		}		
	}
}

// ���������� ���� ������/����
t_rc INXM_IndexHandle::makeRoot(void *pData, const REM_RecordID &rid){
	t_rc rc;
	int pageID;
	STORM_PageHandle myPageHandle;


	// �������� �������
	rc = this->fh.ReservePage(myPageHandle);
	if (rc != OK) return rc;
	
	rc = this->fh.GetNextPage(this->fileHeaderPageID, myPageHandle);
	if (rc != OK) return rc;

	rc = myPageHandle.GetPageID(pageID);
	if (rc != OK) { return rc; }
	
	char* rData;
	rc = myPageHandle.GetDataPtr(&rData);
	if (rc != OK) { return rc; }

	this->fileHeader.rootPage = pageID;

	// ���������� header ��� �� ������
	// ���� ������ ����� ��� node
	NodeHeader nh;
	nh.numberOfItems = 1;
	for (int i = 0; i < 1200; i++)
		nh.sStatus.set(i,false);
	nh.sStatus.set(0, true);
	nh.left = -1;
	nh.right = -1;
	nh.parent = -1;
	nh.leaf = true;	

	LeafItem item = { pData, rid };
	// ������� ��� ������������
	memcpy(&rData[sizeof(NodeHeader)], this->getLeafItemData(item), this->getLeafItemSize());
	this->fileHeader.numberOfNodes++;
	this->fileHeader.numberOfLeafNodes++;

	// ������� ��� nodeheader
	memcpy(&rData[0], &nh, sizeof(NodeHeader));

	rc = this->fh.MarkPageDirty(pageID);
	if (rc != OK) return rc;

	rc = this->fh.FlushPage(pageID);
	if (rc != OK) return rc;

	rc = this->fh.UnpinPage(pageID);
	if (rc != OK) return rc;

	return(OK);
}

t_rc INXM_IndexHandle::insert(int rightPageID, void* key, const REM_RecordID &rid){
	t_rc rc;

	// ���������� leaf item
	LeafItem newItem;
	newItem.key = key;
	newItem.rid = rid;

	// ������� ��� header ��� �� �������� ��� ������� ( rightPage )
	STORM_PageHandle pageHandle;
	char* hData;

	rc = fh.GetPage(rightPageID, pageHandle);
	if (rc != OK) return rc;

	rc = pageHandle.GetDataPtr(&hData);
	if (rc != OK) return rc;

	NodeHeader rightPageHeader;
	memcpy(&rightPageHeader, &hData[0], sizeof(NodeHeader));


	bool posFound = false;
	LeafItem tempItem1 = newItem;
	LeafItem tempItem2;
	int i;
	for (i = 0; i < rightPageHeader.numberOfItems; i++) {

		// ������� ��� ��� �� ����������� ��� �������
		char* currentData = new char[this->getLeafItemSize()];
		memcpy(currentData, &hData[sizeof(NodeHeader) + i*this->getLeafItemSize()], this->getLeafItemSize());

		LeafItem currentItem;
		currentItem = this->getLeafItemStruct(currentData);

		if (keyCompare(newItem.key, currentItem.key) < 0 ) posFound = true;

		// ������� � ����� ���� ���������� ���� ��� ������ item
		// ��� ���� �����
		if (posFound) {
			tempItem2 = currentItem;
			currentItem = tempItem1;
			tempItem1 = tempItem2;
			 
			int j = rightPageHeader.numberOfItems ;
			rightPageHeader.sStatus.set(j,true);
			while( j != i ){
				char* myCurrentData = new char[this->getLeafItemSize()];
				memcpy(myCurrentData, &hData[sizeof(NodeHeader) + (j-1)*this->getLeafItemSize()], this->getLeafItemSize());

				LeafItem myCurrentItem;
				myCurrentItem = this->getLeafItemStruct(myCurrentData);

				memcpy(&hData[sizeof(NodeHeader) + j*this->getLeafItemSize()], this->getLeafItemData(myCurrentItem), this->getLeafItemSize());
				j--;
			}
			memcpy(&hData[sizeof(NodeHeader) + i*this->getLeafItemSize()], this->getLeafItemData(currentItem), this->getLeafItemSize());
			break;
		}
	}

	if (!posFound){
		memcpy(&hData[sizeof(NodeHeader) + i*this->getLeafItemSize()], this->getLeafItemData(newItem),this->getLeafItemSize());
		rightPageHeader.sStatus.set(i,true);
	}
	
	rightPageHeader.numberOfItems++;

	// ������� ��� header
	memcpy(&hData[0], &rightPageHeader, sizeof(NodeHeader));

	rc = this->fh.MarkPageDirty(rightPageID);
	if (rc != OK) return rc;

	rc = this->fh.FlushPage(rightPageID);
	if (rc != OK) return rc;

	rc = this->fh.UnpinPage(rightPageID);
	if (rc != OK) return rc;

	return (OK);
}



t_rc INXM_IndexHandle::InsertEntry(void *pData, const REM_RecordID &rid){
	t_rc rc;
	STORM_PageHandle pageHandle;	
	/* 
	 * ������� �� ������� ������
	 * ���������� ���� �������/�����
	 */	
	if( this->isOpen && this->fileHeader.numberOfNodes == 0 ){
		makeRoot(pData, rid);
		return(OK);
	}

	/*
	 * ������� ��� ������
	 */

	// ��������� ��� ��� ������ ��� ������ �������/������ ���� �� ���� � ��� �������
	int rightPageID = searchRightPage( fileHeader.rootPage , pData);
	
	// ������� ��� header ����� ��� �������
	
	char* data;

	rc = fh.GetPage(rightPageID, pageHandle);
	if (rc != OK) return rc;

	rc = pageHandle.GetDataPtr(&data);
	if (rc != OK) return rc;

	NodeHeader rightPageHeader;
	memcpy(&rightPageHeader, &data[0], sizeof(NodeHeader));

	// ������� �� � ������ ���� ����
	bool freeSpace = checkLeafNodeForSpace(rightPageHeader);

	if(freeSpace){		// ��������� ��� � ������ ���� ����

		insert(rightPageID, pData, rid);
		return (OK);

	} else {			// ��������� ��� � ������ ��� ���� ����

		rc = splitLeafNode(rightPageID);
		if (rc != OK) return rc;

		int pageAfterSplitID = searchRightPage( fileHeader.rootPage , pData);
		insert(pageAfterSplitID, pData, rid);
		return (OK);

	}
}

