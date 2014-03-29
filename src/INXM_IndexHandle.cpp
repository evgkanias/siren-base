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

// Επιστρέφει το μέγεθος της δομής InterItem
int INXM_IndexHandle::getInterItemSize(){
	return fileHeader.attrLength + 2*sizeof(int);
}

// Επιστρέφει το μέγεθος της δομής LeafItem
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

// Διαβάζει το struct ενός Inter Item
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

// Διαβάζει το struct ενός Leaf Item
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


// Διαχωρισμός κόμβου (φύλλου) ο οποίος έχει γεμίσει
// Χρειάζομαι μόνο το id της σελίδας του κόμβου
t_rc INXM_IndexHandle::splitLeafNode(int pageToSplitID){
	t_rc rc;
	
	// Διαβάζω τον Node Header της σελίδας
	STORM_PageHandle pageHandle;
	char* data;

	rc = fh.GetPage(pageToSplitID, pageHandle);
	if (rc != OK) return rc;

	rc = pageHandle.GetDataPtr(&data);
	if (rc != OK) return rc;

	NodeHeader nh;		// o header της σελίδας πρός διάσπαση
	memcpy(&nh, &data[0], sizeof(NodeHeader));

	// Δημιουργία ΝΕΟΥ leaf node
	// Δέσμευση σελίδας
	this->fh.ReservePage(pageHandle);
	if (rc != OK) return rc;

	int newLeafPageID;
	rc = pageHandle.GetPageID(newLeafPageID);
	if (rc != OK) { return rc; }
	
	char* newLeafData;
	rc = pageHandle.GetDataPtr(&newLeafData);
	if (rc != OK) { return rc; }
	
	// Αρχικοποίηση του νέου node header
	NodeHeader nhNew;
	nhNew.leaf = true;
	nhNew.parent = nh.parent;
	for (int i = 0; i < 1200; i++)
		nhNew.sStatus.set(i,false);
	
	// Μετακίνηση των μισών αντικειμένων από τον γεμάτο κόμβο
	// στον καινούριο
	for (int i = nh.numberOfItems/2; i < nh.numberOfItems; i++) {
		memcpy(
			&newLeafData[sizeof(NodeHeader)+(i-nh.numberOfItems/2)*this->getLeafItemSize()],
			&data[sizeof(NodeHeader)+i*this->getLeafItemSize()],
			this->getLeafItemSize());
		
		// Μείωση πλήθους αντικειμένων στον έναν κόμβο
		nh.numberOfItems--;
		nh.sStatus.set(i, false);

		// Αύξηση πλήθους αντικειμένων στον άλλο κόμβο
		nhNew.numberOfItems++;
		nhNew.sStatus.set(i-nh.numberOfItems/2, true);
		
	}	
	// Δεικτοδότηση
	nhNew.right = nh.right;
	nh.right = newLeafPageID;
	nhNew.left = pageToSplitID;
	

	// Αρχικοποίηση του ΠΑΤΕΡΑ ενδιάμεσου κόμβου
	NodeHeader nhParent;

	// Διαβάζω το ΠΡΩΤΟ item από το νέο κόμβο φύλλο που μόλις δημιούργησα
	char* firstItemData = new char[this->getLeafItemSize()];
	memcpy(&firstItemData, &newLeafData[sizeof(NodeHeader)], this->getLeafItemSize());

	LeafItem firstLeftItem = this->getLeafItemStruct(firstItemData);

	// Δημιουργία του item που θα ΑΝΤΙΓΡΑΦΕΙ στον κόμβο πατέρα
	InterItem iItemNew;
	iItemNew.key = firstLeftItem.key;
	iItemNew.rightChild = newLeafPageID;
	iItemNew.leftChild = pageToSplitID;

	if ( nh.parent < 0) {		// Περίπτωση που ο προς διάσπαση κόμβος ΔΕΝ έχει πατέρα, δλδ είναι ρίζα
		// Δημιουργία inter node
		// Δέσμευση σελίδας
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

		// Εγγραφή του interItem
		memcpy(&interData[sizeof(NodeHeader)], this->getInterItemData(iItemNew), this->getInterItemSize());

		// Ενημέρωση του header
		memcpy(&interData[0], &nhParent, sizeof(NodeHeader));

		rc = this->fh.MarkPageDirty(interPageID);
		if (rc != OK) return rc;

		rc = this->fh.FlushPage(interPageID);
		if (rc != OK) return rc;

		rc = this->fh.UnpinPage(interPageID);
		if (rc != OK) return rc;

	} else {		// Περίπτωση που ο προς διάσπαση κόμβος έχει πατέρα, δλδ δεν είναι ρίζα
		
		// Διαβάζω τον header του ήδη υπάρχοντα πατέρα
		int parentPageID = nh.parent;	// απλή ανάθεση για ευκολία
		char* parentData;

		rc = fh.GetPage(parentPageID, pageHandle);
		if (rc != OK) return rc;

		rc = pageHandle.GetDataPtr(&parentData);
		if (rc != OK) return rc;

		memcpy(&nhParent, &parentData[0], sizeof(NodeHeader));

		if(checkInterNodeForSpace(nhParent)){		// περίπτωση που ο πατέρας έχει χώρο
			bool posFound = false;
			InterItem tempItem1 = iItemNew;
			InterItem tempItem2;
			int i;

			// Αντιγραφή του πρώτου item του δεξίου παιδιού
			// στον πατέρα
			for (i = 0; i < nhParent.numberOfItems+1; i++) {
				
				char* currentData = new char[this->getInterItemSize()];
				InterItem currentItem;

				memcpy(currentData, &parentData[sizeof(NodeHeader)+i*this->getInterItemSize()], this->getInterItemSize());
				currentItem = this->getInterItemStruct(currentData);

				if (keyCompare(iItemNew.key, currentItem.key) < 0 ) posFound = true;

				// Βρέθηκε η σωστή θέση μετακίνηση όλων των δεξίων item
				// μία θέση δεξιά
				if (posFound) {
					tempItem2 = currentItem;
					currentItem = tempItem1;
					tempItem1 = tempItem2;

					memcpy(&parentData[sizeof(NodeHeader)+i*this->getInterItemSize()],this->getInterItemData(currentItem),this->getInterItemSize());
				}
			}

			nhParent.numberOfItems++;
			nhParent.sStatus.set(i-1,true);

		} else {	// περίπτωση που ο πατέρας δεν έχει χώρο
			 splitInterNode(parentPageID); 

		}

		// Τελική ενημέρωση του header του πατέρα
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

// Διαχωρισμός ΕΝΔΙΑΜΕΣΟΥ κόμβου ο οποίος έχει γεμίσει
// Χρειάζομαι μόνο το id της σελίδας του κόμβου
t_rc INXM_IndexHandle::splitInterNode(int pageToSplitID){
	t_rc rc;
	
	// Διαβάζω τον Inter Node Header της σελίδας
	STORM_PageHandle pageHandle;
	char* data;

	rc = fh.GetPage(pageToSplitID, pageHandle);
	if (rc != OK) return rc;

	rc = pageHandle.GetDataPtr(&data);
	if (rc != OK) return rc;

	NodeHeader nh;		// o header της σελίδας πρός διάσπαση
	memcpy(&nh, &data[0], sizeof(NodeHeader));

	// Δημιουργία ΝΕΟΥ inter node
	// Δέσμευση σελίδας
	this->fh.ReservePage(pageHandle);
	if (rc != OK) return rc;

	int newInterPageID;
	rc = pageHandle.GetPageID(newInterPageID);
	if (rc != OK) { return rc; }
	
	char* newInterData;
	rc = pageHandle.GetDataPtr(&newInterData);
	if (rc != OK) { return rc; }
	
	// Αρχικοποίηση του νέου inter node header
	NodeHeader nhNew;
	nhNew.leaf = false;
	nhNew.parent = nh.parent;
	nhNew.right = -1;
	nhNew.left = -1;
	for (int i = 0; i < 1200; i++)
		nhNew.sStatus.set(i,false);

	// Μετακίνηση των μισών αντικειμένων από τον γεμάτο κόμβο
	// στον καινούριο
	for (int i = nh.numberOfItems/2; i < nh.numberOfItems; i++) {
		memcpy(
			&newInterData[sizeof(NodeHeader)+(i-nh.numberOfItems/2)*this->getInterItemSize()],
			&data[sizeof(NodeHeader)+i*this->getInterItemSize()],
			this->getInterItemSize());
		
		// Μείωση πλήθους αντικειμένων στον έναν κόμβο
		nh.numberOfItems--;
		nh.sStatus.set(i, false);

		// Αύξηση πλήθους αντικειμένων στον άλλο κόμβο
		nhNew.numberOfItems++;
		nhNew.sStatus.set(i-nh.numberOfItems/2, true);
	}

	

	// Δημιουργία του ΠΑΤΕΡΑ ενδιάμεσου κόμβου
	// Δημιουργία του header
	NodeHeader nhParent;

	// Διαβάζω το ΠΡΩΤΟ item από το νέο ενδιάμεσο κόμβο που μόλις δημιούργησα
	char* firstItemData = new char[this->getInterItemSize()];
	memcpy(&firstItemData, &newInterData[sizeof(NodeHeader)], this->getInterItemSize());

	InterItem firstLeftItem = this->getInterItemStruct(firstItemData);

	// Δεικτόδοτηση του item που θα μπεί στον κόμβο πατέρα
	InterItem iItemNew;
	iItemNew.key = firstLeftItem.key;
	iItemNew.rightChild = newInterPageID;
	iItemNew.leftChild = pageToSplitID;

	if ( nh.parent > 0) {		// Περίπτωση που ο προς διάσπαση κόμβος έχει πατέρα, δλδ δεν είναι ρίζα
		int parentPageID = nh.parent;
		char* parentData;

		rc = fh.GetPage(parentPageID, pageHandle);
		if (rc != OK) return rc;

		rc = pageHandle.GetDataPtr(&parentData);
		if (rc != OK) return rc;

		memcpy(&nhParent, &parentData[0], sizeof(NodeHeader));

		if(checkInterNodeForSpace(nhParent)){		// περίπτωση που ο πατέρας έχει χώρο
			bool posFound = false;
			InterItem tempItem1 = iItemNew;
			InterItem tempItem2;
			int i;

			// Αντιγραφή του πρώτου item του δεξίου παιδιού
			// στον πατέρα
			for (i = 0; i < nhParent.numberOfItems+1; i++) {
				
				char* currentData = new char[this->getInterItemSize()];
				InterItem currentItem;

				memcpy(currentData, &parentData[sizeof(NodeHeader)+i*this->getInterItemSize()], this->getInterItemSize());
				currentItem = this->getInterItemStruct(currentData);

				if (keyCompare(iItemNew.key, currentItem.key) < 0 ) posFound = true;

				// Βρέθηκε η σωστή θέση μετακίνηση όλων των δεξίων item
				// μία θέση δεξιά
				if (posFound) {
					tempItem2 = currentItem;
					currentItem = tempItem1;
					tempItem1 = tempItem2;

					memcpy(&parentData[sizeof(NodeHeader)+i*this->getInterItemSize()],this->getInterItemData(currentItem),this->getInterItemSize());
				}
			}

			nhParent.numberOfItems++;
			nhParent.sStatus.set(i-1,true);

		} else {	// περίπτωση που ο πατέρας δεν έχει χώρο
			// **
			// Anadromika splitInterNode(parentPageID); 
			// **
		}

	} else {		// Περίπτωση που ο προς διάσπαση κόμβος ΔΕΝ έχει πατέρα, δλδ είναι ρίζα
		// Δημιουργία inter node
		// Δέσμευση σελίδας
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

		// Εγγραφή του interItem
		memcpy(&interData[sizeof(NodeHeader)], this->getInterItemData(iItemNew), this->getInterItemSize());

		// Ενημέρωση του header
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

// Επιστρέφει το id της σελίδας που θα πρέπει να γίνει η
// εισαγωγή του νέου στοιχείου
int INXM_IndexHandle::searchRightPage(int pageID, void* key){

	STORM_PageHandle pageHandle;
	char* data;

	// Διαβάζουμε τον header της σελίδας
	fh.GetPage(pageID, pageHandle);	
	pageHandle.GetDataPtr(&data);

	NodeHeader currentHeader;
	memcpy(&currentHeader, &data[0], sizeof(NodeHeader));

	if( currentHeader.leaf == true ){
		// ο κόμβος που μόλις διάβασα είναι φύλλο, τελειώνει η αναζήτηση
		return pageID;
	} else {
		// ο κόμβος που μόλις διάβασα είναι ενδιάμεσος, ελέγχω τις τιμές
		
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

// Δημιουργία νέου κόμβου/ρίζα
t_rc INXM_IndexHandle::makeRoot(void *pData, const REM_RecordID &rid){
	t_rc rc;
	int pageID;
	STORM_PageHandle myPageHandle;


	// Δέσμευση σελίδας
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

	// Δημιουργία header για τη σελίδα
	// Κάθε σελίδα είναι και node
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
	// Εγγραφή του αντικειμένου
	memcpy(&rData[sizeof(NodeHeader)], this->getLeafItemData(item), this->getLeafItemSize());
	this->fileHeader.numberOfNodes++;
	this->fileHeader.numberOfLeafNodes++;

	// Εγγραφή του nodeheader
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

	// Δημιουργία leaf item
	LeafItem newItem;
	newItem.key = key;
	newItem.rid = rid;

	// Διαβάζω τον header και τα δεδομένα της σελίδας ( rightPage )
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

		// Διαβάζω ένα ένα τα αντικείμενα της σελίδας
		char* currentData = new char[this->getLeafItemSize()];
		memcpy(currentData, &hData[sizeof(NodeHeader) + i*this->getLeafItemSize()], this->getLeafItemSize());

		LeafItem currentItem;
		currentItem = this->getLeafItemStruct(currentData);

		if (keyCompare(newItem.key, currentItem.key) < 0 ) posFound = true;

		// Βρέθηκε η σωστή θέση μετακίνηση όλων των δεξίων item
		// μία θέση δεξιά
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

	// Εγγραφή του header
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
	 * Έλεγχος αν υπάρχει δέντρο
	 * Δημιουργία νέου δέντρου/ρίζας
	 */	
	if( this->isOpen && this->fileHeader.numberOfNodes == 0 ){
		makeRoot(pData, rid);
		return(OK);
	}

	/*
	 * Υπάρχει ήδη δέντρο
	 */

	// Αναζήτηση για την εύρεση της ΣΩΣΤΗΣ σελίδας/φύλλου όπου θα μπεί ή νέα εγγραφή
	int rightPageID = searchRightPage( fileHeader.rootPage , pData);
	
	// Διαβάζω τον header αυτής της σελίδας
	
	char* data;

	rc = fh.GetPage(rightPageID, pageHandle);
	if (rc != OK) return rc;

	rc = pageHandle.GetDataPtr(&data);
	if (rc != OK) return rc;

	NodeHeader rightPageHeader;
	memcpy(&rightPageHeader, &data[0], sizeof(NodeHeader));

	// Έλεγχος αν η σελίδα έχει χώρο
	bool freeSpace = checkLeafNodeForSpace(rightPageHeader);

	if(freeSpace){		// περίπτωση που η σελίδα έχει χώρο

		insert(rightPageID, pData, rid);
		return (OK);

	} else {			// περίπτωση που η σελίδα ΔΕΝ έχει χώρο

		rc = splitLeafNode(rightPageID);
		if (rc != OK) return rc;

		int pageAfterSplitID = searchRightPage( fileHeader.rootPage , pData);
		insert(pageAfterSplitID, pData, rid);
		return (OK);

	}
}

