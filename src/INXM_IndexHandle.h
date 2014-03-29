#ifndef _INXM_IndexHandle_h
#define _INXM_IndexHandle_h

#include "retcodes.h"
#include "REM.h"
#include "STORM.h"

// Index File Header
typedef struct IndexFileHeader {
	int numberOfNodes;
	int numberOfLeafNodes;
	int rootPage;
	t_attrType attrType;
	int attrLength;
} IndexFileHeader;

// Node Header
typedef struct NodeHeader{
	int numberOfItems;
	int parent;
	int left;
	int right;
	bitset<1200> sStatus;
	bool leaf;
} NodeHeader;

// Intermediate Item
typedef struct InterItem {
	void *key;
	int leftChild;
	int rightChild;
} InterItem;

// Leaf Item
typedef struct LeafItem {
	void *key;
	REM_RecordID rid;
} LeafItem;


//*************************************************************************
class INXM_IndexHandle {

	bool isOpen;
	IndexFileHeader fileHeader;
	int fileHeaderPageID;
	STORM_FileHandle fh;

	int keyCompare(void *key1,void *key2);
	int getInterItemSize();
	int getLeafItemSize();
	char* getInterItemData(InterItem iItem);
	char* getLeafItemData(LeafItem lItem);
	InterItem getInterItemStruct(char* data);
	LeafItem getLeafItemStruct(char* data);
	bool checkInterNodeForSpace(NodeHeader nh);
	bool checkLeafNodeForSpace(NodeHeader nh);

	t_rc makeRoot(void *pData, const REM_RecordID &rid);
	int searchRightPage(int rootPageID, void* key);
	t_rc splitInterNode(int pageToSplitID);
	t_rc splitLeafNode(int pageToSplitID);
	t_rc insert(int rightPageID, void* key, const REM_RecordID &rid);

public:
	INXM_IndexHandle();
	~INXM_IndexHandle();

	t_rc InsertEntry(void *pData, const REM_RecordID &rid);
	t_rc FlushPages();

	friend class INXM_IndexManager;
	friend class INXM_IndexScan;

};

#endif