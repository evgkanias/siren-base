#ifndef _INXM_IndexScan_h
#define _INXM_IndexScan_h

#include "retcodes.h"
#include "REM.h"
#include "INXM_IndexHandle.h"
#include "STORM_StorageManager.h"


class INXM_IndexScan { 

	bool isOpen;
	IndexFileHeader fileHeader;
	STORM_FileHandle * fh;

	INXM_IndexHandle * ih;
	t_compOp compOp;
	void * value;

	REM_RecordID cRecord;		// Η τρέχουσα εγγραφή
	int occasion;				// Βοηθητική μεταβλητή για τον διαχωρισμό περιπτώσεων
public:
	INXM_IndexScan(); 
	~INXM_IndexScan();
	
	t_rc OpenIndexScan(INXM_IndexHandle &ih,
					   t_compOp compOp,
					   void *value);
	t_rc GetNextEntry(REM_RecordID &rid);
	t_rc CloseIndexScan();
};

#endif