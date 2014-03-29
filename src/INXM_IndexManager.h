#ifndef _INXM_IndexManager_h
#define _INXM_IndexManager_h

#include "retcodes.h"
#include "REM_RecordID.h"
#include "STORM.h"
#include "INXM_IndexHandle.h"

class INXM_IndexManager {

private:
	
	STORM_StorageManager *sm;
	int fileHeaderPageID;
public:
	INXM_IndexManager (STORM_StorageManager *sm);
	~INXM_IndexManager();
	
	t_rc CreateIndex  (const char *fname,
					  int indexNo, 
					  t_attrType attrType,
					  int attrLength);
	t_rc DestroyIndex (const char *fname, int indexNo);
	t_rc OpenIndex	  (const char *fname, int indexNo, INXM_IndexHandle &ih);
	t_rc CloseIndex   (INXM_IndexHandle &ih);
};

#endif