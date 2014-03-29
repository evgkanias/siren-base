#ifndef _REM_RecordHandle
#define _REM_RecordHandle

#include "REM_RecordID.h"
#include "STORM_PageHandle.h"
#include "retcodes.h"

class REM_RecordHandle {
	REM_RecordID rid;
	char * pData;

public:
	REM_RecordHandle();
	~REM_RecordHandle();

	t_rc GetData (char * &pData) const;
	t_rc GetRecordID (REM_RecordID &rid) const;

	friend class REM_RecordFileHandle;
};

#endif