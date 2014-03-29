#ifndef _REM_RecordFileManager_h
#define _REM_RecordFileManager_h

#include "STORM.h"
#include "REM.h"
#include "retcodes.h"

class REM_RecordFileManager {
private:
	STORM_StorageManager * sm;
public:
	REM_RecordFileManager(STORM_StorageManager * sm);
	~REM_RecordFileManager(void);

	t_rc CreateRecordFile(const char * fname, int rs);
	t_rc DestroyRecordFile(const char * fname);
	t_rc OpenRecordFile(const char * fname, REM_RecordFileHandle &rfh);
	t_rc CloseRecordFile(REM_RecordFileHandle &rfh);
};

#endif