#include "REM_RecordHandle.h"


REM_RecordHandle::REM_RecordHandle() {}

REM_RecordHandle::~REM_RecordHandle() {}

t_rc REM_RecordHandle::GetData (char * &pData) const {
	pData = this->pData;
	return (OK);
}

t_rc REM_RecordHandle::GetRecordID (REM_RecordID &rid) const {
	rid = this->rid;
	return (OK);
}