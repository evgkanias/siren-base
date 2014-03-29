#ifndef _REM_RecordFileHandle_h
#define _REM_RecordFileHandle_h

#include <bitset>
#include "retcodes.h"
#include "STORM_FileHandle.h"
#include "REM.h"

typedef struct RecordFileInfo {
	int nr;					// number of records in the file
	int rs;					// record size
	int rpp;				// records per page
} RecordFileInfo;

typedef struct RecordPageInfo {
	int recs;				// Πλήθος των εγγραφών
	int alocRecs;			// Πλήθος δεσμευμένων εγγραφών
	bitset<1200> sStatus;	// πίνακας που δείχνει την κατάσταση της κάθε θέσης (ελεύθερη ή όχι)
} RecordPageInfo;

class REM_RecordFileHandle {
	bool isOpen;			// Δείχνει αν το αρχείο είναι ανοιχτό
	STORM_FileHandle fh;	// Ο αντίστοιχος χειριστής αρχείων του STORM
	RecordFileInfo fInfo;	// Πληροφορίες για το αρχείο
	int fInfoPageID;		// Ο κωδικός της σελίδας που βρίσκονται οι πληροφορίες του αρχείου

	RecordPageInfo InitRecordPageInfo(int recs);
public:
	REM_RecordFileHandle();
	~REM_RecordFileHandle();

	t_rc ReadRecord (const REM_RecordID &rid, REM_RecordHandle &rh) const;
	t_rc InsertRecord (const char * pData, REM_RecordID &rid);
	t_rc DeleteRecord (const REM_RecordID &rid);
	t_rc UpdateRecord (const REM_RecordHandle &rh);
	t_rc FlushPages () const;

	friend class REM_RecordFileManager;
	friend class REM_RecordFileScan;
	friend class SYSM_SystemManager;
};

#endif