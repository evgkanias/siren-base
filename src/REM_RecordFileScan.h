#ifndef _REM_RecordFileScan_h
#define _REM_RecordFileScan_h

#include "retcodes.h"
#include "REM_RecordFileHandle.h"
#include "REM_RecordHandle.h"
#include "REM_Types.h"

class REM_RecordFileScan { 
  private:
	
	bool isOpen;				// Δηλώνει αν το scan είναι ανοιχτό
	
	REM_RecordFileHandle * rfh;	// Αναφορά σε χειριστή αρχείου εγγραφών
	t_attrType attrType;		// Τύπος πεδίου
	int attrLength;				// Μήκος πεδίου
	int attrOffset;				// Offset του πεδίου
	t_compOp compOp;			// Τελεστής σύγκρισης
	void * value;				// Αναφορά στην τιμή

	REM_RecordID cRecord;		// Η τρέχουσα εγγραφή
	
  public:
	
	REM_RecordFileScan(); 
	~REM_RecordFileScan();
	
	t_rc OpenRecordScan (REM_RecordFileHandle &rfh, 
						 t_attrType attrType,
						 int attrLength, 
						 int attrOffset, 
						 t_compOp compOp, 
						 void * value);
	t_rc GetNextRecord(REM_RecordHandle &rh); 
	t_rc CloseRecordScan();
	
};

#endif