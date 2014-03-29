#ifndef _REM_RecordFileScan_h
#define _REM_RecordFileScan_h

#include "retcodes.h"
#include "REM_RecordFileHandle.h"
#include "REM_RecordHandle.h"
#include "REM_Types.h"

class REM_RecordFileScan { 
  private:
	
	bool isOpen;				// ������� �� �� scan ����� �������
	
	REM_RecordFileHandle * rfh;	// ������� �� �������� ������� ��������
	t_attrType attrType;		// ����� ������
	int attrLength;				// ����� ������
	int attrOffset;				// Offset ��� ������
	t_compOp compOp;			// �������� ���������
	void * value;				// ������� ���� ����

	REM_RecordID cRecord;		// � �������� �������
	
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