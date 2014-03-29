#ifndef _SYSM_SystemManager_h
#define _SYSM_SystemManager_h

#include <fstream>
#include <direct.h>
#include <Windows.h>
#include "REM.h"
#include "retcodes.h"
#include "SYSM.h"

class SYSM_SystemManager  {
	
	std::string path;			// � �������� ��� �� ������ �� ��� ������ ���������

	bool isOpen;				// ������� �� ������ ���� ����� �������
	std::string openedDBName;	// �� ����� ��� �������� ����� ���������

	REM_RecordFileManager * rfm;
	
public:
	SYSM_SystemManager (REM_RecordFileManager  * rfm);
	~SYSM_SystemManager ();

	t_rc CreateDatabase (char * databaseName);
	t_rc DropDatabase (char * databaseName);
	t_rc OpenDatabase (char * databaseName);
	t_rc CloseDatabase ();

	friend class UIM_UserInterfaceManager;
	friend class SYSM_MetaManager;
};
#endif
