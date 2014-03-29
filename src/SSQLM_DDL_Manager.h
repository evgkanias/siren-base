#ifndef _SSQLM_DDL_Manager_h
#define _SSQLM_DDL_Manager_h

#include "REM.h"
#include "INXM.h"
#include "SSQLM.h"
#include "SYSM.h"

class SSQLM_DDL_Manager
{
	std::string path;			// Η διαδρομή για το φάκελο με τις βάσεις δεδομένων

	REM_RecordFileManager * rfm;	//to handle the "table" files
	INXM_IndexManager * im;
	SYSM_MetaManager * smm;
	std::string openedDatabaseName;

	t_rc checkAttrType(char * attrType, t_attrType * attrTypeAsStruct, int * attrSize);

public:
	SSQLM_DDL_Manager(REM_RecordFileManager * rfm, INXM_IndexManager * im, SYSM_MetaManager * smm);
	~SSQLM_DDL_Manager(void);

	t_rc SetOpenedDatabase(char * db_Name);
	t_rc CreateTable(char * t_Name, char * attrNames);
	t_rc DropTable(char * t_Name);
	t_rc CreateIndex(char * t_Name, char * a_Name);
	t_rc DropIndex(char * t_Name, char * a_Name);

	friend class UIM_UserInterfaceManager;
};

#endif
