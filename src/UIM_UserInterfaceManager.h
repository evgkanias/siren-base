#ifndef _UIM_UserInterface_Manager_h
#define _UIM_UserInterface_Manager_h

#include <iostream>
#include "UIM.h"
#include "SSQLM.h"
#include "SYSM.h"
#include "retcodes.h"

class UIM_UserInterfaceManager
{
	SYSM_UserManager * user_Mgr;
	SYSM_SystemManager * sysm_Mgr;
	SSQLM_DDL_Manager * ssqlm_ddl_Mgr;
	SSQLM_DML_Manager * ssqlm_dml_Mgr;

	t_rc ExecuteQuery (char * query);
	t_rc Execute_SYSM_Command (char * command);
	t_rc Execute_SSQLM_DDL_Command (char * command);
	t_rc Execute_SSQLM_DML_Command (char * command);
	t_rc RunScript (char * fileName);
	t_rc AddPrivileges (char * command);
	
	char * FixQuery (char * query);
	bool changeEQ (char ch, char ch_1, bool mode);
	bool changeNE (char ch, char ch_1);
	bool changeLT (char ch, char ch_1, bool mode);
	bool changeGT (char ch, char ch_1, bool mode);
	bool changeOther (char ch, char ch_1, bool mode);
public:
	UIM_UserInterfaceManager ();
	~UIM_UserInterfaceManager ();
	t_rc StartUI ();
};

#endif
