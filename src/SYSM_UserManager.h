#ifndef _SYSM_UserManager_h
#define _SYSM_UserManager_h

#include "REM.h"
#include "SYSM.h"

class SYSM_UserManager
{
	char * path;

	char * user;
	bool isLoggedIn;

	REM_RecordFileManager * rfm;
	SYSM_MetaManager * smm;

public:
	SYSM_UserManager(REM_RecordFileManager * rfm);
	~SYSM_UserManager();

	t_rc LogIn(char * username, char * password);
	t_rc LogOut();
	t_rc CheckPrivilege(char * dbname);
	t_rc InsertUser(char * username, char * password);
	t_rc DeleteUser(char * username, char * password);
	t_rc AddPrivilege(char * username, char * dbname);
	t_rc RemovePrivilege(char * username, char * dbname);
	t_rc SYSM_UserManager::RemoveAllPrivileges (char * username);
};

#endif