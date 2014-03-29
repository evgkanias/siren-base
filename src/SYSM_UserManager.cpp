#include "SYSM_UserManager.h"


SYSM_UserManager::SYSM_UserManager(REM_RecordFileManager * rfm) {
	this->rfm = rfm;
	this->smm = new SYSM_MetaManager(rfm, new SYSM_SystemManager(rfm));
	this->path = "C:\\sirenbase\\data\\sirenbase\\";
	this->user = "";
	this->isLoggedIn = false;

	this->smm->sysm->OpenDatabase("sirenbase");
}

SYSM_UserManager::~SYSM_UserManager() {
	this->smm->sysm->CloseDatabase();
}

t_rc SYSM_UserManager::LogIn(char * username, char * password) {
	t_rc rc;

	if (this->isLoggedIn) return (SYSM_ALREADYLOGGEDIN);
	
	std::string t_path(this->path);
	t_path.append("users");

	AttrMet u_meta;
	rc = this->smm->GetAttributeMetadata("users","username",u_meta);
	if (rc != OK) return rc;

	AttrMet p_meta;
	rc = this->smm->GetAttributeMetadata("users","password",p_meta);
	if (rc != OK) return rc;

	REM_RecordFileHandle rfh;
	rc = this->rfm->OpenRecordFile((char *) t_path.c_str(),rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan u_rfs;
	rc = u_rfs.OpenRecordScan(rfh,u_meta.type,u_meta.length,u_meta.offset,EQ_OP,username);
	if (rc != OK) return rc;

	REM_RecordHandle rh;
	while ((rc = u_rfs.GetNextRecord(rh)) == OK) {
		char * pData;
		rc = rh.GetData(pData);
		if (rc != OK) return rc;

		char * pwd = new char[p_meta.length];
		memcpy(pwd,&pData[p_meta.offset],p_meta.length);

		if (strcmp(pwd,password) == 0) {
			this->user = username;
			this->isLoggedIn = true;
			return (OK);
		}
	}
	
	rc = u_rfs.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (SYSM_LOGINERROR);
}

t_rc SYSM_UserManager::LogOut() {

	if (!this->isLoggedIn) return (SYSM_NOTLOGGEDIN);

	this->isLoggedIn = false;
	this->user = "";

	return (OK);
}

t_rc SYSM_UserManager::CheckPrivilege(char * dbname) {
	t_rc rc;

	if (!this->isLoggedIn) return (SYSM_NOTLOGGEDIN);

	std::string t_path(this->path);
	t_path.append("privileges");

	AttrMet u_meta;
	rc = this->smm->GetAttributeMetadata("privileges","username",u_meta);
	if (rc != OK) return rc;

	AttrMet db_meta;
	rc = this->smm->GetAttributeMetadata("privileges","dbname",db_meta);
	if (rc != OK) return rc;

	REM_RecordFileHandle rfh;
	rc = this->rfm->OpenRecordFile((char *) t_path.c_str(), rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,u_meta.type,u_meta.length,u_meta.offset,EQ_OP,this->user);
	if (rc != OK) return rc;

	REM_RecordHandle rh;
	while ((rc = rfs.GetNextRecord(rh)) == OK) {
		char * pData;
		rc = rh.GetData(pData);
		if (rc != OK) return rc;

		char * priv = new char[db_meta.length];
		memcpy(priv,&pData[db_meta.offset],db_meta.length);

		if (strcmp(priv,"all") == 0 || strcmp(priv,dbname) == 0) return (OK);
	}

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (SYSM_PRIVNOTFOUND);
}

t_rc SYSM_UserManager::InsertUser(char * username, char * password) {
	t_rc rc;

	if (!this->isLoggedIn) return (SYSM_NOTLOGGEDIN);
	if (this->CheckPrivilege("all") != OK) return (SYSM_NOTALLOWEDUSER);

	std::string t_path(this->path);
	t_path.append("users");

	AttrMet u_meta;
	rc = this->smm->GetAttributeMetadata("users", "username", u_meta);
	if (rc != OK) return rc;

	AttrMet p_meta;
	rc = this->smm->GetAttributeMetadata("users", "password", p_meta);
	if (rc != OK) return rc;

	REM_RecordFileHandle rfh;
	rc = this->rfm->OpenRecordFile((char *) t_path.c_str(),rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,u_meta.type,u_meta.length,u_meta.offset,EQ_OP,username);
	if (rc != OK) return rc;

	REM_RecordHandle rh;
	if (rfs.GetNextRecord(rh) == OK) return (SYSM_USEREXISTS);
	
	RelMet r_meta;
	rc = this->smm->GetRelationMetadata("users",r_meta);
	if (rc != OK) return rc;

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	char * pData = new char[r_meta.rs];
	memcpy(&pData[u_meta.offset],username,u_meta.length);
	memcpy(&pData[p_meta.offset],password,p_meta.length);

	REM_RecordID rid;
	rc = rfh.InsertRecord(pData,rid);
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (OK);
}

t_rc SYSM_UserManager::DeleteUser(char * username, char * password) {
	t_rc rc;

	if (!this->isLoggedIn) return (SYSM_NOTLOGGEDIN);
	if (this->CheckPrivilege("all") != OK) return (SYSM_NOTALLOWEDUSER);

	std::string t_path(this->path);
	t_path.append("users");

	AttrMet u_meta;
	rc = this->smm->GetAttributeMetadata("users", "username", u_meta);
	if (rc != OK) return rc;

	AttrMet p_meta;
	rc = this->smm->GetAttributeMetadata("users", "password", p_meta);
	if (rc != OK) return rc;

	REM_RecordFileHandle rfh;
	rc = this->rfm->OpenRecordFile((char *) t_path.c_str(),rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,u_meta.type,u_meta.length,u_meta.offset,EQ_OP,username);
	if (rc != OK) return rc;

	REM_RecordHandle rh;
	while ((rc = rfs.GetNextRecord(rh)) == OK) {
		char * pData;
		rc = rh.GetData(pData);
		if (rc != OK) return rc;

		char * aData = new char[p_meta.length];
		memcpy(aData,&pData[p_meta.offset],p_meta.length);

		if (strcmp(aData,password) == 0) {
			REM_RecordID rid;
			rc = rh.GetRecordID(rid);
			if (rc != OK) return rc;

			rc = rfh.DeleteRecord(rid);
			if (rc != OK) return rc;
		}
	}

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	rc = this->RemoveAllPrivileges(username);
	if (rc != OK) return rc;

	return (OK);
}

t_rc SYSM_UserManager::AddPrivilege (char * username, char * dbname) {
	t_rc rc;

	if (!this->isLoggedIn) return (SYSM_NOTLOGGEDIN);
	if (this->CheckPrivilege("all") != OK) return (SYSM_NOTALLOWEDUSER);

	std::string t_path(this->path);
	t_path.append("privileges");

	AttrMet u_meta;
	rc = this->smm->GetAttributeMetadata("privileges", "username", u_meta);
	if (rc != OK) return rc;

	AttrMet db_meta;
	rc = this->smm->GetAttributeMetadata("privileges", "dbname", db_meta);
	if (rc != OK) return rc;

	REM_RecordFileHandle rfh;
	rc = this->rfm->OpenRecordFile((char *) t_path.c_str(),rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,u_meta.type,u_meta.length,u_meta.offset,EQ_OP,username);
	if (rc != OK) return rc;

	REM_RecordHandle rh;
	while ((rc = rfs.GetNextRecord(rh)) == OK) {
		
		if (strcmp(dbname,"grand") == 0) {
			REM_RecordID rid;
			rc = rh.GetRecordID(rid);
			if (rc != OK) return rc;

			rc = rfh.DeleteRecord(rid);
			if (rc != OK) return rc;

		} else {
			char * pData;
			rc = rh.GetData(pData);
			if (rc != OK) return rc;

			char * aData = new char[db_meta.length];
			memcpy(aData,&pData[db_meta.offset],db_meta.length);

			if (strcmp(aData,dbname) == 0) return (SYSM_PRIVEXISTS);
		}
	}
	
	RelMet r_meta;
	rc = this->smm->GetRelationMetadata("privileges",r_meta);
	if (rc != OK) return rc;

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	char * pData = new char[r_meta.rs];
	memcpy(&pData[u_meta.offset],username,u_meta.length);
	memcpy(&pData[db_meta.offset],dbname,db_meta.length);

	REM_RecordID rid;
	rc = rfh.InsertRecord(pData,rid);
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (OK);
}

t_rc SYSM_UserManager::RemovePrivilege (char * username, char * dbname) {
	t_rc rc;

	if (!this->isLoggedIn) return (SYSM_NOTLOGGEDIN);
	if (this->CheckPrivilege("all") != OK) return (SYSM_NOTALLOWEDUSER);

	std::string t_path(this->path);
	t_path.append("privileges");

	AttrMet u_meta;
	rc = this->smm->GetAttributeMetadata("privileges", "username", u_meta);
	if (rc != OK) return rc;
	
	AttrMet db_meta;
	rc = this->smm->GetAttributeMetadata("privileges", "dbname", db_meta);
	if (rc != OK) return rc;

	REM_RecordFileHandle rfh;
	rc = this->rfm->OpenRecordFile((char *) t_path.c_str(),rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,u_meta.type,u_meta.length,u_meta.offset,EQ_OP,username);
	if (rc != OK) return rc;

	REM_RecordHandle rh;
	while ((rc = rfs.GetNextRecord(rh)) == OK) {
		
		char * pData;
		rc = rh.GetData(pData);
		if (rc != OK) return rc;

		char * aData = new char[db_meta.length];
		memcpy(aData,&pData[db_meta.offset],db_meta.length);

		if (strcmp(dbname,aData) == 0) {
			REM_RecordID rid;
			rc = rh.GetRecordID(rid);
			if (rc != OK) return rc;

			rc = rfh.DeleteRecord(rid);
			if (rc != OK) return rc;
		}
	}

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (OK);
}

t_rc SYSM_UserManager::RemoveAllPrivileges (char * username) {
	t_rc rc;

	if (!this->isLoggedIn) return (SYSM_NOTLOGGEDIN);
	if (this->CheckPrivilege("all") != OK) return (SYSM_NOTALLOWEDUSER);

	std::string t_path(this->path);
	t_path.append("privileges");

	AttrMet u_meta;
	rc = this->smm->GetAttributeMetadata("privileges", "username", u_meta);
	if (rc != OK) return rc;

	REM_RecordFileHandle rfh;
	rc = this->rfm->OpenRecordFile((char *) t_path.c_str(),rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,u_meta.type,u_meta.length,u_meta.offset,EQ_OP,username);
	if (rc != OK) return rc;

	REM_RecordHandle rh;
	while ((rc = rfs.GetNextRecord(rh)) == OK) {
		
		REM_RecordID rid;
		rc = rh.GetRecordID(rid);
		if (rc != OK) return rc;

		rc = rfh.DeleteRecord(rid);
		if (rc != OK) return rc;
	}

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (OK);
}