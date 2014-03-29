#include "SYSM_SystemManager.h"

SYSM_SystemManager::SYSM_SystemManager(REM_RecordFileManager * rfm) {
	this->path = "C:\\sirenbase\\data\\";
	this->rfm = rfm;
	this->isOpen = false;
	this->openedDBName = "";
}

SYSM_SystemManager::~SYSM_SystemManager() {}

t_rc SYSM_SystemManager::CreateDatabase(char * dbName) {
	t_rc rc;

	// Φτιάχνουμε το φάκελο για τα αρχεία της βάσης

	_mkdir((this->path + dbName).c_str());

	if (errno == EEXIST) {
		return (SYSM_DATABASEEXISTS);
	} else if (errno == ENOENT) {
		return (SYSM_PATHNOTFOUND);
	}

	// Φτιάχνουμε το αρχείο rel.met
	std::string relPath = this->path + dbName + "\\rel.met";

	rc = this->rfm->CreateRecordFile(relPath.c_str(),REL_SIZE);
	if (rc != OK) return rc;

	// Φτιάχνουμε το αρχείο attr.met
	std::string attrPath = this->path + dbName + "\\attr.met";

	rc = this->rfm->CreateRecordFile(attrPath.c_str(),ATTR_SIZE);
	if (rc != OK) return rc;

	return (OK);
}

t_rc SYSM_SystemManager::DropDatabase (char * dbName) {
	
	t_rc rc;
	ifstream db_File;

	HANDLE hFile;
	WIN32_FIND_DATA FileInfo;
	std::string pattern(this->path + dbName + "\\*.*");
	std::wstring filePattern(pattern.begin(),pattern.end());
	std::string filePath;

	hFile = ::FindFirstFile(filePattern.c_str(), &FileInfo);
	if (hFile != INVALID_HANDLE_VALUE) {
		do {
			if (FileInfo.cFileName[0] != '.') {
				filePath.erase();
				std::wstring fileName(FileInfo.cFileName);
				std::string fName(fileName.begin(),fileName.end());

				filePath = this->path + dbName + "\\" + fName;

				rc = this->rfm->DestroyRecordFile(filePath.c_str());
				if (rc != OK) return rc;
			}
		} while (FindNextFile(hFile, &FileInfo) == true);
	}

	_rmdir((this->path + dbName).c_str());

	if (errno == ENOTEMPTY) {
		return (SYSM_DIRECTORYNOTEMPTY);
	} else if (errno == ENOENT) {
		return (SYSM_PATHNOTFOUND);
	} else if (errno == 13) {
		return (SYSM_NOACCESSTODIRECTORY);
	}
	
	return (OK);
}

t_rc SYSM_SystemManager::OpenDatabase (char * dbName) {

	if (this->isOpen) return (SYSM_ANOTHERDATABASEISOPEN);

	_mkdir((this->path + dbName).c_str());

	if (errno != EEXIST) {
		_rmdir((this->path + dbName).c_str());
		return (SYSM_ERROROPENDATABASE);
	}
	
	this->openedDBName = dbName;
	this->isOpen = true;

	return (OK);
}

t_rc SYSM_SystemManager::CloseDatabase () {

	if (!this->isOpen) return (SYSM_NOTOPENEDDATABASE);
	
	this->openedDBName = "";
	this->isOpen = false;

	return (OK);
}
