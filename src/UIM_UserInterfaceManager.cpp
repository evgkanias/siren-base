#include "UIM_UserInterfaceManager.h"
#include <iostream>;
#include <fstream>;


UIM_UserInterfaceManager::UIM_UserInterfaceManager() {
	STORM_StorageManager * sm = new STORM_StorageManager();
	REM_RecordFileManager * rfm = new REM_RecordFileManager(sm);
	INXM_IndexManager * im = new INXM_IndexManager(sm);
	this->sysm_Mgr = new SYSM_SystemManager(rfm);
	SYSM_MetaManager * meta_Mgr = new SYSM_MetaManager(rfm,this->sysm_Mgr);
	this->user_Mgr = new SYSM_UserManager(rfm);
	this->ssqlm_ddl_Mgr = new SSQLM_DDL_Manager(rfm,im,meta_Mgr);
	this->ssqlm_dml_Mgr = new SSQLM_DML_Manager(rfm,im,meta_Mgr);
}

UIM_UserInterfaceManager::~UIM_UserInterfaceManager() {}

t_rc UIM_UserInterfaceManager::StartUI() {

	t_rc rc;

	std::string input = "";
	std::string command;
	
	cout << "SirenBase Version 1.1 (SirenExtension)" << endl << endl;
	cout << "Connect to the local database server." << endl;
	cout << "\tType: SirenBase> -u [username] -p [password] to log in." << endl << endl;

	bool loggedIn = false;
	do {
		cout << "SirenBase> ";
		size_t pos;
		getline(cin,input);

		if (strcmp(input.c_str(),"quit;") == 0) return (OK);

		size_t u_pos = input.find("-u");
		size_t p_pos = input.find("-p");

		if (u_pos == string::npos || p_pos == string::npos) {
			loggedIn = false;
			continue;
		}

		std::string username = input.substr(u_pos + 3, p_pos - u_pos - 4);
		std::string password = input.substr(p_pos + 3, input.length() - p_pos - 3);

		rc = this->user_Mgr->LogIn((char *) username.c_str(), (char *) password.c_str());
		if (rc != OK) {
			cout << "\tWrong username or password. Try again." << endl;

			loggedIn = false;
			continue;
		} else
			loggedIn = true;


		while (loggedIn) {
			if (strcmp(command.c_str(),"") == 0)
				cout << "SirenBase> ";
			else
				cout << "\t";
			getline(cin,input);
			if (strcmp(input.c_str(),"") == 0) continue;
		
			command.append(input);

			if (input.find(";") == string::npos) {
				command.append(" ");
				continue;
			}
		
			command = this->FixQuery((char *) command.c_str());
			if (strcmp(command.c_str(),"quit ;") == 0) {
				cout << "Saving your data..." << endl;
				cout << endl;
				this->sysm_Mgr->CloseDatabase();
				this->user_Mgr->LogOut();

				return (OK);
			} else if (command.find("create user ") != string::npos) {
				if ((rc = this->user_Mgr->CheckPrivilege("sirenbase")) != OK) DisplayReturnCode(rc);

				size_t pos;
				std::string n_username;
				std::string n_password;
				command.replace(0,12,"");
				if ((pos = command.find(" with password = ")) != string::npos) {
					n_username = command.substr(0,pos);
					command.replace(0, pos + 17, "");
				} else DisplayReturnCode(UIM_NOTVALIDFORMAT);

				if ((pos = command.find_first_of("\"\'")) != string::npos) {
					size_t end = command.find_last_of("\'\"");
					if (end == string::npos) DisplayReturnCode(UIM_NOTVALIDFORMAT);

					n_password = command.substr(pos+1,end-1);

				} else DisplayReturnCode(UIM_NOTVALIDFORMAT);

				rc = this->user_Mgr->InsertUser((char *) n_username.c_str(), (char *) n_password.c_str());
				if (rc != OK) DisplayReturnCode(rc);

			} else if (command.find("delete user ") != string::npos) {
				if ((rc = this->user_Mgr->CheckPrivilege("sirenbase")) != OK) DisplayReturnCode(rc);

				size_t pos;
				std::string n_username;
				std::string n_password;
				command.replace(0,12,"");
				if ((pos = command.find(" with password = ")) != string::npos) {
					n_username = command.substr(0,pos);
					command.replace(0, pos + 17, "");
				} else DisplayReturnCode(UIM_NOTVALIDFORMAT);

				if ((pos = command.find_first_of("\"\'")) != string::npos) {
					size_t end = command.find_last_of("\'\"");
					if (end == string::npos) {
						DisplayReturnCode(UIM_NOTVALIDFORMAT);
						continue;
					}

					n_password = command.substr(pos+1,end-1);

				} else DisplayReturnCode(UIM_NOTVALIDFORMAT);

				rc = this->user_Mgr->DeleteUser((char *) n_username.c_str(), (char *) n_password.c_str());
				if (rc != OK) DisplayReturnCode(rc);

			} else if (command.find("grant ") != string::npos &&
					   command.find(" privilege") != string::npos &&
					   command.find(" privilege") <= 10) {

				if ((rc = this->user_Mgr->CheckPrivilege("sirenbase")) != OK) DisplayReturnCode(rc);

				rc = this->AddPrivileges((char *) command.c_str());
				if (rc != OK) DisplayReturnCode(rc);
			} else if (command.find("run script ") != string::npos) {

				size_t pos;
				if ((pos = command.find("\"")) == string::npos || (pos = command.find("\'")) == string::npos)
					DisplayReturnCode(UIM_NOTVALIDFORMAT);

				command.replace(0,pos+1,"");
				if ((pos = command.find_last_of("\"")) == string::npos || (pos = command.find_last_of("\'")) == string::npos)
					DisplayReturnCode(UIM_NOTVALIDFORMAT);
				command.replace(pos,command.length() - pos,"");

				rc = this->RunScript((char *) command.c_str());
				if (rc != OK) DisplayReturnCode(rc);
			} else {
				rc = this->ExecuteQuery((char *) command.c_str());
				if (rc != OK) DisplayReturnCode(rc);
			}
		
			if (input.find(";") != string::npos) {
				command = "";
			}
		}
	} while (!loggedIn);
}

t_rc UIM_UserInterfaceManager::ExecuteQuery(char * q) {
		t_rc rc;
		
		std::string query(this->FixQuery(q));

		size_t pos;
		if ((query.find("create database ") != string::npos) ||
				   (query.find("drop database ") != string::npos) ||
				   (query.find("open database ") != string::npos) ||
				   (query.find("close database ") != string::npos)) {

			rc = this->Execute_SYSM_Command((char *) query.c_str());
			if (rc != OK) return rc;

		} else if ((query.find("create table ") != string::npos) ||
				   (query.find("drop table ") != string::npos) ||
				   (query.find("create index ") != string::npos) ||
				   (query.find("drop index ") != string::npos)) {

			rc = this->Execute_SSQLM_DDL_Command((char *) query.c_str());
			if (rc != OK) return rc;

		} else if ((query.find("insert into ") != string::npos) ||
				   (query.find("delete from ") != string::npos) ||
				   (query.find("select ") != string::npos) ||
				   (query.find("update ") != string::npos)) {
			rc = this->Execute_SSQLM_DML_Command((char *) query.c_str());
			if (rc != OK) return rc;
		} else
			DisplayReturnCode(UIM_NOTVALIDFORMAT);

		return (OK);
}

t_rc UIM_UserInterfaceManager::Execute_SYSM_Command(char * command) {
	std::string m_Com(command);	//constructs a string object
	size_t pos;
	t_rc rc;

	std::string mode("create database ");	//constructs a string object
	pos = m_Com.find(mode);
	if (pos != string::npos) {	//means we found that mode
		
		m_Com.replace(m_Com.find(mode),mode.length(),"");//erase the "mode" in order to take the rest command
		pos = m_Com.find_first_of(" ;");
		char * dbName;
		if (pos == string::npos) {
			dbName = new char[m_Com.length()];
			strcpy(dbName,m_Com.c_str());
		} else {
			dbName = new char[pos];
			std::string name =  m_Com.substr(0, pos);
			strcpy(dbName, name.c_str());
		}
		if ((rc = this->user_Mgr->CheckPrivilege(dbName)) != OK) return rc;

		rc = this->sysm_Mgr->CreateDatabase(dbName);
		if (rc != OK) return rc;

		return (OK);
	}

	mode = "drop database ";
	pos = m_Com.find(mode);
	if (pos != string::npos) {

		m_Com.replace(m_Com.find(mode),mode.length(),"");
		pos = m_Com.find_first_of(" ;");
		char * dbName;
		if (pos == string::npos) {
			dbName = new char[m_Com.length()];
			strcpy(dbName,m_Com.c_str());
		} else {
			dbName = new char[pos];
			std::string name =  m_Com.substr(0, pos);
			strcpy(dbName, name.c_str());
		}
		if ((rc = this->user_Mgr->CheckPrivilege(dbName)) != OK) return rc;

		rc =  this->sysm_Mgr->DropDatabase(dbName);
		if (rc != OK) return rc;

		return (OK);
	}

	mode = "open database ";
	pos = m_Com.find(mode);
	if (pos != string::npos) {

		m_Com.replace(m_Com.find(mode),mode.length(),"");
		pos = m_Com.find_first_of(" ;");
		char * dbName;
		if (pos == string::npos) {
			dbName = new char[m_Com.length()];
			strcpy(dbName,m_Com.c_str());
		} else {
			dbName = new char[pos];
			std::string name =  m_Com.substr(0, pos);
			strcpy(dbName, name.c_str());
		}

		if ((rc = this->user_Mgr->CheckPrivilege(dbName)) != OK) return rc;

		rc = this->sysm_Mgr->OpenDatabase(dbName);
		if (rc != OK) return rc;

		rc = this->ssqlm_ddl_Mgr->SetOpenedDatabase(dbName);
		if (rc != OK) return rc;

		rc = this->ssqlm_dml_Mgr->SetOpenedDatabase(dbName);
		if (rc != OK) return rc;

		return (OK);
	}

	mode = "close database ";
	pos = m_Com.find(mode);
	if (pos != string::npos) {

		m_Com.replace(m_Com.find(mode),mode.length(),"");
		pos = m_Com.find_first_of(" ;");
		char * dbName;
		if (pos == string::npos) {
			dbName = new char[m_Com.length()];
			strcpy(dbName,m_Com.c_str());
		} else {
			dbName = new char[pos];
			std::string name =  m_Com.substr(0, pos);
			strcpy(dbName, name.c_str());
		}

		if ((rc = this->user_Mgr->CheckPrivilege(dbName)) != OK) return rc;

		if (strcmp(dbName,this->sysm_Mgr->openedDBName.c_str()) == 0) {
			rc = this->sysm_Mgr->CloseDatabase();
			if (rc != OK) return rc;

			rc = this->ssqlm_ddl_Mgr->SetOpenedDatabase("");
			if (rc != OK) return rc;

			rc = this->ssqlm_dml_Mgr->SetOpenedDatabase("");
			if (rc != OK) return rc;

			return (OK);
		} else return (SYSM_ERRORCLOSEDATABASENAME);
	}

	return (SYSM_ERRORCOMMAND);
}

t_rc UIM_UserInterfaceManager::Execute_SSQLM_DDL_Command(char * command) {
	std::string m_Com(command);
	size_t pos;
	t_rc rc;

	std::string mode("create table ");
	pos = m_Com.find(mode);
	if (pos != string::npos) {
		
		char * t_name;
		char * t_attributes;
		std::string name;
		std::string attrs;
		
		m_Com.replace(m_Com.find(mode),mode.length(),"");
		pos = m_Com.find(" (");
		if (pos != string::npos) {
			size_t end = m_Com.find_last_of(" )");
			name = m_Com.substr(0, pos);
			attrs = m_Com.substr(pos + 3, end - pos - 5);
		} else return (UIM_NOTVALIDFORMAT);

		t_name = (char *) name.c_str();
		t_attributes = (char *) attrs.c_str();
		rc = this->ssqlm_ddl_Mgr->CreateTable(t_name, t_attributes);
		if (rc != OK) return rc;

		return (OK);
	}

	mode = "drop table ";
	pos = m_Com.find(mode);
	if (pos != string::npos) {

		std::string name;
		size_t t_start = pos + mode.length();
		size_t t_length;
		if ((pos = m_Com.find(" ;")) != string::npos)
			t_length = pos - t_start;
		else
			t_length = m_Com.length() - t_start;
		name = m_Com.substr(t_start, t_length);
		char * t_Name = (char *) name.c_str();

		rc =  this->ssqlm_ddl_Mgr->DropTable(t_Name);
		if (rc != OK) return rc;

		return (OK);
	}

	mode = "create index ";
	pos = m_Com.find(mode);
	if (pos != string::npos) {

		size_t t_start = pos + mode.length();
		size_t t_length;
		if ((pos = m_Com.find(" ( ")) != string::npos)
			t_length = pos - t_start;
		else
			return (UIM_NOTVALIDFORMAT);
		std::string name = m_Com.substr(t_start, t_length);
		char * t_Name = (char *) name.c_str();

		t_start = pos + 3;
		t_length = m_Com.find(" ) ") - t_start;
		std::string attr = m_Com.substr(t_start, t_length);
		char * t_Attr = (char *) attr.c_str();

		rc =  this->ssqlm_ddl_Mgr->CreateIndex(t_Name, t_Attr);
		if (rc != OK) return rc;

		return (OK);
	}

	mode = "drop index ";
	pos = m_Com.find(mode);
	if (pos != string::npos) {

		size_t t_start = pos + mode.length();
		size_t t_length;
		if ((pos = m_Com.find(" ( ")) != string::npos)
			t_length = pos - t_start;
		else
			return (UIM_NOTVALIDFORMAT);
		std::string name = m_Com.substr(t_start, t_length);
		char * t_Name = (char *) name.c_str();

		t_start = pos + 3;
		t_length = m_Com.find(" ) ") - t_start;
		std::string attr = m_Com.substr(t_start, t_length);
		char * t_Attr = (char *) attr.c_str();

		rc =  this->ssqlm_ddl_Mgr->DropIndex(t_Name, t_Attr);
		if (rc != OK) return rc;

		return (OK);
	}

	return (SSQLM_DDL_ERRORCOMMAND);
}

t_rc UIM_UserInterfaceManager::Execute_SSQLM_DML_Command(char * command) {
	std::string m_Com(command);
	size_t pos;
	t_rc rc;
	char * subconditions = new char[2000];
	bool noSubconditions = false;

	std::string mode("select ");
	pos = m_Com.find(mode);
	if (pos != string::npos) {

		m_Com.replace(0, mode.length() - 1, "");
		//get SELECTS attributes list
		char * selectList = new char[1500];
		size_t t_length = m_Com.find(" from ") + 1;
		m_Com.copy(selectList, t_length, 0);
		selectList[t_length] = '\0';

		m_Com.replace(0, t_length + 4, "");
		//get from tables
		char * fromTables = new char[1000];
		t_length = m_Com.find(" where ");
		if(t_length == string::npos){
			t_length = m_Com.find(";") - 1;
			noSubconditions = true;
		}
		t_length++;
		m_Com.copy(fromTables, t_length, 0);
		fromTables[t_length] = '\0';

		m_Com.replace(0, t_length + 5, "");

		if(noSubconditions){
			subconditions[0] = '\0';
		} else {
			//get subconditions
			t_length = m_Com.length()-1;
			m_Com.copy(subconditions, t_length, 0);
			subconditions[t_length] = '\0';			
		}

		rc = this->ssqlm_dml_Mgr->SelectRecord(selectList, fromTables, subconditions);
		if(rc != OK) return rc;
	}

	mode = "insert into ";
	pos = m_Com.find(mode);
	if (pos != string::npos) {
		m_Com.replace(0, mode.length(), "");
		//get tables name
		size_t t_start = 0;
		size_t t_length = m_Com.find(" ") - t_start;
		char * tableName = new char[255];
		m_Com.copy(tableName, t_length, t_start);
		tableName[t_length] = '\0';
		
		//get values		
		char * valuesList = new char [1000];
		t_start = m_Com.find_first_of("(") + 1;
		t_length = m_Com.find_last_of(")") - t_start;
		m_Com.copy(valuesList, t_length, t_start);
		valuesList[t_length] = '\0';
		
		rc = this->ssqlm_dml_Mgr->InsertRecord(tableName, valuesList);
		if(rc != OK) return rc;
	}

	mode = "delete from ";
	pos = m_Com.find(mode);
	if (pos != string::npos) {
		m_Com.replace(0, mode.length(), "");
		//get tables name
		size_t t_pos = m_Com.find(" ");
		char * tableName = new char[255];
		m_Com.copy(tableName, t_pos, 0);
		tableName[t_pos] = '\0';

		m_Com.replace(0, m_Com.find("where "), "");
		//also replace the "where ".
		m_Com.replace(0, m_Com.find(" ")+1, "");
		//get condition
		char * condition = new char[1000];
		m_Com.copy(condition, m_Com.length()-2, 0);
		condition[m_Com.length()-2] = '\0';

		rc = this->ssqlm_dml_Mgr->DeleteRecord(tableName, condition);
		if(rc != OK) return rc;
	}

	mode = "update ";
	pos = m_Com.find(mode);
	if (pos != string::npos) {
		m_Com.replace(0, mode.length(), "");
		//get tables name
		size_t t_pos = m_Com.find(" ");
		char * tableName = new char[255];
		m_Com.copy(tableName, t_pos, 0);
		tableName[t_pos] = '\0';

		m_Com.replace(0, m_Com.find("set "), "");
		m_Com.replace(0, m_Com.find(" ")+1, "");
		//get attribute and new value
		char * attribute = new char[255];
		char * value = new char[255];
		m_Com.copy(attribute, m_Com.find("=")-1, 0);
		attribute[m_Com.find("=")-1] = '\0';

		m_Com.replace(0, m_Com.find(" = ")+3, "");
		m_Com.copy(value, m_Com.find(" "), 0);
		value[m_Com.find(" ")] = '\0';
		
		m_Com.replace(0, m_Com.find("where "), "");
		m_Com.replace(0, m_Com.find(" ")+1, "");

		//get condition
		char * condition = new char[1000];
		m_Com.copy(condition, m_Com.length()-1, 0);
		condition[m_Com.length()-2] = '\0';
		
		rc = this->ssqlm_dml_Mgr->UpdateRecord(tableName, attribute, value, condition);
		if(rc != OK) return rc;
	}
	
	return (OK);
}

t_rc UIM_UserInterfaceManager::RunScript(char * fileName) {
	t_rc rc;

	std::string line;
	std::string query;
	ifstream script(fileName);

	if (script.is_open()) {

		size_t pos;
		while (script.good()) {
			
			getline(script,line);

			if (line.c_str()[0] == '-' && line.c_str()[1] == '-')
				continue;
	
			query.append(line);
			query.append(" ");

			while ((pos = query.find(";")) != string::npos ||
				(pos = query.find(" go ")) != string::npos ||
				(pos = query.find(" GO ")) != string::npos ||
				(pos = query.find(" Go ")) != string::npos ||
				(pos = query.find(" gO ")) != string::npos) {
					
				if (line.find(";") == string::npos) {
					pos++;
					query.replace(pos, 2, ";");
				}
				pos++;

				rc = this->ExecuteQuery((char *) query.c_str());
				if (rc != OK) return rc;

				query.replace(0, pos, "");
			}
		}

		script.close();
	} else {
		return (UIM_SCRIPTNOTEXISTS);
	}

	return (OK);
}

t_rc UIM_UserInterfaceManager::AddPrivileges(char * command) {
	t_rc rc;
	size_t pos;
	std::string com(command);

	if ((pos = com.find("grant all privileges to ")) != string::npos) {
		pos += 23;
		com.replace(0,pos,"");

		pos = com.find(" ;");
		std::string username = com.substr(0,pos);

		rc = this->user_Mgr->AddPrivilege((char *) username.c_str(), "all");
		if (rc != OK) return rc;

		return (OK);
	} else if ((pos = com.find("grant privilege on ")) != string::npos) {
		pos += 19;
		com.replace(0,pos,"");

		pos = com.find(" to ");
		std::string dbname = com.substr(0,pos);

		com.replace(0,pos+4,"");
		pos = com.find(" ;");
		std::string username = com.substr(0,pos);

		rc = this->user_Mgr->AddPrivilege((char *) username.c_str(), (char *) dbname.c_str());
		if (rc != OK) return rc;

		return (OK);
	}

	return (UIM_NOTVALIDFORMAT);
}

char * UIM_UserInterfaceManager::FixQuery(char * query) {
	t_rc rc;
	bool pen = true;
	int differ = 'A' - 'a';
	char ch;
	
	size_t pos;
	std::string str(query);
	while ((pos = str.find("\t")) != string::npos) str.replace(pos,1," ");
	while (str.find(" ") == 0) str.replace(0,1,"");

	query = new char[str.length()];
	memcpy(query,str.c_str(),strlen(query));

	for (int i = 0; i < strlen(query); i++) {
		if (query[i] == '\"' || query[i] == '\'') {
			if (pen) pen = false;
			else pen = true;
		} else {
			memcpy(&ch, query+i, sizeof(char));
			if (pen && ch >= 'A' && ch <= 'Z') {
				ch = ch - differ;
				memcpy(query+i, &ch, sizeof(char));
			}
			if ((this->changeEQ(query[i],query[i-1],true) ||
				this->changeNE(query[i],query[i-1]) ||
				this->changeLT(query[i],query[i-1],true) ||
				this->changeGT(query[i],query[i-1],true) ||
				this->changeOther(query[i],query[i-1],true)) && i > 0) {
				
				std::string str(query);
				str.replace(i,0," ");

				query = new char[str.length()];
				memcpy(query,str.c_str(),strlen(query));
				query[str.length()] = '\0';

			}

			if ((this->changeEQ(query[i],query[i+1],false) ||
				this->changeLT(query[i],query[i+1],false) ||
				this->changeGT(query[i],query[i+1],false) ||
				this->changeOther(query[i],query[i+1],false)) && i+1 < strlen(query)) {
				
				std::string str(query);
				str.replace(i+1,0," ");

				query = new char[str.length()];
				memcpy(query,str.c_str(),strlen(query));

			}

			if (query[i] == ';' && query[i+1] == ' ')
				query[i+1] = '\0';

		}
	}

	str = query;
	while((pos = str.find("  ")) != string::npos) str.replace(pos,2," ");
	
	while ((pos = str.find(" int ")) != string::npos)
		str.replace(pos,5," INT ");
	while ((pos = str.find(" float ")) != string::npos)
		str.replace(pos,5," FLOAT ");
	while ((pos = str.find(" double ")) != string::npos)
		str.replace(pos,5," DOUBLE ");
	while ((pos = str.find(" char ")) != string::npos)
		str.replace(pos,6," CHAR ");

	query = new char[str.length()];
	memcpy(query,str.c_str(),strlen(query));

	return query;
}

bool UIM_UserInterfaceManager::changeEQ(char ch, char ch_1, bool mode) {
	if (mode) return ch == '=' && ch_1 != ' ' && ch_1 != '=' && ch_1 != '<' && ch_1 != '>' && ch_1 != '!';
	else return ch == '=' && ch_1 != ' ' && ch_1 != '=' && ch_1 != '<' && ch_1 != '>';
}

bool UIM_UserInterfaceManager::changeNE(char ch, char ch_1) {
	return ch == '!' && ch_1 != ' ';
}

bool UIM_UserInterfaceManager::changeLT(char ch, char ch_1, bool mode) {
	if (mode) return ch == '<' && ch_1 != ' ' && ch_1 != '=';
	else return ch == '<' && ch_1 != ' ' && ch_1 != '=' && ch_1 != '>';
}

bool UIM_UserInterfaceManager::changeGT(char ch, char ch_1, bool mode) {
	if (mode) return ch == '>' && ch_1 != ' ' && ch_1 != '=' && ch_1 != '<';
	else return ch == '>' && ch_1 != ' ' && ch_1 != '=';
}

bool UIM_UserInterfaceManager::changeOther(char ch, char ch_1, bool mode) {
	if (mode) return (ch == ';' || ch == ',' || ch == '(' || ch == ')') && ch_1 != ' ';
	else return (ch == ',' || ch == '(' || ch == ')') && ch_1 != ' ';
}
