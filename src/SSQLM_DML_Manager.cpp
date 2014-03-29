#include "SSQLM_DML_Manager.h"
#include "REM_Types.h"
#include "SYSM_Parameters.h"
#include "SYSM_Parameters.h"
#include <iostream>


SSQLM_DML_Manager::SSQLM_DML_Manager(REM_RecordFileManager * rfm, INXM_IndexManager * im, SYSM_MetaManager * smm) {
	this->rfm = rfm;
	this->im = im;
	this->smm = smm;
}

SSQLM_DML_Manager::~SSQLM_DML_Manager(void){}

t_rc SSQLM_DML_Manager::SetOpenedDatabase(char * db_Name) {
	this->openedDatabaseName = db_Name;
	this->path =  "C:\\sirenbase\\data\\" + this->openedDatabaseName + "\\";
	return (OK);
}

t_rc SSQLM_DML_Manager::SelectRecord(char * selectAttrNames,char * fromTables, char * conditions){
	
	t_rc rc;
	//*
	size_t foundAND;

	//---QUERY VALIDATION---1st stage//
	if (strcmp(selectAttrNames," * ") != 0) {
		rc = queryValidationStageOne(selectAttrNames, fromTables);	
		if (rc != OK) return rc;
	}

	//---QUERY VALIDATION---2nd stage//
	//check if WHERE attributes exist in the FROM table
	rc = validateWhereSubconditions(conditions, fromTables);
	if (rc != OK) return rc;
	
	std::string selectAttrList(selectAttrNames);			//turn the "SELECT" attributes list into string
	std::string fromTablesAsString(fromTables);				//turn the "FROM" tables list into string
	std::string subConditionsList(conditions);				//turn the "WHERE" conditions into string

	rc = this->CreateTempTable(fromTables);
	if (rc != OK) {
		rc = this->DropTempTable();
		if (rc != OK) return rc;

		rc = this->CreateTempTable(fromTables);
		if (rc != OK) return rc;
	}
	
	//start executing the first subcondition
	char currentSubCondition[225] = "\0";
	char * leftAttr = new char[225], * rightAttr = new char[225];
	char currOperator[4] = "\0";
	t_compOp currOp;

	leftAttr[0] = '\0';
	rightAttr[0] = '\0';

	if(subConditionsList.length() != 0){
		while (true) {
			//begin by decomposing the sub-condition
			foundAND = subConditionsList.find(" and ");
			if(foundAND != string::npos){
				subConditionsList.copy(currentSubCondition, foundAND + 1, 0);
				currentSubCondition[foundAND+1] = '\0';
				subConditionsList.replace(0, foundAND+4, "");
			}else{
				subConditionsList.copy(currentSubCondition, subConditionsList.length(), 0);
				currentSubCondition[subConditionsList.length()] = '\0';
				subConditionsList.replace(0, subConditionsList.length(), "");
			}
	
			//get the compOp (in currOp) as t_compOp.
			rc = reconCompOp(currentSubCondition, currOperator, currOp);
			if (rc != OK) return rc;

			//break the subocndition apart and get left attr and right attr
			rc = breakSubCond(currentSubCondition,currOperator, leftAttr, rightAttr);
			if (rc != OK) return rc;

			std::string la(leftAttr);
			std::string ra(rightAttr);

			la.replace(0,1,"");
			la.replace(la.length()-1,1,"");

			ra.replace(0,1,"");
			ra.replace(ra.length()-1,1,"");

			leftAttr = new char[la.length()];
			rightAttr = new char[ra.length()];

			memcpy(leftAttr,la.c_str(),strlen(leftAttr));
			memcpy(rightAttr,ra.c_str(),strlen(rightAttr));

			if (rightAttr[0] == '\"' || rightAttr[0] == '\'') {
				std::string value(rightAttr);
				size_t pos;
				if ((pos = value.find_first_of("\'")) == string::npos)
					pos = value.find_first_of("\"");
				value.replace(pos,1,"");
				value.replace(value.length() - 1, 1, "");
				strcpy(rightAttr,value.c_str());

				rc = this->executeSubconditionValue(leftAttr,rightAttr,currOp);

			} else if (rightAttr[0] == '0' || rightAttr[0] == '1' || 
				rightAttr[0] == '2' || rightAttr[0] == '3' || 
				rightAttr[0] == '4' || rightAttr[0] == '5' || 
				rightAttr[0] == '6' || rightAttr[0] == '7' || 
				rightAttr[0] == '8' || rightAttr[0] == '9') {
					
					AttrMet aMeta;
					char * currRelName = new char[255];
					char * currAttrName = new char[255];
					la.copy(currRelName, la.find("."), 0);
					currRelName[la.find(".")] = '\0';
					la.copy(currAttrName, la.length(), la.find(".")+1);
					currAttrName[la.length() - la.find(".")-1] = '\0';
					rc = this->smm->GetAttributeMetadata(currRelName, currAttrName, aMeta);

					if(aMeta.type == TYPE_INT){
						int value = atoi(rightAttr);
						rc = this->executeSubconditionValue(leftAttr,&value,currOp);
					} else {
						double value = atof(rightAttr);
						rc = this->executeSubconditionValue(leftAttr,&value,currOp);
					}

				

			} else {
				rc = this->executeSubcondition(leftAttr,rightAttr,currOp);
			}

			if (rc != OK) return rc;

			if (foundAND == string::npos) break;
		}
	}
	

	rc = this->showTable(TEMP_T,selectAttrNames);
	if (rc != OK) return rc;
	//*/
	rc = this->DropTempTable();
	if (rc != OK) return rc;

	return (OK);
}

t_rc SSQLM_DML_Manager::queryValidationStageOne(char * selectAttrNames,char * fromTables){

	t_rc rc;
	std::string retNames(selectAttrNames);
	
	char pData[200];
	bool selectTempFileCreated = false;
	size_t foundComma, foundComma2, foundDot, foundCompop, foundAND, stringLength;
	std::string selectAttrList(selectAttrNames);			//turn the "SELECT" attributes list into string
	std::string fromTablesAsString(fromTables);				//turn the "FROM" tables list into string
	
	do{		//proccess the SELECT attributes

		bool select_from = false;
		char fullAtribute[150], table[100], attrName[100];

		//break attributes

		foundComma = selectAttrList.find(",");
		if(foundComma != string::npos){	//if not the SELECT's last attribute
			selectAttrList.copy(fullAtribute, foundComma, 0);
			fullAtribute[foundComma] = '\0';
		} else {						//SELECT's last atrribute		
			selectAttrList.copy(fullAtribute, selectAttrList.length(), 0);
			fullAtribute[selectAttrList.length()] = '\0';
		}

		//keep one attribute
		std::string fullAttributeString(fullAtribute);			
		
		//check SELECT's attibutes format.
		foundDot = fullAttributeString.find(".");
		if (foundDot != string::npos){		//"table.attr"-like
				
			//get table from attribute
			fullAttributeString.copy(table, foundDot, 0);
			table[foundDot] = ' ';
			table[foundDot+1] = '\0';
			fullAttributeString.replace(0, foundDot, "");
			fullAttributeString.replace(0, 1, " ");
			//get attributes Name
			fullAttributeString.copy(attrName, fullAttributeString.length(), 0);
			attrName[fullAttributeString.length()] = '\0';

			//compare to FROM tables
			//compare table.attr with FROM tables
			do{		//"walk" all FROM tables.			
				
				char currFromTable[255];
				
				foundComma2 = fromTablesAsString.find(",");
				if(foundComma2 != string::npos){
										
					fromTablesAsString.copy(currFromTable, foundComma2, 0);
					currFromTable[foundComma2] = '\0';
					fromTablesAsString.replace(0, foundComma2 +1, "");
				}else{		//handle the last table on the FROM list
					fromTablesAsString.copy(currFromTable, fromTablesAsString.length(), 0);
					currFromTable[fromTablesAsString.length()] = '\0';
					fromTablesAsString.replace(0, fromTablesAsString.length(),"");
				}
				
				if(strcmp(table,currFromTable) == 0){
					select_from = true;
					break;	//since we found a match we dont check the rest FROM statement.
				}

			}while(fromTablesAsString.compare("") != 0);

			if(!select_from){	//we have not found a match
				return SSQLM_DML_SELECT_FROM_STATEMENTS_ERROR;
			}

			//once we have run through all the FROM tables, we restore the string for the next attribute.
			fromTablesAsString.assign(fromTables);			
		}else{		//"attr"-like  format! (sketo)

			//get its table and check if it is found on two tables

			//cleanattributes spaces from start and end
			std::string attrNameString(fullAttributeString);

			attrNameString.replace(0, 1, "");
			attrNameString.replace(attrNameString.length()-1, 1, "");
			char * trimedAttrName = (char *) attrNameString.c_str();
			rc = scanFromTables(attrNameString, fromTables, table, trimedAttrName, pData, &select_from);
			if (rc != OK) return rc;

			std::string a_name(fullAttributeString);
			std::string n_data(" ");
			n_data.append(pData);
			n_data.append(" ");
			retNames.replace(retNames.find(fullAttributeString), a_name.length(),n_data.c_str());
		}
		
		if(foundComma != string::npos){	//if not the last SELECT's attribute			
			selectAttrList.replace(0, foundComma+1, "");	//prepare for the next attribure on the list
		} else {						//last SELECT's atrribute
			selectAttrList.replace(0, selectAttrList.length(), "");
		}
	}while(selectAttrList.compare("") != 0);	//here, we are done with SELECT-FROM "relation"

	strcpy(selectAttrNames,retNames.c_str());

	return (OK);
}

//very much alike processAttribute but NOT the same
t_rc SSQLM_DML_Manager::validateWhereSubconditions(char * conditions, char * fromTables){
	
	t_rc rc;
	bool where_from = false;
	size_t foundComma2, foundAND, foundDot;
	char table[100];
	char pData[200];

	std::string subConditionsList(conditions);
	std::string fromTablesAsString(fromTables);
	std::string retConditions(conditions);

	while(subConditionsList.compare("") != 0){	//FOR ALL SUBCONDITIONS
		char currentSubCondition[530];
		char rightWhereAttr[255], leftWhereAttr[255];

		foundAND = subConditionsList.find(" and ");
		if(foundAND != string::npos){
			subConditionsList.copy(currentSubCondition, foundAND+1, 0);
			currentSubCondition[foundAND+1] = '\0';
			subConditionsList.replace(0, foundAND+4, "");
		}else{
			subConditionsList.copy(currentSubCondition, subConditionsList.length(), 0);
			currentSubCondition[subConditionsList.length()] = '\0';
			subConditionsList.replace(0, subConditionsList.length(), "");
		}

		
		char currOperator[4];
		char * pData = new char[500];
		t_compOp currOpTemp;
		char whereAttrTable[255], whereAttrName[255];
		bool where_from = false;

		//get the compOperator
		rc = reconCompOp(currentSubCondition, currOperator, currOpTemp);
		if (rc != OK) return rc;
		//get the attributes
		rc = breakSubCond(currentSubCondition, currOperator, leftWhereAttr, rightWhereAttr);
		if (rc != OK) return rc;
		
		std::string lwa(leftWhereAttr);
		rc = queryValidationStageOne(leftWhereAttr, fromTables);
		if (rc != OK) return SSQLM_DML_SELECT_WHERE_STATEMENTS_ERROR;

		retConditions.replace(retConditions.find(lwa.c_str()),lwa.length(),leftWhereAttr);
		
		std::string rwa(rightWhereAttr);
		if (rwa.find_first_of("\"") == string::npos && rwa.find_first_of("\'") == string::npos &&
			rwa.find_first_of("1") == string::npos && rwa.find_first_of("2") == string::npos &&
			rwa.find_first_of("3") == string::npos && rwa.find_first_of("4") == string::npos &&
			rwa.find_first_of("5") == string::npos && rwa.find_first_of("6") == string::npos &&
			rwa.find_first_of("7") == string::npos && rwa.find_first_of("8") == string::npos &&
			rwa.find_first_of("9") == string::npos && rwa.find_first_of("0") == string::npos) {
			
			rc = queryValidationStageOne(rightWhereAttr, fromTables);
			if (rc != OK) return SSQLM_DML_SELECT_WHERE_STATEMENTS_ERROR;

			retConditions.replace(retConditions.find(rwa.c_str()),rwa.length(),rightWhereAttr);
		}
	}
	strcpy(conditions,retConditions.c_str());

	return (OK);
}

t_rc SSQLM_DML_Manager::reconCompOp(char * currentSubCondition,char currOperator[4], t_compOp & currOp){

	t_rc rc;
	size_t foundCompOp;
	std::string currSubcondAsString(currentSubCondition);

	foundCompOp = currSubcondAsString.find("<");
	if(foundCompOp != string::npos){
		currSubcondAsString.copy(currOperator, 1, foundCompOp);
		currOperator[1] = '\0';
		currOp = LT_OP;
	}

	foundCompOp = currSubcondAsString.find(">");
	if (foundCompOp != string::npos){
		currSubcondAsString.copy(currOperator, 1, foundCompOp);
		currOperator[1] = '\0';
		currOp = GT_OP;
	}

	
	foundCompOp = currSubcondAsString.find("=");
	if (foundCompOp != string::npos){
		currSubcondAsString.copy(currOperator, 1, foundCompOp);
		currOperator[1] = '\0';
		currOp = EQ_OP;
	}

	foundCompOp = currSubcondAsString.find("==");
	if (foundCompOp != string::npos){
		currSubcondAsString.copy(currOperator, 2, foundCompOp);
		currOperator[2] = '\0';
		currOp = EQ_OP;
	}


	foundCompOp = currSubcondAsString.find("<=");
	if (foundCompOp != string::npos){
		currSubcondAsString.copy(currOperator, 2, foundCompOp);
		currOperator[2] = '\0';
		currOp = LE_OP;
	}

	foundCompOp = currSubcondAsString.find(">=");
	if (foundCompOp != string::npos){
		currSubcondAsString.copy(currOperator, 2, foundCompOp);
		currOperator[2] = '\0';
		currOp = GE_OP;
	}

	foundCompOp = currSubcondAsString.find("!=");
	if (foundCompOp != string::npos){
		currSubcondAsString.copy(currOperator, 2, foundCompOp);
		currOperator[2] = '\0';
		currOp = NE_OP;
	}

	foundCompOp = currSubcondAsString.find("<>");
	if (foundCompOp != string::npos){
		currSubcondAsString.copy(currOperator, 2, foundCompOp);
		currOperator[2] = '\0';
		currOp = NE_OP;
	}
	
	return OK;
}

t_rc SSQLM_DML_Manager::breakSubCond(char * currentSubCondition,char currOperator[4], char * leftAttr, char * rightAttr){
	
	t_rc rc;
	int operatorLength;

	size_t foundCompOp, foundEqual;
	std::string currSubcondAsString(currentSubCondition);
	std::string currOperatorAsString(currOperator);
	
	operatorLength = currOperatorAsString.length();

	foundCompOp = currSubcondAsString.find(currOperator);
	if(foundCompOp != string::npos){
		currSubcondAsString.copy(leftAttr, foundCompOp, 0);
		leftAttr[foundCompOp] = '\0';

		currSubcondAsString.replace(0, foundCompOp+operatorLength, "");

		strcpy(rightAttr, currSubcondAsString.c_str() );

	}else{
		return SSQLM_DML_BAD_COMP_OP;
	}
	
	return OK;
}

t_rc SSQLM_DML_Manager::checkAttrType(char * table, char * name, t_attrType *attrType){

	t_rc rc;	
	REM_RecordFileScan rfs;
	REM_RecordFileHandle rfh;
	REM_RecordHandle rh;
			
	char currDBName[100];
	strcpy(currDBName, this->path.c_str());
	strcat(currDBName, "attr.met");
	rc = this->rfm->OpenRecordFile(currDBName, rfh);
	if (rc != OK) return rc;

	rc = rfs.OpenRecordScan(rfh, TYPE_STRING, ATTR_NAME_SIZE, ATTR_NAME_OFFSET, EQ_OP, name);
	if (rc != OK) return rc;

	bool done = false;	
	while ((rc = rfs.GetNextRecord(rh)) == OK) {

		char * pData;
		rc = rh.GetData(pData);
		if (rc != OK) return rc;

		char table2[255];
		memcpy(table2, &pData[ATTR_REL_NAME_OFFSET], REL_NAME_SIZE);
		memcpy(attrType, &pData[ATTR_TYPE_OFFSET], sizeof(t_attrType) );
		if( (strcmp(table2, table) ==0 )){
			done = true;
			break;
		}
	}

	if (!done) return SSQLM_DML_ERROR_GETTING_ATTRIBUTES_TYPE;

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	return OK;
}

//finds the table an atribute belongs to
t_rc SSQLM_DML_Manager::scanFromTables(const std::string &fullAttributeString, char * fromTables,
										char * table, char * attrName, char * pData, bool * select_from){

	t_rc rc;
	size_t pos;
	size_t foundComma2;
	std::string fromTablesAsString;	

	fullAttributeString.copy(attrName, fullAttributeString.length(), 0);
	attrName[fullAttributeString.length()] = '\0';

	//search for its table!!!
	REM_RecordFileScan rfs;
	REM_RecordFileHandle rfh;
	REM_RecordHandle rh;
			
	char finalTable[100];
	char currDBName[100];
	strcpy(currDBName, this->path.c_str());
	strcat(currDBName, "attr.met");
	rc = this->rfm->OpenRecordFile(currDBName, rfh);
	if (rc != OK) return rc;

	rc = rfs.OpenRecordScan(rfh, TYPE_STRING, ATTR_NAME_SIZE, ATTR_NAME_OFFSET, EQ_OP, attrName);
	if (rc != OK) return rc;

	bool done = false;	//flag: if we already have found the table
	//for all the records of metadata that match
	while ((rc = rfs.GetNextRecord(rh)) == OK) {

		char * pData;
		rc = rh.GetData(pData);
		if (rc != OK) return rc;

		//gets the tables name the attr belongs to!
		memcpy(table, &pData[ATTR_REL_NAME_OFFSET], REL_NAME_SIZE);
		
		fromTablesAsString = fromTables;
		if (rc != OK) return rc;

		//trim spaces on FROM statement
		while ((pos = fromTablesAsString.find(" ")) != string::npos)
			fromTablesAsString.replace(pos, 1, "");

		// An done == true kai vrisketai sto table tote error;
		// An to table yparxei sto FROM done = true;
		//in this do loop, we check if the table found above, also exists in FROM statement!
		do{		//"walk" all FROM tables.

			char currFromTable[255];
					
			foundComma2 = fromTablesAsString.find(",");
			if(foundComma2 != string::npos){
				fromTablesAsString.copy(currFromTable, foundComma2, 0);
				currFromTable[foundComma2] = '\0';
				fromTablesAsString.replace(0, foundComma2 +1,"");
			}else{		//handle the last table on the FROM list
				fromTablesAsString.copy(currFromTable, fromTablesAsString.length(), 0);
				currFromTable[fromTablesAsString.length()] = '\0';
				fromTablesAsString.replace(0, fromTablesAsString.length(),"");
			}
					
			if(strcmp(table,currFromTable) == 0){
				bool test = true;
				select_from = &test;
				if(done){	//to exw vrei idi mia fora.
					rc = SSQLM_DML_SELECT_ATTR_FOUND_IN_TWO_TABLES;
					return rc;
				}
				strcpy(finalTable,table);
				done = true;
			}
										
		}while(fromTablesAsString.compare("") != 0);
	}

	//in case we have a value and no table was found
	if(!*select_from){
		strcpy(finalTable, "NO_TABLE");
	}

	//strore the names on pData for the file we are going to create
	strcpy(pData, finalTable);
	strcat(pData, ".");
	strcat(pData, attrName);

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;
			
	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (OK);
}

t_rc SSQLM_DML_Manager::CreateTempTable(char * fromTables) {
	
	t_rc rc;
	RelMet tr_met;
	char * t1, * t2, * t3;
	t1 = new char[255]; t1[0] = '\0';
	t2 = new char[255]; t2[0] = '\0';
	t3 = new char[255]; t3[0] = '\0';

	tr_met.name = new char[10];
	tr_met.name = TEMP_T;
	tr_met.numAttrs = 0;
	tr_met.numOfIndexes = 0;
	tr_met.rs = 0;

	std::string tables(fromTables);
	int countTables = 0;

	size_t pos;
	while ((pos = tables.find(" ")) != string::npos)
		tables.replace(pos, 1, "");

	std::string t_path(this->path + tr_met.name);
	std::string a_path(this->path + "attr.met");

	REM_RecordFileHandle rfh;
	rc = this->rfm->OpenRecordFile(a_path.c_str(),rfh);
	if (rc != OK) return rc;

	while (true) {
		countTables++;
		
		char * t_name = new char[255];
		pos = tables.find(",");

		if (pos != string::npos) {
			tables.copy(t_name, pos, 0);
			tables.replace(0, pos + 1, "");
			t_name[pos] = '\0';
		} else {
			tables.copy(t_name, tables.length(), 0);
			t_name[tables.length()] = '\0';
		}

		switch (countTables) {
		case 1:
			t1 = t_name;
			break;
		case 2:
			t2 = t_name;
			break;
		case 3:
			t3 = t_name;
			break;
		default:
			return (SSQLM_DML_TOOMANYTABLES);
		}

		RelMet r_meta;
		rc = this->smm->GetRelationMetadata(t_name,r_meta);
		if (rc != OK) return rc;

		tr_met.numAttrs += r_meta.numAttrs;
		tr_met.rs += r_meta.rs;
		tr_met.numOfIndexes += r_meta.numOfIndexes;

		REM_RecordFileScan rfs;
		rc = rfs.OpenRecordScan(rfh,TYPE_STRING,REL_NAME_SIZE,ATTR_REL_NAME_OFFSET,EQ_OP,t_name);
		if (rc != OK) return rc;

		REM_RecordHandle rh;
		while (rfs.GetNextRecord(rh) == OK) {
			char * rData = new char[ATTR_SIZE];
			rc = rh.GetData(rData);
			if (rc != OK) return rc;

			char * a_name = new char[ATTR_NAME_SIZE];
			memcpy(a_name,&rData[ATTR_NAME_OFFSET],ATTR_NAME_SIZE);

			AttrMet a_meta;
			rc = this->smm->GetAttributeMetadata(t_name,a_name,a_meta);
			if (rc != OK) return rc;

			char * aData = new char[ATTR_SIZE];
			std::string n_name(t_name);
			n_name.append("."); n_name.append(a_name);
			char * nn_name = new char[ATTR_NAME_SIZE];
			n_name.copy(nn_name,n_name.length(),0);
			nn_name[n_name.length()] = '\0';
			
			rc = this->rfm->CloseRecordFile(rfh);
			if (rc != OK) return rc;

			rc = this->smm->InsertAttributeMetadata(
				TEMP_T,
				nn_name,
				tr_met.rs - r_meta.rs + a_meta.offset,
				a_meta.type,
				a_meta.length,
				a_meta.indexNo);
			if (rc != OK) return rc;

			rc = this->rfm->OpenRecordFile(a_path.c_str(),rfh);
			if (rc != OK) return rc;

		}

		rc = rfs.CloseRecordScan();
		if (rc != OK) return rc;

		if (pos == string::npos) break;
	}

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	rc = this->smm->InsertRelationMetadata(tr_met.name,tr_met.rs,tr_met.numAttrs,tr_met.numOfIndexes);
	if (rc != OK) return rc;

	rc = this->rfm->CreateRecordFile(t_path.c_str(),tr_met.rs);
	if (rc != OK) return rc;
	

	rc = this->rfm->OpenRecordFile(t_path.c_str(),rfh);
	if (rc != OK) return rc;
	
	REM_RecordFileHandle rfh1;
	std::string t_path1(this->path + t1);
	rc = this->rfm->OpenRecordFile(t_path1.c_str(),rfh1);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs1;
	rc = rfs1.OpenRecordScan(rfh1, TYPE_INT, sizeof(int), 0, NO_OP, "");
	if (rc != OK) return rc;

	REM_RecordID rid;
	REM_RecordHandle rh1;
	while(rfs1.GetNextRecord(rh1) == OK){
		char * rData;
		char * rData1;
		rc = rh1.GetData(rData1);
		if (rc != OK) return rc;

		RelMet met1;
		rc = this->smm->GetRelationMetadata(t1,met1);
		if (rc != OK) return rc;

		if (t2[0] != '\0') {
			REM_RecordFileHandle rfh2;
			std::string t_path2(this->path + t2);
			rc = this->rfm->OpenRecordFile(t_path2.c_str(), rfh2);
			if (rc != OK) return rc;

			REM_RecordFileScan rfs2;
			rc = rfs2.OpenRecordScan(rfh2, TYPE_INT, sizeof(int), 0, NO_OP, "");
			if (rc != OK) return rc;

			REM_RecordHandle rh2;
			while(rfs2.GetNextRecord(rh2) == OK){
				char * rData2;
				rc = rh2.GetData(rData2);
				if (rc != OK) return rc;

				RelMet met2;
				rc = this->smm->GetRelationMetadata(t2, met2);
				if (rc != OK) return rc;
				
				if (t3[0] != '\0') {
					REM_RecordFileHandle rfh3;
					std::string t_path3(this->path + t3);
					rc = this->rfm->OpenRecordFile(t_path3.c_str(), rfh3);
					if (rc != OK) return rc;

					REM_RecordFileScan rfs3;
					rc = rfs3.OpenRecordScan(rfh3, TYPE_INT, sizeof(int), 0, NO_OP, "");
					if (rc != OK) return rc;

					REM_RecordHandle rh3;
					while(rfs3.GetNextRecord(rh3) == OK){
						char * rData3;
						rc = rh3.GetData(rData3);
						if (rc != OK) return rc;

						RelMet met3;
						rc = this->smm->GetRelationMetadata(t3, met3);
						if (rc != OK) return rc;

						rData = new char[met1.rs + met2.rs + met3.rs];
						memcpy(&rData[0], rData1, met1.rs);
						memcpy(&rData[met1.rs], rData2, met2.rs);
						memcpy(&rData[met1.rs + met2.rs], rData3, met3.rs);

						rc = rfh.InsertRecord(rData, rid);
						if (rc != OK) return rc;
					}//3rd while 

					rc = this->rfm->CloseRecordFile(rfh3);
					if (rc != OK) return rc;

					rc = rfs3.CloseRecordScan();
					if (rc != OK) return rc;

				} else {	//table3
					
					rData = new char[met1.rs + met2.rs];
					memcpy(&rData[0], rData1, met1.rs);
					memcpy(&rData[met1.rs], rData2, met2.rs);

					rc = rfh.InsertRecord(rData, rid);
					if (rc != OK) return rc;
				}
			} //2nd wile

			rc = this->rfm->CloseRecordFile(rfh2);
			if (rc != OK) return rc;

			rc = rfs2.CloseRecordScan();
			if (rc != OK) return rc;

		} else { //table2
			
			rData = new char[met1.rs];
			memcpy(&rData[0],rData1,met1.rs);

			rc = rfh.InsertRecord(rData, rid);
			if (rc != OK) return rc;
		}
		
	}
	
	rc = this->rfm->CloseRecordFile(rfh1);
	if (rc != OK) return rc;

	rc = rfs1.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (OK);

}

t_rc SSQLM_DML_Manager::executeSubcondition(char * leftAttr, char * rightAttr, t_compOp currOp) {
	t_rc rc;
	
	INXM_IndexScan is;
	INXM_IndexHandle ih;
	REM_RecordFileScan rfs;
	REM_RecordFileHandle rfh;
	REM_RecordHandle rh;
	REM_RecordID rid;
	
	AttrMet l_attr_met;
	rc = this->smm->GetAttributeMetadata(TEMP_T,leftAttr,l_attr_met);
	if (rc != OK ) return rc;
	
	AttrMet r_attr_met;
	rc = this->smm->GetAttributeMetadata(TEMP_T,rightAttr,r_attr_met);
	if (rc != OK ) return rc;
			
	char currDBName[100] = "\0";
	strcpy(currDBName, this->path.c_str());	
	strcat(currDBName, TEMP_T);
	rc = this->rfm->OpenRecordFile(currDBName, rfh);
	if (rc != OK) return rc;

	if (l_attr_met.indexNo >= 0) {
		rc = is.OpenIndexScan(ih,NO_OP,"");
		if (rc != OK) return rc;
	} else {
		rc = rfs.OpenRecordScan(rfh, l_attr_met.type, l_attr_met.length, l_attr_met.offset, NO_OP, "");
		if (rc != OK) return rc;
	}

	//get the record of left table.
	while (((rc = rfs.GetNextRecord(rh)) == OK) || ((rc = is.GetNextEntry(rid)) == OK)) {
		
		if (l_attr_met.indexNo >= 0) {
			rc = rfh.ReadRecord(rid,rh);
			if (rc != OK) return rc;
		} else {
			rc = rh.GetRecordID(rid);
			if (rc != OK) return rc;
		}

		char * rData;
		rc = rh.GetData(rData);
		if (rc != OK) return rc;

		char * leftAttrStr = new char[l_attr_met.length];
		memcpy(leftAttrStr, &rData[l_attr_met.offset],l_attr_met.length);

		char * rightAttrStr = new char[r_attr_met.length];
		memcpy(rightAttrStr, &rData[r_attr_met.offset],r_attr_met.length);
		

		int rightAttrInt, leftAttrInt;
		memcpy(&leftAttrInt, &rData[l_attr_met.offset],l_attr_met.length);
		memcpy(&rightAttrInt, &rData[r_attr_met.offset],r_attr_met.length);

		switch (currOp) {
		case EQ_OP:
			if (l_attr_met.type == TYPE_INT) {
				if (leftAttrInt != rightAttrInt) {
					rc = rfh.DeleteRecord(rid);
					if (rc != OK) return rc;
				}
			} else {
				if (strcmp(leftAttrStr,rightAttrStr) != 0) {
					rc = rfh.DeleteRecord(rid);
					if (rc != OK) return rc;
				}
			}
			break;
		case NE_OP:
			if (l_attr_met.type == TYPE_INT) {
				if (leftAttrInt == rightAttrInt) {
					rc = rfh.DeleteRecord(rid);
					if (rc != OK) return rc;
				}
			} else {
				if (strcmp(leftAttrStr,rightAttrStr) == 0) {
					rc = rfh.DeleteRecord(rid);
					if (rc != OK) return rc;
				}
			}
			break;
		case LT_OP:
			if (l_attr_met.type == TYPE_INT) {
				if (leftAttrInt >= rightAttrInt) {
					rc = rfh.DeleteRecord(rid);
					if (rc != OK) return rc;
				}
			} else {
				if (strcmp(leftAttrStr,rightAttrStr) >= 0) {
					rc = rfh.DeleteRecord(rid);
					if (rc != OK) return rc;
				}
			}
			break;
		case GT_OP:
			if (l_attr_met.type == TYPE_INT) {
				if (leftAttrInt <= rightAttrInt) {
					rc = rfh.DeleteRecord(rid);
					if (rc != OK) return rc;
				}
			} else {
				if (strcmp(leftAttrStr,rightAttrStr) <= 0) {
					rc = rfh.DeleteRecord(rid);
					if (rc != OK) return rc;
				}
			}
			break;
		case LE_OP:
			if (l_attr_met.type == TYPE_INT) {
				if (leftAttrInt >rightAttrInt) {
					rc = rfh.DeleteRecord(rid);
					if (rc != OK) return rc;
				}
			} else {
				if (strcmp(leftAttrStr,rightAttrStr) > 0) {
					rc = rfh.DeleteRecord(rid);
					if (rc != OK) return rc;
				}
			}
			break;
		case GE_OP:
			if (l_attr_met.type == TYPE_INT) {
				if (leftAttrInt < rightAttrInt) {
					rc = rfh.DeleteRecord(rid);
					if (rc != OK) return rc;
				}
			} else {
				if (strcmp(leftAttrStr,rightAttrStr) < 0) {
					rc = rfh.DeleteRecord(rid);
					if (rc != OK) return rc;
				}
			}
			break;
		case NO_OP:
			break;
		default:
			return (SSQLM_DML_WRONGOP);
		}
		
	}

	if (l_attr_met.indexNo >= 0) {
		rc = is.CloseIndexScan();
		if (rc != OK) return rc;
	} else {
		rc = rfs.CloseRecordScan();
		if (rc != OK) return rc;
	}

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (OK);
}

t_rc SSQLM_DML_Manager::executeSubconditionValue(char * leftAttr, void * value, t_compOp currOp) {
	t_rc rc;
	
	INXM_IndexScan is;
	INXM_IndexHandle ih;
	REM_RecordFileScan rfs;
	REM_RecordFileHandle rfh;
	
	AttrMet l_attr_met;
	rc = this->smm->GetAttributeMetadata(TEMP_T,leftAttr,l_attr_met);
	if (rc != OK ) return rc;

	char currDBName[100] = "\0";
	strcpy(currDBName, this->path.c_str());
	
	strcat(currDBName, TEMP_T);
	rc = this->rfm->OpenRecordFile(currDBName, rfh);
	if (rc != OK) return rc;
	if (l_attr_met.indexNo >= 0) {
		char currTableName[100];
		std::string table(leftAttr);
		size_t pos = table.find(".");
		table = table.substr(0, pos);
		strcpy(currTableName, this->path.c_str());
		strcat(currTableName, table.c_str());
		
		rc = this->im->OpenIndex(currTableName,l_attr_met.indexNo,ih);
		if (rc != OK) return rc;
	}

	t_compOp oposOp;
	switch (currOp) {
	case EQ_OP:
		oposOp = NE_OP;
		break;
	case NE_OP:
		oposOp = EQ_OP;
		break;
	case LT_OP:
		oposOp = GE_OP;
		break;
	case GT_OP:
		oposOp = LE_OP;
		break;
	case LE_OP:
		oposOp = GT_OP;
		break;
	case GE_OP:
		oposOp = LT_OP;
		break;
	case NO_OP:
		break;
	default:
		return (SSQLM_DML_WRONGOP);
	}

	if (l_attr_met.indexNo < 0) {
		rc = rfs.OpenRecordScan(rfh, l_attr_met.type, l_attr_met.length, l_attr_met.offset, oposOp, value);
		if (rc != OK) return rc;
	} else {
		rc = is.OpenIndexScan(ih, oposOp, value);
		if (rc != OK) return rc;
	}

	REM_RecordHandle rh;
	REM_RecordID rid;
	while (((rc = rfs.GetNextRecord(rh)) == OK) || ((rc = is.GetNextEntry(rid)) == OK)) {
		if (l_attr_met.indexNo < 0) {
			rc = rh.GetRecordID(rid);
			if (rc != OK) return rc;
		}

		rc = rfh.DeleteRecord(rid);
		if (rc != OK) return rc;
	}
	
	if (l_attr_met.indexNo < 0) {
		rc = rfs.CloseRecordScan();
		if (rc != OK) return rc;
	} else {
		rc = is.CloseIndexScan();
		if (rc != OK) return rc;

		rc = this->im->CloseIndex(ih);
		if (rc != OK) return rc;
	}

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (OK);
}

t_rc SSQLM_DML_Manager::DropTempTable() {
	
	t_rc rc;

	rc = smm->DeleteRelationMetadata(TEMP_T);
	if (rc != OK) return rc;

	std::string dbname(this->openedDatabaseName);
	std::string path("C:\\sirenbase\\data\\" + dbname + "\\select.tmp");
	rc = this->rfm->DestroyRecordFile(path.c_str());
	if (rc != OK) return rc;
	
	return (OK);
}

t_rc SSQLM_DML_Manager::showTable(char * t_name, char * selectAttrNames) {
	
	t_rc rc;
	std::string s_names(selectAttrNames);
	std::string a_path(this->path + "attr.met");
	std::string t_path(this->path + t_name);
	
	RelMet r_meta;
	rc = this->smm->GetRelationMetadata(t_name,r_meta);
	if (rc != OK) return rc;

	REM_RecordFileHandle a_rfh;
	rc = this->rfm->OpenRecordFile(a_path.c_str(),a_rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan a_rfs;
	rc = a_rfs.OpenRecordScan(a_rfh,TYPE_STRING,REL_SIZE,ATTR_REL_NAME_OFFSET,EQ_OP,t_name);
	if (rc != OK) return rc;

	REM_RecordHandle a_rh;
	while (a_rfs.GetNextRecord(a_rh) == OK) {
		
		char * aData;
		rc = a_rh.GetData(aData);
		if (rc != OK) return rc;

		char * a_name = new char[ATTR_NAME_SIZE];
		memcpy(a_name,&aData[ATTR_NAME_OFFSET],ATTR_NAME_SIZE);

		std::string a_name_str(a_name);
		size_t pos = a_name_str.find(".");

		if ((s_names.find(a_name) != string::npos) || (s_names.find("*") != string::npos))
			cout << "\t" << a_name;
	}
	cout << endl;

	rc = a_rfs.CloseRecordScan();
	if (rc != OK) return rc;

	REM_RecordFileHandle t_rfh;
	rc = this->rfm->OpenRecordFile(t_path.c_str(),t_rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan t_rfs;
	rc = t_rfs.OpenRecordScan(t_rfh,TYPE_INT,0,0,NO_OP,0);
	if (rc != OK) return rc;

	REM_RecordHandle t_rh;
	while ((rc = t_rfs.GetNextRecord(t_rh)) == OK) {
		
		char * rData;
		rc = t_rh.GetData(rData);
		if (rc != OK) return rc;

		rc = a_rfs.OpenRecordScan(a_rfh,TYPE_STRING,REL_SIZE,ATTR_REL_NAME_OFFSET,EQ_OP,t_name);
		if (rc != OK) return rc;

		while (a_rfs.GetNextRecord(a_rh) == OK) {
			
			char * aData;
			rc = a_rh.GetData(aData);
			if (rc != OK) return rc;

			char * a_name = new char[ATTR_NAME_SIZE];
			memcpy(a_name,&aData[ATTR_NAME_OFFSET],ATTR_NAME_SIZE);
			
			std::string a_name_str(a_name);
			size_t pos = a_name_str.find(".");

			if ((s_names.find(a_name) != string::npos) || (s_names.find("*") != string::npos)) {
				AttrMet a_meta;
				rc = this->smm->GetAttributeMetadata(t_name,a_name,a_meta);
				if (rc != OK) return rc;

				if (a_meta.type == TYPE_STRING) {
					char * a_value = new char[a_meta.length];
					memcpy(a_value,&rData[a_meta.offset],a_meta.length);
					cout << "\t" << a_value;
				} else if(a_meta.type == TYPE_DOUBLE || a_meta.type == TYPE_FLOAT){
					double i_value;
					memcpy(&i_value,&rData[a_meta.offset],a_meta.length);
					cout << "\t" << i_value << "\t";
				} else {
					int i_value;
					memcpy(&i_value,&rData[a_meta.offset],a_meta.length);
					cout << "\t" << i_value << "\t";
				}
			}
		}
		cout << endl;

		rc = a_rfs.CloseRecordScan();
		if (rc != OK) return rc;
	}

	rc = t_rfs.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(a_rfh);
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(t_rfh);
	if (rc != OK) return rc;

	return (OK);
}

t_rc SSQLM_DML_Manager::InsertRecord(char * tableName, char * uInput){
	
	t_rc rc;
	char * rData;
	std::string uInputAsStr(uInput);
	std::string t_path = this->path + tableName;
	std::string m_path = this->path + "attr.met";
	
	RelMet r_Meta;
	rc = this->smm->GetRelationMetadata(tableName,r_Meta);
	if (rc != OK) return rc;

	rData = new char[r_Meta.rs];

	REM_RecordFileHandle rfh;
	rc = this->rfm->OpenRecordFile(m_path.c_str(), rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,TYPE_STRING,ATTR_SIZE,ATTR_REL_NAME_OFFSET,EQ_OP,tableName);
	if (rc != OK) return rc;

	REM_RecordHandle rh;
	while ((rc = rfs.GetNextRecord(rh)) == OK) {
		char * aData;
		rc = rh.GetData(aData);
		if (rc != OK) return rc;

		char * attrName = new char[255];
		memcpy(attrName,&aData[ATTR_NAME_OFFSET],ATTR_NAME_SIZE);

		AttrMet a_Meta;
		rc = this->smm->GetAttributeMetadata(tableName,attrName,a_Meta);
		if (rc != OK) return rc;
		
		size_t foundComma;
		char * attrValue = new char[a_Meta.length];

		//manage values in ""
		size_t foundFirstQuote, foundSecondQuote;
		foundComma = uInputAsStr.find(",");
		if (foundComma != string::npos) {

			if( (foundFirstQuote = uInputAsStr.find("\"")) == string::npos)
				foundFirstQuote = uInputAsStr.find("'");

			if( foundFirstQuote != string::npos && foundFirstQuote < foundComma ){	//means the value BEFORE comma is in ""
				uInputAsStr.replace(0, 2, "");	//take out space and '|"
				
				if( (foundSecondQuote = uInputAsStr.find("\"", 1)) == string::npos)
					foundSecondQuote = uInputAsStr.find("'", 1);
				
				uInputAsStr.copy(attrValue, foundSecondQuote, 0);
				attrValue[foundSecondQuote] = '\0';
				//erase space AND COMMA
				uInputAsStr.replace( 0, foundSecondQuote+3, "");
			} else {
				uInputAsStr.copy(attrValue, foundComma-2, 1);
				attrValue[foundComma-2] = '\0';
				uInputAsStr.replace(0, foundComma+1, "");
			}
		} else {
			if( (foundFirstQuote = uInputAsStr.find("\"")) == string::npos)
				foundFirstQuote = uInputAsStr.find("'");

			if( foundFirstQuote != string::npos){
				uInputAsStr.replace(0, 2, "");
				if( (foundSecondQuote = uInputAsStr.find("\"", 1)) == string::npos)
					foundSecondQuote = uInputAsStr.find("'", 1);

				uInputAsStr.copy(attrValue, foundSecondQuote, 0);
				attrValue[foundSecondQuote] = '\0';
			} else {
				uInputAsStr.copy(attrValue, uInputAsStr.length()-1, 1);
				attrValue[uInputAsStr.length()-2] = '\0';
			}
		}
		
		double attrDouble =0;
		if (a_Meta.type == TYPE_INT) {
			int attrInt = atoi(attrValue);
			memcpy(&rData[a_Meta.offset],&attrInt,a_Meta.length);
		} else if (a_Meta.type == TYPE_DOUBLE || a_Meta.type == TYPE_FLOAT){
			attrDouble = atof(attrValue);
			memcpy(&rData[a_Meta.offset],&attrDouble,a_Meta.length);
		} else
			memcpy(&rData[a_Meta.offset],attrValue,a_Meta.length);

	}

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	rc = this->rfm->OpenRecordFile(t_path.c_str(), rfh);
	if (rc != OK) return rc;
	
	REM_RecordID rid;
	rc = rfh.InsertRecord(rData, rid);
	if (rc != OK) return SSQLM_DML_INSERTRECORDERROR;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;
		
	return (OK);
}

t_rc SSQLM_DML_Manager::UpdateRecord(char * tableName, char * attrName , char * newValue, char * condition){

	t_rc rc;
	std::string t_path = this->path + tableName;
	char currOperator[4] = "\0", * conditionsValue = new char[255], * leftConditionPart = new char [255];
	
	t_compOp compOp;
	REM_RecordFileHandle rfh;
	rc = rfm->OpenRecordFile(t_path.c_str(), rfh);
	if(rc != OK) return rc;
	
	//recognize comparsion operator
	rc = this->reconCompOp(condition, currOperator, compOp);
	if(rc != OK) return rc;
	//process condition
	rc = breakSubCond(condition, currOperator, leftConditionPart, conditionsValue);
	if(rc != OK) return rc;

	std::string la(leftConditionPart);
	std::string ra(conditionsValue);

	la.replace(la.length()-1,1,"");
	ra.replace(0,1,"");

	leftConditionPart = new char[la.length()];
	conditionsValue = new char[ra.length()];

	memcpy(leftConditionPart,la.c_str(),strlen(leftConditionPart));
	memcpy(conditionsValue,ra.c_str(),strlen(conditionsValue));

	AttrMet aMeta;
	rc = this->smm->GetAttributeMetadata(tableName, leftConditionPart, aMeta);
	if(rc != OK) return rc;
	
	//the attribute to be updated!
	AttrMet u_aMeta;
	rc = this->smm->GetAttributeMetadata(tableName, attrName, u_aMeta);
	if(rc != OK) return rc;

	INXM_IndexHandle ih;
	if (aMeta.indexNo >= 0) {
		rc = this->im->OpenIndex((char *) t_path.c_str(),aMeta.indexNo,ih);
		if(rc != OK) return rc;
	}

	std::string cont(conditionsValue);
	size_t pos = cont.find_first_of("\"\'");
	if (pos == 0) cont.replace(pos,1,"");
	pos = cont.find_last_of("\"\'");
	if (pos == cont.length() - 1) cont.replace(pos,1,"");

	char * condValue2 = new char[aMeta.length];
	if(aMeta.type == TYPE_INT){
		int conditionsValueInt = atoi(conditionsValue);
		memcpy(condValue2, &conditionsValueInt, aMeta.length);		
	} else if (aMeta.type == TYPE_DOUBLE || aMeta.type == TYPE_FLOAT){
		double attrDouble = atof(conditionsValue);
		memcpy(condValue2,&attrDouble,aMeta.length);
	} else {
		memcpy(condValue2, cont.c_str(), aMeta.length);
	}

	REM_RecordFileScan rfs;
	INXM_IndexScan is;
	if (aMeta.indexNo < 0) {
		rc = rfs.OpenRecordScan(rfh, aMeta.type, aMeta.length, aMeta.offset, compOp, condValue2);
		if(rc != OK) return rc;
	} else {
		rc = is.OpenIndexScan(ih, compOp, condValue2);
		if(rc != OK) return rc;
	}
	
	REM_RecordHandle rh;
	REM_RecordID rid;
	while(((rc = rfs.GetNextRecord(rh))  == OK) || ((rc = is.GetNextEntry(rid)) == OK)){

		if (aMeta.indexNo < 0) {
			rc = rh.GetRecordID(rid);
			if (rc != OK) return rc;
		} else {
			rc = rfh.ReadRecord(rid,rh);
			if (rc != OK) return rc;
		}

		char * pData = new char[u_aMeta.offset];
		rc = rh.GetData(pData);
		if (rc !=OK) return rc;

		if(u_aMeta.type == TYPE_INT){
			int newValueInt = atoi(newValue);
			memcpy(&pData[u_aMeta.offset], &newValueInt, u_aMeta.length);			
		} else if(u_aMeta.type == TYPE_DOUBLE || u_aMeta.type == TYPE_FLOAT){
			double newValueDbl = atof(newValue);
			memcpy(&pData[u_aMeta.offset], &newValueDbl, u_aMeta.length);
		} else {
			memcpy(&pData[u_aMeta.offset], newValue, u_aMeta.length);
		}

		rc = rfh.UpdateRecord(rh);
		if (rc != OK) return SSQLM_DML_UPDATEFAILERROR;
	}

	if (aMeta.indexNo < 0) {
		rc = rfs.CloseRecordScan();
		if (rc != OK) return rc;
	} else {
		rc = is.CloseIndexScan();
		if (rc != OK) return rc;

		rc = this->im->CloseIndex(ih);
		if (rc != OK) return rc;
	}

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;
	
	return (OK);
}

t_rc SSQLM_DML_Manager::DeleteRecord(char * tableName, char * condition){

	t_rc rc;
	std::string t_path = this->path + tableName;
	char currOperator[4] = "\0", * conditionsValue = new char[255], * leftConditionPart = new char [255];
	
	t_compOp compOp;
	REM_RecordFileHandle rfh;
	rc = rfm->OpenRecordFile(t_path.c_str(), rfh);
	if(rc != OK) return rc;
	
	//recognize comparsion operator
	rc = reconCompOp(condition, currOperator, compOp);
	if(rc != OK) return rc;
	//process condition
	rc = breakSubCond(condition, currOperator, leftConditionPart, conditionsValue);
	if(rc != OK) return rc;

    std::string la(leftConditionPart);
	std::string ra(conditionsValue);

	la.replace(la.length()-1,1,"");
	ra.replace(0,1,"");

	leftConditionPart = new char[la.length()];
	conditionsValue = new char[ra.length()];

	memcpy(leftConditionPart,la.c_str(),strlen(leftConditionPart));
	memcpy(conditionsValue,ra.c_str(),strlen(conditionsValue));

    AttrMet aMeta;
	rc = this->smm->GetAttributeMetadata(tableName, leftConditionPart, aMeta);
	if(rc != OK) return rc;
	
	INXM_IndexHandle ih;
	if (aMeta.indexNo >= 0) {
		rc = this->im->OpenIndex(t_path.c_str(),aMeta.indexNo,ih);
		if (rc != OK) return rc;
	}

	std::string cont(conditionsValue);
	size_t pos = cont.find_first_of("\"\'");
	if (pos == 0) cont.replace(pos,1,"");
	pos = cont.find_last_of("\"\'");
	if (pos == cont.length() - 1) cont.replace(pos,1,"");

	char * condValue2 = new char[aMeta.length];
	if(aMeta.type == TYPE_INT){
		int conditionsValueInt = atoi(conditionsValue);
		memcpy(condValue2, &conditionsValueInt, aMeta.length);
	} else if (aMeta.type == TYPE_DOUBLE || aMeta.type == TYPE_FLOAT){
		double attrDouble = atof(conditionsValue);
		memcpy(condValue2, &attrDouble,aMeta.length);
	} else {
		memcpy(condValue2, cont.c_str(), aMeta.length);
	}

	REM_RecordFileScan rfs;
	INXM_IndexScan is;
	if (aMeta.indexNo < 0) {
		rc = rfs.OpenRecordScan(rfh, aMeta.type, aMeta.length, aMeta.offset, compOp, condValue2);
		if(rc != OK) return rc;
	} else {
		rc = is.OpenIndexScan(ih, compOp, &condValue2);
		if (rc != OK) return rc;
	}
	
	REM_RecordHandle rh;
	REM_RecordID rid;
	while(((rc = rfs.GetNextRecord(rh)) == OK) || ((rc = is.GetNextEntry(rid)) == OK)){

		if (aMeta.indexNo < 0) {
			rc = rh.GetRecordID(rid);
			if(rc != OK) return rc;
		}

		rc = rfh.DeleteRecord(rid);
		if (rc != OK) return SSQLM_DML_COULDNOTDELETERECORD;
	}

	if (aMeta.indexNo < 0) {
		rc = rfs.CloseRecordScan();
		if (rc !=OK) return rc;
	} else {
		rc = is.CloseIndexScan();
		if (rc != OK) return rc;

		rc = this->im->CloseIndex(ih);
		if (rc != OK) return rc;
	}

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (OK);
}