#include "SSQLM_DDL_Manager.h"



SSQLM_DDL_Manager::SSQLM_DDL_Manager(REM_RecordFileManager * rfm, INXM_IndexManager * im, SYSM_MetaManager * smm) {
	this->rfm = rfm;
	this->im = im;
	this->smm = smm;
	this->openedDatabaseName = "";
	this->path = "";
}

SSQLM_DDL_Manager::~SSQLM_DDL_Manager() {}

t_rc SSQLM_DDL_Manager::SetOpenedDatabase(char * db_Name) {
	this->openedDatabaseName = db_Name;
	this->path =  "C:\\sirenbase\\data\\" + this->openedDatabaseName + "\\";
	return (OK);
}

t_rc SSQLM_DDL_Manager::CreateTable(char * t_Name, char * attrNames) {
	
	t_rc rc;
	size_t foundComma, foundParenthesis, stringLength, nameEndsAt;
	int attributeAsStringLength;

	std::string attrList(attrNames);
	t_attrType attrTypeAsStruct;	//attributes type
	int attrSize=0;					//atributes size
	int recordSize=0;				//records total size
	int numOfAttributes=0;			//the number of attributes
	
	//handle the attributes list and extract metadata.
	do{
		foundComma = attrList.find(" , ");
		
		numOfAttributes++;

		char attributeAsChar[ATTR_NAME_SIZE];
		if (foundComma != string::npos){	//get the arguments
			stringLength = attrList.copy(attributeAsChar, foundComma, 0);
		}else{		//handle the last attr that is not followed by ","
			stringLength = attrList.copy(attributeAsChar, attrList.length(), 0);
		}
		attributeAsChar[stringLength] = '\0';
		std::string attributeAsString(attributeAsChar);
		attributeAsStringLength = attributeAsString.length();
			
		nameEndsAt = attributeAsString.find(" ");
		if (nameEndsAt != string::npos){
			char attrName[50], attrType[15];
			stringLength = attributeAsString.copy(attrName, nameEndsAt, 0);
			attrName[stringLength] = '\0';

			stringLength = attributeAsString.copy(attrType, attrList.length(), nameEndsAt+1);
			attrType[stringLength] = '\0';

			//check the kind of type and calculate size
			rc = checkAttrType(attrType, &attrTypeAsStruct, &attrSize);
			if (rc != OK) return rc;

			//add entry to attr.met
			rc = this->smm->InsertAttributeMetadata(t_Name, attrName,recordSize,attrTypeAsStruct,attrSize,-1);
			if (rc != OK) return SSQLM_DML_FAIL_TO_INSERT_RECORD_ATTR_MET;

			//add to total record size
			recordSize+=attrSize;

		} else {
			return SSQLM_DDL_ATTR_IN_WRONG_FORMAT;
		}
		attrList.replace(0, attributeAsStringLength+3, "");		//+2 in order to erase ", "
	}while(attrList.compare("") != 0);

	//add new entry at rel.met
	rc = this->smm->InsertRelationMetadata(t_Name,recordSize,numOfAttributes,0);
	if (rc != OK) return SSQLM_DML_FAIL_TO_INSERT_RECORD_REL_MET;
	
	std::string t_path = this->path + t_Name;
	rc = this->rfm->CreateRecordFile(t_path.c_str(), recordSize);
	if (rc != OK) return rc;

	return (OK);
}

//assistant function. Returns the attributes Type and defines its size.
t_rc SSQLM_DDL_Manager::checkAttrType(char * attrType, t_attrType * attrTypeAsStruct, int * attrSize){

	t_rc rc;
	size_t stringLength;
	//t_attrType attrTypeAsStruct;
	std::string attrTypeAsString(attrType);

	if(attrTypeAsString.find("INT") != string::npos ||
		attrTypeAsString.find("int") != string::npos){
		*attrTypeAsStruct = TYPE_INT;	//define TYPE
		*attrSize = sizeof(int);	//define SIZE
	} else if(attrTypeAsString.find("FLOAT") != string::npos ||
				attrTypeAsString.find("float") != string::npos){
		*attrTypeAsStruct = TYPE_FLOAT;	
		*attrSize = sizeof(double);	
	} else if(attrTypeAsString.find("DOUBLE") != string::npos ||
				attrTypeAsString.find("double") != string::npos){
		*attrTypeAsStruct = TYPE_DOUBLE;
		*attrSize = sizeof(double);
	}else{
		size_t foundLeftParenthesis, foundRightParenthesis;
		foundLeftParenthesis = attrTypeAsString.find("(");
		foundRightParenthesis = attrTypeAsString.find(")");
		if(foundLeftParenthesis != string::npos && foundRightParenthesis != string::npos ){
			*attrTypeAsStruct = TYPE_STRING;	//define TYPE
			//get the attributes Size
			char attrLengthAsChar[5];
			int attrLengthAsInt;

			//stringLength = attrTypeAsString.copy(attrLengthAsChar, foundRightParenthesis+1, foundLeftParenthesis+1);//attrTypeAsString.length()-1, foundParenthesis+1);
			stringLength = attrTypeAsString.copy(attrLengthAsChar, foundRightParenthesis-2, foundLeftParenthesis+2);
			std::string attrLengthAsCharStr(attrLengthAsChar);
			foundRightParenthesis = attrLengthAsCharStr.find(")");
			//attrLengthAsChar[stringLength] = '\0';
			attrLengthAsChar[foundRightParenthesis-1] = '\0';
			attrLengthAsInt = atoi(attrLengthAsChar);
			int size = sizeof(char) * attrLengthAsInt;	//define SIZE
			*attrSize = size;
		}
	}

	if( *attrTypeAsStruct != TYPE_INT &&  *attrTypeAsStruct != TYPE_STRING
		&& *attrTypeAsStruct != TYPE_FLOAT && *attrTypeAsStruct != TYPE_DOUBLE){
		return SSQLM_DDL_WRONG_ATTR_TYPE;
	}

	return OK;
}

t_rc SSQLM_DDL_Manager::DropTable(char * t_Name) {

	t_rc rc;

	std::string a_path = this->path + "attr.met";
	REM_RecordFileHandle rfh;
	rc = this->rfm->OpenRecordFile(a_path.c_str(), rfh);
	if (rc != OK) return rc;
	
	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,TYPE_STRING,REL_NAME_SIZE,ATTR_REL_NAME_OFFSET,EQ_OP,t_Name);
	if (rc != OK) return rc;

	REM_RecordHandle rh;
	while ((rc = rfs.GetNextRecord(rh)) == OK) {
		char * pData;
		rc = rh.GetData(pData);
		if (rc != OK) return rc;

		int indexNo;
		memcpy(&indexNo,&pData[ATTR_INX_NO_OFFSET],sizeof(int));

		if (indexNo >= 0) {
			char * attrName = new char[ATTR_NAME_SIZE];
			memcpy(attrName,&pData[ATTR_NAME_OFFSET],ATTR_NAME_SIZE);

			rc = this->DropIndex(t_Name, attrName);
			if (rc != OK) return rc;
		}
	}

	rc = smm->DeleteRelationMetadata(t_Name);
	if (rc != OK) return rc;

	std::string dbname(this->openedDatabaseName);
	std::string path("C:\\sirenbase\\data\\" + dbname + "\\" + t_Name);
	rc = this->rfm->DestroyRecordFile(path.c_str());
	if (rc != OK) return rc;

	
	return (OK);
}

t_rc SSQLM_DDL_Manager::CreateIndex(char * t_Name, char * a_Name) {
	
	t_rc rc;

	AttrMet a_meta;
	rc = this->smm->GetAttributeMetadata(t_Name, a_Name, a_meta);
	if (rc != OK) return rc;
	
	RelMet r_meta;
	rc = this->smm->GetRelationMetadata(t_Name, r_meta);
	if (rc != OK) return rc;

	if (a_meta.indexNo >= 0) return (SSQLM_DDL_INDEXEXISTS);

	int indexNo = 0;
	while (this->im->CreateIndex((path + t_Name).c_str(),indexNo,a_meta.type,a_meta.length) != OK) indexNo++;

	a_meta.indexNo = indexNo;
	r_meta.numOfIndexes++;

	rc = this->smm->UpdateAttributeMetadata(t_Name, a_Name, a_meta.relName, a_meta.attrName, a_meta.offset, a_meta.type, a_meta.length, a_meta.indexNo);
	if (rc != OK) return rc;

	rc = this->smm->UpdateRelationMetadata(t_Name, r_meta.name, r_meta.rs, r_meta.numAttrs, r_meta.numOfIndexes);
	if (rc != OK) return rc;

	REM_RecordFileHandle rfh;
	rc = this->rfm->OpenRecordFile((path + t_Name).c_str(),rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,TYPE_INT,0,0,NO_OP,0);
	if (rc != OK) return rc;
	
	INXM_IndexHandle ih;
	rc = this->im->OpenIndex((path + t_Name).c_str(),indexNo,ih);
	if (rc != OK) return rc;

	REM_RecordHandle rh;
	REM_RecordID rid;
	char * rData;

	while ((rc = rfs.GetNextRecord(rh)) == OK) {

		rc = rh.GetData(rData);
		if (rc != OK) return rc;

		rc = rh.GetRecordID(rid);
		if (rc != OK) return rc;

		char * aData = new char[a_meta.length];
		memcpy(aData,&rData[a_meta.offset],a_meta.length);

		rc = ih.InsertEntry(aData, rid);
		if (rc != OK) return rc;
	}

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	rc = this->im->CloseIndex(ih);
	if (rc != OK) return rc;
	

	return (OK);
}

t_rc SSQLM_DDL_Manager::DropIndex(char * t_Name, char * a_Name) {

	t_rc rc;

	AttrMet a_meta;
	rc = this->smm->GetAttributeMetadata(t_Name, a_Name, a_meta);
	if (rc != OK) return rc;
	
	RelMet r_meta;
	rc = this->smm->GetRelationMetadata(t_Name, r_meta);
	if (rc != OK) return rc;

	if (a_meta.indexNo < 0) return (SSQLM_DDL_INDEXDOESNOTEXIST);

	rc = this->im->DestroyIndex((path + t_Name).c_str(), a_meta.indexNo);
	if (rc != OK) return rc;

	a_meta.indexNo = -1;
	r_meta.numOfIndexes--;

	rc = this->smm->UpdateAttributeMetadata(t_Name, a_Name, a_meta.relName, a_meta.attrName, a_meta.offset, a_meta.type, a_meta.length, a_meta.indexNo);
	if (rc != OK) return rc;

	rc = this->smm->UpdateRelationMetadata(t_Name, r_meta.name, r_meta.rs, r_meta.numAttrs, r_meta.numOfIndexes);
	if (rc != OK) return rc;

	return (OK);
}