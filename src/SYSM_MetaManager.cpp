#include "SYSM_MetaManager.h"


SYSM_MetaManager::SYSM_MetaManager(REM_RecordFileManager  * rfm, SYSM_SystemManager * sysm) {
	this->rfm = rfm;
	this->sysm = sysm;
}


SYSM_MetaManager::~SYSM_MetaManager() {}


t_rc SYSM_MetaManager::InsertRelationMetadata(char * relName, int recSize, int numOfAttributes, int numOfIndexes) {

	if (!this->sysm->isOpen) return (SYSM_NOTOPENEDDATABASE);

	t_rc rc;
	REM_RecordFileHandle rfh;
	RelMet relMeta;

	if (GetRelationMetadata(relName,relMeta) == OK) return (SYSM_RELATIONEXISTS);

	std::string relPath(this->sysm->path + this->sysm->openedDBName + "\\rel.met");

	rc = this->rfm->OpenRecordFile(relPath.c_str(), rfh);
	if (rc != OK) return rc;

	char * pData = new char[REL_SIZE];
	REM_RecordID rid;

	memcpy(&pData[REL_NAME_OFFSET],relName,REL_NAME_SIZE);
	memcpy(&pData[REL_REC_SIZE_OFFSET],&recSize,sizeof(int));
	memcpy(&pData[REL_ATTR_NUM_OFFSET],&numOfAttributes,sizeof(int));
	memcpy(&pData[REL_INX_NUM_OFFSET],&numOfIndexes,sizeof(int));
		
	rc = rfh.InsertRecord(pData,rid);
	if (rc != OK) return rc;
		
	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (OK);
}

t_rc SYSM_MetaManager::InsertAttributeMetadata(char * relName, char * attrName, int attrOffset, t_attrType attrType, int attrLength, int indexNum) {

	if (!this->sysm->isOpen) return (SYSM_NOTOPENEDDATABASE);

	t_rc rc;
	REM_RecordID rid;
	REM_RecordFileHandle rfh;
	AttrMet attrMeta;
	std::string attrPath(this->sysm->path + this->sysm->openedDBName + "\\attr.met");

	if (GetAttributeMetadata(relName,attrName,attrMeta) == OK) return (SYSM_ATTRIBUTEEXISTS);

	rc = this->rfm->OpenRecordFile(attrPath.c_str(), rfh);
	if (rc != OK) return rc;
		
	char * pData = new char[ATTR_SIZE];

	memcpy(&pData[ATTR_REL_NAME_OFFSET],relName,REL_NAME_SIZE);
	memcpy(&pData[ATTR_NAME_OFFSET],attrName,ATTR_NAME_SIZE);
	memcpy(&pData[ATTR_OFFSET_OFFSET],&attrOffset,sizeof(int));
	memcpy(&pData[ATTR_TYPE_OFFSET],&attrType,sizeof(t_attrType));
	memcpy(&pData[ATTR_LENGTH_OFFSET],&attrLength,sizeof(int));
	memcpy(&pData[ATTR_INX_NO_OFFSET],&indexNum,sizeof(int));

	rc = rfh.InsertRecord(pData,rid);
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;
		
	return (OK);

}

t_rc SYSM_MetaManager::DeleteRelationMetadata(char * rel_Name) {

	if (!this->sysm->isOpen) return (SYSM_NOTOPENEDDATABASE);

	t_rc rc;
	REM_RecordFileHandle rfh;
	std::string relPath(this->sysm->path + this->sysm->openedDBName + "\\rel.met");

	rc = this->rfm->OpenRecordFile(relPath.c_str(), rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,TYPE_STRING,REL_NAME_SIZE,REL_NAME_OFFSET,EQ_OP,rel_Name);
	if (rc != OK) {
		rc = this->rfm->CloseRecordFile(rfh);
		if (rc != OK) return rc;
		
		return (SYSM_RELATIONDOESNOTEXISTS);
	}

	REM_RecordHandle rh;
	if (rfs.GetNextRecord(rh) != OK) return (SYSM_RELATIONDOESNOTEXISTS);

	REM_RecordID rid;
	rc = rh.GetRecordID(rid);
	if (rc != OK) return rc;

	rc = rfh.DeleteRecord(rid);
	if (rc != OK) return rc;

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	std::string attrPath(this->sysm->path + this->sysm->openedDBName + "\\attr.met");
	rc = this->rfm->OpenRecordFile(attrPath.c_str(),rfh);
	if (rc != OK) return rc;

	rc = rfs.OpenRecordScan(rfh,TYPE_STRING,REL_NAME_SIZE,ATTR_REL_NAME_OFFSET,EQ_OP,rel_Name);
	if (rc != OK) {
		rc = this->rfm->CloseRecordFile(rfh);
		if (rc != OK) return rc;
		
		return (OK);
	}

	while (rfs.GetNextRecord(rh) == OK) {
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

t_rc SYSM_MetaManager::DeleteAttributeMetadata(char * relName, char * attrName) {

	if (!this->sysm->isOpen) return (SYSM_NOTOPENEDDATABASE);

	t_rc rc;
	REM_RecordFileHandle rfh;
	std::string attrPath(this->sysm->path + this->sysm->openedDBName + "\\attr.met");

	rc = this->rfm->OpenRecordFile(attrPath.c_str(), rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,TYPE_STRING,ATTR_NAME_SIZE,ATTR_NAME_OFFSET,EQ_OP,attrName);
	if (rc != OK) {
		rc = this->rfm->CloseRecordFile(rfh);
		if (rc != OK) return rc;

		return (SYSM_ATTRIBUTEDOESNOTEXISTS);
	}

	bool done = false;
	REM_RecordHandle rh;
	while (rfs.GetNextRecord(rh) == OK) {
		
		REM_RecordFileScan temp_rfs;
		rc = temp_rfs.OpenRecordScan(rfh,TYPE_STRING,REL_NAME_SIZE,ATTR_REL_NAME_OFFSET,EQ_OP,relName);
		if (rc != OK) return rc;

		REM_RecordHandle temp_rh;
		while (temp_rfs.GetNextRecord(temp_rh) == OK) {
			
			REM_RecordID rid, temp_rid;
			rc = rh.GetRecordID(rid);
			if (rc != OK) return rc;

			rc = temp_rh.GetRecordID(temp_rid);
			if (rc != OK) return rc;

			int pageID, temp_pageID;
			rc = rid.GetPageID(pageID);
			if (rc != OK) return rc;

			rc = temp_rid.GetPageID(temp_pageID);
			if (rc != OK) return rc;

			int slot, temp_slot;
			rc = rid.GetSlot(slot);
			if (rc != OK) return rc;

			rc = temp_rid.GetSlot(temp_slot);
			if (rc != OK) return rc;

			if ((pageID == temp_pageID) && (slot == temp_slot)) {
				rc = rfh.DeleteRecord(rid);
				if (rc != OK) return rc;

				done = true;
				break;
			}
		}

		rc = temp_rfs.CloseRecordScan();
		if (rc != OK) return rc;

		if (done) break;
	}

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	if (!done) return (SYSM_ATTRIBUTEDOESNOTEXISTS);
	return (OK);
}

t_rc SYSM_MetaManager::UpdateRelationMetadata(char * p_Rel_Name, char * n_Rel_Name, int n_Rec_Size, int n_Num_Of_Attributes, int numOfIndexes) {

	if (!this->sysm->isOpen) return (SYSM_NOTOPENEDDATABASE);

	t_rc rc;
	REM_RecordFileHandle rfh;
	std::string relPath(this->sysm->path + this->sysm->openedDBName + "\\rel.met");

	rc = this->rfm->OpenRecordFile(relPath.c_str(), rfh);
	if (rc != OK) {
		rc = this->rfm->CloseRecordFile(rfh);
		if (rc != OK) return rc;
		
		return (SYSM_RELATIONDOESNOTEXISTS);
	}

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,TYPE_STRING,REL_NAME_SIZE,REL_NAME_OFFSET,EQ_OP,p_Rel_Name);
	if (rc != OK) return rc;

	REM_RecordHandle rh;
	if (rfs.GetNextRecord(rh) != OK) return (SYSM_RELATIONDOESNOTEXISTS);
	
	char * pData;
	REM_RecordID rid;

	rc = rh.GetData(pData);
	if (rc != OK) return rc;

	memcpy(&pData[REL_NAME_OFFSET],n_Rel_Name,REL_NAME_SIZE);
	memcpy(&pData[REL_REC_SIZE_OFFSET],&n_Rec_Size,sizeof(int));
	memcpy(&pData[REL_ATTR_NUM_OFFSET],&n_Num_Of_Attributes,sizeof(int));
	memcpy(&pData[REL_INX_NUM_OFFSET],&numOfIndexes,sizeof(int));

	rc = rfh.UpdateRecord(rh);
	if (rc != OK) return rc;

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	if (strcmp(p_Rel_Name,n_Rel_Name) != 0) {
		std::string attrPath(this->sysm->path + this->sysm->openedDBName + "\\attr.met");
		rc = this->rfm->OpenRecordFile(attrPath.c_str(),rfh);
		if (rc != OK) return rc;

		rfs.OpenRecordScan(rfh,TYPE_STRING,REL_NAME_SIZE,ATTR_REL_NAME_OFFSET,EQ_OP,p_Rel_Name);
		if (rc != OK) {
			rc = this->rfm->CloseRecordFile(rfh);
			if (rc != OK) return rc;

			return (OK);
		}

		while (rfs.GetNextRecord(rh) == OK) {
			char * pData;
			rc = rh.GetData(pData);
			if (rc != OK) return rc;

			memcpy(&pData[ATTR_REL_NAME_OFFSET],n_Rel_Name,REL_NAME_SIZE);

			rc = rfh.UpdateRecord(rh);
			if (rc != OK) return rc;
		}

		rc = rfs.CloseRecordScan();
		if (rc != OK) return rc;

		rc = this->rfm->CloseRecordFile(rfh);
		if (rc != OK) return rc;
	}

	return (OK);
}

t_rc SYSM_MetaManager::UpdateAttributeMetadata(char * p_RelName, char * p_AttrName, 
	char * n_RelName, char * n_AttrName, int n_AttrOffset, t_attrType n_AttrType, int n_AttrLength, int indexNum) {

	if (!this->sysm->isOpen) return (SYSM_NOTOPENEDDATABASE);

	t_rc rc;
	REM_RecordFileHandle rfh;
	std::string attrPath(this->sysm->path + this->sysm->openedDBName + "\\attr.met");

	rc = this->rfm->OpenRecordFile(attrPath.c_str(), rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,TYPE_STRING,ATTR_NAME_SIZE,ATTR_NAME_OFFSET,EQ_OP,p_AttrName);
	if (rc != OK) {
		rc = this->rfm->CloseRecordFile(rfh);
		if (rc != OK) return rc;
		
		return (SYSM_ATTRIBUTEDOESNOTEXISTS);
	}

	bool done = false;
	REM_RecordHandle rh;
	while (rfs.GetNextRecord(rh) == OK) {
		
		REM_RecordFileScan temp_rfs;
		rc = temp_rfs.OpenRecordScan(rfh,TYPE_STRING,REL_NAME_SIZE,ATTR_REL_NAME_OFFSET,EQ_OP,p_RelName);
		if (rc != OK) return rc;

		REM_RecordHandle temp_rh;
		while (temp_rfs.GetNextRecord(temp_rh) == OK) {
			
			REM_RecordID rid, temp_rid;
			rc = rh.GetRecordID(rid);
			if (rc != OK) return rc;

			rc = temp_rh.GetRecordID(temp_rid);
			if (rc != OK) return rc;

			int pageID, temp_pageID;
			rc = rid.GetPageID(pageID);
			if (rc != OK) return rc;

			rc = temp_rid.GetPageID(temp_pageID);
			if (rc != OK) return rc;

			int slot, temp_slot;
			rc = rid.GetSlot(slot);
			if (rc != OK) return rc;

			rc = temp_rid.GetSlot(temp_slot);
			if (rc != OK) return rc;

			if ((pageID == temp_pageID) && (slot == temp_slot)) {
				char * pData;
				rc = rh.GetData(pData);
				if (rc != OK) return rc;

				memcpy(&pData[ATTR_REL_NAME_OFFSET],n_RelName,REL_NAME_SIZE);
				memcpy(&pData[ATTR_NAME_OFFSET],n_AttrName,ATTR_NAME_SIZE);
				memcpy(&pData[ATTR_OFFSET_OFFSET],&n_AttrOffset,sizeof(int));
				memcpy(&pData[ATTR_TYPE_OFFSET],&n_AttrType,sizeof(t_attrType));
				memcpy(&pData[ATTR_LENGTH_OFFSET],&n_AttrLength,sizeof(int));
				memcpy(&pData[ATTR_INX_NO_OFFSET],&indexNum,sizeof(int));

				rc = rfh.UpdateRecord(rh);
				if (rc != OK) return rc;

				done = true;
				break;
			}
		}

		rc = temp_rfs.CloseRecordScan();
		if (rc != OK) return rc;

		if (done) break;
	}

	rc = rfs.CloseRecordScan();
	if (rc != OK) return rc;

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	if (done) return (OK);
	return (SYSM_ATTRIBUTEDOESNOTEXISTS);

}

t_rc SYSM_MetaManager::GetRelationMetadata(char * relName, RelMet &relMeta) {

	if (!this->sysm->isOpen) return (SYSM_NOTOPENEDDATABASE);
	t_rc rc;
	REM_RecordFileHandle rfh;
	std::string relPath(this->sysm->path + this->sysm->openedDBName + "\\rel.met");

	rc = this->rfm->OpenRecordFile(relPath.c_str(), rfh);
	if (rc != OK) return rc;

	bool exists = true;
	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,TYPE_STRING,REL_NAME_SIZE,REL_NAME_OFFSET,EQ_OP,relName);
	if (rc == OK) {
	
		REM_RecordHandle rh;
		if (rfs.GetNextRecord(rh) != OK) {
			rc = this->rfm->CloseRecordFile(rfh);
			if (rc != OK) return rc;
			
			return (SYSM_RELATIONDOESNOTEXISTS);
		}

		char * pData;
		rc = rh.GetData(pData);
		if (rc != OK) return rc;
	
		relMeta.name = new char[255];

		memcpy(relMeta.name,&pData[REL_NAME_OFFSET],REL_NAME_SIZE);
		memcpy(&relMeta.rs,&pData[REL_REC_SIZE_OFFSET],sizeof(int));
		memcpy(&relMeta.numAttrs,&pData[REL_ATTR_NUM_OFFSET],sizeof(int));
		memcpy(&relMeta.numOfIndexes,&pData[REL_INX_NUM_OFFSET],sizeof(int));

		rc = rfs.CloseRecordScan();
		if (rc != OK) return rc;

		rc = this->rfm->CloseRecordFile(rfh);
		if (rc != OK) return rc;
		
		return (OK);
	} else {
		rc = this->rfm->CloseRecordFile(rfh);
		if (rc != OK) return rc;
		
		return (SYSM_RELATIONDOESNOTEXISTS);
	}
}

t_rc SYSM_MetaManager::GetAttributeMetadata(char * relName, char * attrName, AttrMet &attrMeta) {
	
	if (!this->sysm->isOpen) return (SYSM_NOTOPENEDDATABASE);

	t_rc rc;
	REM_RecordID rid;
	REM_RecordFileHandle rfh;
	std::string attrPath;
	attrPath.append(this->sysm->path.c_str());
	attrPath.append(this->sysm->openedDBName.c_str());
	attrPath.append("\\attr.met");

	rc = this->rfm->OpenRecordFile(attrPath.c_str(), rfh);
	if (rc != OK) return rc;

	REM_RecordFileScan rfs;
	rc = rfs.OpenRecordScan(rfh,TYPE_STRING,ATTR_NAME_SIZE,ATTR_NAME_OFFSET,EQ_OP,attrName);
	if (rc == OK) {
		REM_RecordHandle rh;
		while (rfs.GetNextRecord(rh) == OK) {
			rc = rh.GetRecordID(rid);
			if (rc != OK) return rc;

			char * n_relName = new char[REL_NAME_SIZE];
			char * pData = new char[ATTR_SIZE];

			rc = rh.GetData(pData);
			if (rc != OK) return rc;

			memcpy(n_relName,&pData[ATTR_REL_NAME_OFFSET],REL_NAME_SIZE);

			if (strcmp(relName,n_relName) == 0) {
				
				attrMeta.relName = new char[255];
				attrMeta.attrName = new char[255];

				memcpy(attrMeta.relName,&pData[ATTR_REL_NAME_OFFSET],REL_NAME_SIZE);
				memcpy(attrMeta.attrName,&pData[ATTR_NAME_OFFSET],ATTR_NAME_SIZE);
				memcpy(&attrMeta.offset,&pData[ATTR_OFFSET_OFFSET],sizeof(int));
				memcpy(&attrMeta.type,&pData[ATTR_TYPE_OFFSET],sizeof(t_attrType));
				memcpy(&attrMeta.length,&pData[ATTR_LENGTH_OFFSET],sizeof(int));
				memcpy(&attrMeta.indexNo,&pData[ATTR_INX_NO_OFFSET],sizeof(int));

				rc = rfs.CloseRecordScan();
				if (rc != OK) return rc;

				rc = this->rfm->CloseRecordFile(rfh);
				if (rc != OK) return rc;

				return (OK);
			}
		}

		rc = rfs.CloseRecordScan();
		if (rc != OK) return rc;
	}

	rc = this->rfm->CloseRecordFile(rfh);
	if (rc != OK) return rc;

	return (SYSM_ATTRIBUTEDOESNOTEXISTS);
}