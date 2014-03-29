#ifndef _SSQLM_DML_Manager_h
#define _SSQLM_DML_Manager_h

#include "REM.h"
#include "INXM.h"
#include "SSQLM.h"
#include "REM_Types.h"
#include "SYSM.h"

#define TEMP_T "select.tmp"

class SSQLM_DML_Manager
{
	
	INXM_IndexManager * im;
	REM_RecordFileManager * rfm;
	SYSM_MetaManager * smm;
	std::string openedDatabaseName;
	std::string path;			// Η διαδρομή για το φάκελο με τις βάσεις δεδομένων

	//checks if SELECT-FROM tables match
	t_rc queryValidationStageOne(char * selectAttrNames,char * fromTables);

	//check the subcondition...
	t_rc validateWhereSubconditions(char * conditions, char * fromTables);

	//get the comparsion operator as t_compOp and as char
	t_rc reconCompOp(char * currentSubCondition, char currOperator[4], t_compOp & currOp);

	//returns the left and right attribute of a subcondition
	t_rc breakSubCond(char * currentSubCondition, char currOperator[4], char * leftAttr, char * rightAttr);

	//gets attribute's table and checks if it is found on more than one of FROM's tables
	//finds the table an atribute belongs to
	t_rc scanFromTables(const std::string &fullAttributeString, char * fromTables,
						char * table,	char * attrName, char * pData, bool * selectFrom);
	 
	//finds the attrType of the atribute with the given name
	t_rc checkAttrType(char * table, char * namet, t_attrType *attrType);
	
	t_rc SSQLM_DML_Manager::executeSubcondition(char * leftAttr, char * rightAttr, t_compOp currOp);

	t_rc SSQLM_DML_Manager::executeSubconditionValue(char * leftAttr, void * value, t_compOp currOp);

	t_rc SSQLM_DML_Manager::CreateTempTable(char * fromTables);

	t_rc SSQLM_DML_Manager::DropTempTable();

	t_rc SSQLM_DML_Manager::showTable(char * t_name, char * selectAttrNames);
		
public:
	SSQLM_DML_Manager(REM_RecordFileManager * rfm, INXM_IndexManager * im, SYSM_MetaManager * smm);
	~SSQLM_DML_Manager(void);

	t_rc SetOpenedDatabase(char * db_Name);

	t_rc SelectRecord(char * attrNames,char * fromTables, char * subConditions);
	t_rc InsertRecord(char * tableName, char * rData);
	t_rc DeleteRecord(char * tableName, char * condition);
	t_rc UpdateRecord(char * tableName, char * attrName , char * newValue, char * condition);
	
	friend class UIM_UserInterfaceManager;
};

#endif
