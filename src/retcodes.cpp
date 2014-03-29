#include "retcodes.h"
#include <cstdio>

void DisplayReturnCode(t_rc rc)
{
	char *msg;

	switch(rc)
	{
	case OK : msg = "OK"; break;
	case STORM_FILENOTFOUND : msg = "Specified file is not found."; break;
	case STORM_FILEEXISTS : msg = "File already exists."; break;
	case STORM_FILEDOESNOTEXIST : msg="File does not exist."; break;
	case STORM_FILEALREADYOPENED : msg = "File already opened."; break;
	case STORM_EOF : msg = "End Of File (EOF) has been reached."; break;
	case STORM_FILEFULL : msg = "File has reached its maximum capacity."; break;
	case STORM_FILEOPENERROR : msg = "File open error."; break;
	case STORM_FILECLOSEERROR : msg = "File close error."; break;
	case STORM_FILENOTOPENED : msg = "File is not opened."; break;
	case STORM_OPENEDFILELIMITREACHED : msg = "Limit of opened files reached."; break;
	case STORM_INVALIDFILEHANDLE : msg = "Invalid File Handle."; break;
	case STORM_INVALIDPAGE : msg = "Page is not valid."; break;
	case STORM_CANNOTCREATEFILE : msg = "Cannot create file."; break;
	case STORM_CANNOTDESTROYFILE : msg = "Cannot destroy file."; break;
	case STORM_PAGENOTINBUFFER : msg = "Page is not in buffer."; break;
	case STORM_PAGEISPINNED : msg = "Page is pinned."; break;
	case STORM_ALLPAGESPINNED : msg = "All pages are pinned."; break;
	case STORM_IOERROR : msg = "Input/Output error."; break;
	case STORM_MEMALLOCERROR : msg = "Memory allocation error."; break;
	
	case REM_FILEISOPEN : msg = "Record file is already opened."; break;
	case REM_FILEISNOTOPEN : msg = "There is no opened record file."; break;
	case REM_PAGEIDENTIFIERRETURNERROR : msg = "Fail to return the page id."; break;
	case REM_PAGEIDENTIFIERSETERROR : msg = "Fail to set the page id."; break;
	case REM_SLOTRETURNERROR : msg = "Fail to return slot."; break;
	case REM_SLOTSETERROR : msg = "Fail to set the slot."; break;
	case REM_FILEEXISTS : msg = "The record file you try to create already exists."; break;
	case REM_NOTVALIDRECORDTOREAD : msg = "Invalid record to read."; break;
	case REM_FIRSTPAGEERROR : msg = "Fail to return the first page"; break;
	case REM_RESERVEPAGEERROR : msg = "Fail to reserve a page."; break;
	case REM_GETPAGEERROR : msg = "Fail to get a page."; break;
	case REM_GETDATAERROR : msg = "Fail to get page's data."; break;
	case REM_FILESCANISOPEN : msg = "The record file scan is allready opened."; break;
	case REM_FILESCANISNOTOPEN : msg = "There is no opened record file scan."; break;
	case REM_WRONGTYPE : msg = "The type has to be INT or CHAR(INT)."; break;
	case REM_FILESCANNOTFINDRESULTS : msg = "There are no results for this subcondition."; break;

	case INXM_WRONGRECORDSIZE : msg = "Wrong record size given"; break;
	case INXM_FILESCANISOPEN : msg = "Index file is already opened."; break;
	case INXM_FILESCANISNOTOPEN : msg = "There is no opened index scan"; break;
	case INXM_FILESCANENDOFFILE : msg = "File scan reached unexpected end of file"; break;
	case INXM_VALUENOTFOUND : msg = "Value not found"; break;
	case INXM_INDEXHANDLECLOSED : msg = "Index hande is already closed"; break;
	case INXM_EOF : msg = "Reached end of file/node"; break;
	case INXM_NOMOREVALUES : msg = "There are no more values in node"; break;

	case SYSM_DATABASEEXISTS : msg = "Database was not created because this name is the name of an other database."; break;
	case SYSM_PATHNOTFOUND : msg = "Path is invalid."; break;
	case SYSM_UNKNOWNERROR : msg = "An unknown error has occured."; break;
	case SYSM_DIRECTORYNOTEMPTY : msg = "Given path is not a directory, directory is not empty or directory is either current working directory or root directory."; break;
	case SYSM_NOACCESSTODIRECTORY : msg = "A program has an open handle to the directory."; break;
	case SYSM_ERRORDELETINGFILE : msg = "Error deleting file."; break;
	case SYSM_ANOTHERDATABASEISOPEN : msg = "An other database is open."; break;
	case SYSM_ERROROPENDATABASE : msg = "Error in opening a database."; break;
	case SYSM_NOTOPENEDDATABASE : msg = "Database is not opened."; break;
	case SYSM_RELATIONEXISTS : msg = "The relation you want to enter already exists."; break;
	case SYSM_ATTRIBUTEEXISTS : msg = "The attribute you want to enter already exists."; break;
	case SYSM_RELATIONDOESNOTEXISTS : msg = "This relation doesn't exist."; break;
	case SYSM_ATTRIBUTEDOESNOTEXISTS : msg = "This attribute doesn't exist."; break;
	case SYSM_ERRORCOMMAND : msg = "The query has to follow the pattern:\n\tcreate database <database_name>;\n\tdrop database <database_name>;\n\topen database <database_name>;\n\tclose database <database_name>;"; break;
	case SYSM_ERRORCLOSEDATABASENAME : msg = "The database you try to close is not opened."; break;

	case SSQLM_DDL_INDEXEXISTS : msg = "The index you try to create already exists."; break;
	case SSQLM_DDL_INDEXDOESNOTEXIST : msg = "The index you try to drop does not exists."; break;
	case SSQLM_DDL_WRONG_ATTR_TYPE : msg = "Could not define the attribute's type"; break;
	case SSQLM_DDL_ATTR_IN_WRONG_FORMAT : msg = "Attribute on CreateTable should have comma and space (attr1, attr2)"; break;
	case SSQLM_DDL_ERRORCOMMAND : msg = "The query has to follow the pattern:\n\tcreate table <table_name> (<attr1_name> <ATTR1_TYPE>, <attr2_name> <ATTR2_TYPE>, ... , <attrn_name> <ATTRn_TYPE>);\n\tdrop table <table_name>;\n\tcreate index <table_name> (<attr_name>);\n\tdrop index <table_name> (<attr_name>);"; break;

	case SSQLM_DML_COULDNOTDELETERECORD : msg = "Unable to delete record (SSQLM)."; break;
	case SSQLM_DML_INSERTRECORDERROR : msg = "Unable to insert record on SSQLM."; break;
	case SSQLM_DML_UPDATEFAILERROR : msg = "Could not update the record."; break;
	case SSQLM_DML_ATTR_NOT_FOUND_ONTHEGIVEN_TABLES : msg = "The attr was not found on the tables declared at FROM"; break;
	case SSQLM_DML_FAIL_TO_INSERT_RECORD_REL_MET : msg = "Failed to insert record to rel.met"; break;
	case SSQLM_DML_FAIL_TO_INSERT_RECORD_ATTR_MET : msg = "Failed to insert record to attr.met"; break;
	case SSQLM_DML_WRONG_SUBCONDITION_FORM : msg = "WHERE conditions should be qriten as: attr1<>attr2 AND attr3..."; break;
	case SSQLM_DML_COULD_NOT_CONNECT_ATTR_TABLE : msg = "Some attribute was not found in any table!"; break;
	case SSQLM_DML_SELECT_FROM_STATEMENTS_ERROR : msg = "The SELECT attributes do not match FROM tables"; break;
	case SSQLM_DML_SELECT_ATTR_FOUND_IN_TWO_TABLES : msg = "One of the SELECT's attributes was found in more than one tables"; break;
	case SSQLM_DML_WHERE_ATTR_FOUND_IN_TWO_TABLES : msg = "One of the WHERES's attributes was found in more than one tables"; break;
	case SSQLM_DML_BAD_COMP_OP : msg = "Could not recognize the comparsion operator."; break;
	case SSQLM_DML_COMPARSIONPROBLEM : msg = "I can not compare the two values of a subcondition."; break;
	case SSQLM_DML_BadAttributesType : msg = "Could not regognize attributes type"; break;
	case SSQLM_DML_TOOMANYTABLES : msg = "There should be at most three tables t FROM statement"; break;
	case SSQLM_DML_WRONGOP : msg = "Could not recognize the comparsion operator."; break;
	case SSQLM_DML_SELECT_WHERE_STATEMENTS_ERROR : msg = "SELECT's attributes do not match with WHERE"; break;
	case SSQLM_DML_ERROR_GETTING_ATTRIBUTES_TYPE : msg = "Could not recognize attributes type."; break;

	case UIM_SCRIPTNOTEXISTS : msg = "The script path you entered does not exists."; break;
	case UIM_NOTVALIDFORMAT : msg = "Not valid syndax."; break;

	default: msg = "Unknown return code."; break;

	}

	fprintf (stderr, "ERROR: %s\n", msg);
}

void DisplayMessage(char *msg)
{
	fprintf (stderr, "ERROR: %s\n", msg); 
}
