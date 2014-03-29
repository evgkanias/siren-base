#ifndef _SYSM_MetaManager_h
#define _SYSM_MetaManager_h

#include <fstream>
#include <direct.h>
#include <Windows.h>
#include "REM.h"
#include "SYSM.h"
#include "retcodes.h"

typedef struct RelMet {
	char * name;			// reletion name
	int rs;					// record size
	int numAttrs;			// records per page
	int numOfIndexes;		// number of indexes
} RelMet;

typedef struct AttrMet {
	char * relName;			// relation name
	char * attrName;		// attribute name
	int offset;				// atribute offset
	t_attrType type;		// attribute type
	int length;				// attribute length
	int indexNo;			// idex number (-1 if there is no index number)
} AttrMet;

class SYSM_MetaManager  {

	REM_RecordFileManager * rfm;
	SYSM_SystemManager * sysm;
	
public:
	SYSM_MetaManager (REM_RecordFileManager  * rfm, SYSM_SystemManager * sysm);
	~SYSM_MetaManager ();

	t_rc InsertRelationMetadata (char * relName, int recSize, int numOfAttributes, int numOfIndexes);
	t_rc InsertAttributeMetadata (char * relName, char * attrName, int attrOffset, t_attrType attrType, int attrLength, int indexNum);
	t_rc DeleteRelationMetadata (char * relName);
	t_rc DeleteAttributeMetadata (char * relName, char * attrName);
	t_rc UpdateRelationMetadata (char * p_RelName, char * n_RelName, int n_RecSize, int n_NumOfAttributes, int numOfIndexes);
	t_rc UpdateAttributeMetadata (char * p_RelName, char * p_AttrName, char * n_RelName, char * n_AttrName, int n_AttrOffset, t_attrType n_AttrType, int n_AttrLength, int indexNum);

	t_rc GetRelationMetadata (char * relName, RelMet &relMeta);
	t_rc GetAttributeMetadata (char * relName, char * attrName, AttrMet &attrMeta);

	friend class SYSM_UserManager;
};
#endif
