#ifndef _SYSM_Parameters_h
#define _SYSM_Parameters_h

#define REL_NAME_SIZE (sizeof(char) * 100)																			// "Relation name" size
#define ATTR_NAME_SIZE (sizeof(char) * 50)																			// "attribute name" size
#define REL_SIZE  (REL_NAME_SIZE + sizeof(int) + sizeof(int) + sizeof(int))											// "relation record" size
#define ATTR_SIZE  (REL_NAME_SIZE + ATTR_NAME_SIZE + sizeof(int) + sizeof(t_attrType) + sizeof(int) + sizeof(int))	// "attribute record" size
#define REL_NAME_OFFSET 0																							// "relation name" offset in relation record
#define REL_REC_SIZE_OFFSET (REL_NAME_SIZE)																			// "record size" offset in relation record
#define REL_ATTR_NUM_OFFSET (REL_REC_SIZE_OFFSET + sizeof(int))														// "attribute amound" offset on relation record
#define REL_INX_NUM_OFFSET (REL_ATTR_NUM_OFFSET + sizeof(int))														// "number of indexes" offset on relation record
#define ATTR_REL_NAME_OFFSET 0																						// "relation name" offset in attribute record
#define ATTR_NAME_OFFSET (REL_NAME_SIZE)																			// "attribute name" offset in attribute record
#define ATTR_OFFSET_OFFSET (ATTR_NAME_OFFSET + ATTR_NAME_SIZE)														// "attribute offset" offset in attribute record
#define ATTR_TYPE_OFFSET (ATTR_OFFSET_OFFSET + sizeof(int))															// "attribute type" offset in attribute record
#define ATTR_LENGTH_OFFSET (ATTR_TYPE_OFFSET + sizeof(t_attrType))													// "attribute length" offset in attribute record
#define ATTR_INX_NO_OFFSET (ATTR_LENGTH_OFFSET + sizeof(int))														// "index number of attribute" offset in attribute record

#endif