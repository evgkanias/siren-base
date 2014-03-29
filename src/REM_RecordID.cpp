#include "REM_RecordID.h"
#include <iostream>
#include <cstdlib>

REM_RecordID::REM_RecordID() {
	p_isSet = false;
	s_isSet = false;
}

REM_RecordID::REM_RecordID(int pageID, int slot) {
	this->pageID = pageID;
	this->slot = slot;
	p_isSet = true;
	s_isSet = true;
}

REM_RecordID::~REM_RecordID() {}

t_rc REM_RecordID::GetPageID( int &pageID) const {
	if (p_isSet) {
		pageID = this->pageID;
		return (OK);
	} else return (REM_PAGEIDENTIFIERRETURNERROR);
}

t_rc REM_RecordID::GetSlot(int &slot) const{
	if (s_isSet) {
		slot = this->slot;
		return (OK);
	} else return (REM_SLOTRETURNERROR);
}

t_rc REM_RecordID::SetPageID(int pageID) {
	if (this->pageID = pageID) {
		p_isSet = true;
		return (OK);
	} else return (REM_PAGEIDENTIFIERSETERROR);     
}

t_rc REM_RecordID::SetSlot(int slot) {
	if(this->slot = slot) {
		s_isSet = true;
		return (OK);
	} else return (REM_SLOTSETERROR);
}
