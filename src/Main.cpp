#include "UIM.h"

using namespace std;

int main() {
    
	t_rc rc;
	UIM_UserInterfaceManager * uim_Mgr = new UIM_UserInterfaceManager();

	rc = uim_Mgr->StartUI();
	if (rc != OK) { DisplayReturnCode(rc); Pause(); exit(-1); }

}