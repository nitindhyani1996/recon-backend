

import logging
from app.services.MatchingRuleService import MatchingRuleService

class MatchingRuleController:
    
    @staticmethod
    async def getMachingSourceFields(db,source):
        try:
            channelsSourceFields = await MatchingRuleService.getMachingSourceFields(db,source)
            return {
                    "success": True,
                    "status_code": 200,
                    "message": "Matching source fields fetched successfully",
                    "data": channelsSourceFields
                }
        except Exception as e:
            logging.exception("Error while fetching matching source fields")
            return {
                "success": False,
                "status_code": 500,
                "message": "Failed to fetch matching source fields",
                "error": str(e)
            }
        
    @staticmethod
    async def runMatchingEngine(db):
        try:
            atm_data = MatchingRuleService.getAllAtmTransactions(db)
            switch_data = MatchingRuleService.getAllSwitchTransactions(db)
            flex_cube_data = MatchingRuleService.getAllFlexcubeTransactions(db)
            matching_json = {
                "matchCondition": {
                    "matchingGroups": [
                        {
                            "groupId": "1",
                            "source": {
                                "sourceA": "ATM_file",
                                "sourceB": "Switch_file",
                                "sourceC": "Flexcube_file",
                                "fields": [
                                    {"matching_fieldA": "RRN", "matching_fieldB": "RRN", "condition": "="},
                                    {"matching_fieldB": "RRN", "matching_fieldC": "RRN", "condition": "="}
                                ]
                            }
                        }
                    ]
                },
                "tolerance": {"allowAmountDiff": "Y", "amountDiff": 10}
            }

            reconMatchingData = await MatchingRuleService.match_three_way_async(atm_data,switch_data,flex_cube_data,matching_json)
            # ref_no = f"RECON{''.join(__import__('random').choices('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', k=2))}{__import__('datetime').datetime.now().strftime('%d%m%y')}"
            ref_no = "RECONfZ161225"
            result = MatchingRuleService.saveReconMatchingSummary(db,reconMatchingData,ref_no)
            return {
                "success": True,
                "status_code": 200,
                "message": "Matching Data save successfully",
                "data": result
            }
        except Exception as e:
            logging.exception("Error while save Matching transactions")
            return {
                "success": False,
                "status_code": 500,
                "message": "Failed to save Matching transactions",
                "error": str(e)
            }

    @staticmethod
    async def saveMarchingRule(db, data):
        result = MatchingRuleService.saveMatchingRule(db,data)
        return {
            "success": True,
            "status_code": 200,
            "message": "Matching Rule Save Successfully",
            "data": result
        }
    
    @staticmethod
    async def updateMatchingRule(db,rule_id, data):
        result = MatchingRuleService.updateMatchingRule(db,rule_id,data)
        return {
            "success": True,
            "status_code": 200,
            "message": "Matching Rule Update Successfully",
            "data": result
        }

    
    @staticmethod
    async def getReconAtmTransactionsSummery(db):
        try:
            getAtmSummeryData = MatchingRuleService.getReconAtmTransactionsSummery(db)
            return {
                    "success": True,
                    "status_code": 200,
                    "message": "Recon ATM Transactions Summery Data",
                    "data": getAtmSummeryData
                }
        except Exception as e:
            logging.exception("Error while fetching matching source fields")
            return {
                "success": False,
                "status_code": 500,
                "message": "Failed to fetch matching source fields",
                "error": str(e)
            }
        
    
