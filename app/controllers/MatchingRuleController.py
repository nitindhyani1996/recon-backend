

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

            if atm_data and switch_data and flex_cube_data:
                get_Matching_json = MatchingRuleService.getMatchingRuleJson(db,userId=10,category=1)
                matching_json = {
                    "matchCondition": get_Matching_json[0]['matchcondition'],
                    "tolerance": get_Matching_json[0]['tolerance']
                }
                reconMatchingData = await MatchingRuleService.match_three_way_async(atm_data,switch_data,flex_cube_data,matching_json)
                # ref_no = f"RECON{''.join(__import__('random').choices('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', k=2))}{__import__('datetime').datetime.now().strftime('%d%m%y')}"
                ref_no = "RECONfZ161225"
                result = MatchingRuleService.saveReconMatchingSummary(db,reconMatchingData,ref_no)
                return {
                    "success": True,
                    "status_code": 200,
                    "message": "Matching Data save successfully.",
                    "data": result,
                    # "dsd":reconMatchingData
                }
            else:
                return {
                    "success": False,
                    "status_code": 400,
                    "message": "Kindly upload all required files.",
                    "data": []
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
    def updateMatchingRule(db, rule_id: int, data: dict):
        result = MatchingRuleService.updateMatchingRule(db, rule_id, data)

        if not result:
            return {
                "success": False,
                "status_code": 404,
                "message": "Matching Rule not found",
                "data": None
            }

        return {
            "success": True,
            "status_code": 200,
            "message": "Matching Rule updated successfully",
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
        
    
