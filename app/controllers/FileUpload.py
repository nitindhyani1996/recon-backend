import asyncio
from app.utils.file_reader import read_file_by_extension
from app.utils.smart_column_mapper import SmartColumnMapper
from app.services.bulkUploadService import BulkUploadService
import pandas as pd
from typing import Dict, Any
from app.config.column_patterns import COLUMN_PATTERNS
import re

class FileUpload:
    @staticmethod
    # async def upload_file(db, file) -> Dict[str, Any]:
    #     """
    #     Upload and process file with smart column detection.
    #     Detects column types based on data patterns, NOT column names.
    #     """
    #     # Step 1: Read file content
    #     file_data = await read_file_by_extension(file)
        
    #     # Step 2: Create DataFrame
    #     df = pd.DataFrame(file_data["data"])
    #     # Step 3: Apply smart column mapping
    #     mapper = SmartColumnMapper()
    #     processing_result = mapper.process_uploaded_file(df)
    #     mapped_df = pd.DataFrame(processing_result["data_preview"])
    #     return {'mapped_df':processing_result["data_preview"]}
    #     save_result = await BulkUploadService.save_bulk(db, mapped_df)
    #     return save_result
    
    async def upload_file(db, file) -> Dict[str, Any]:
        try:
            # Read file
            file_data = await read_file_by_extension(file)
            df = pd.DataFrame(file_data["data"]) if isinstance(file_data, dict) and "data" in file_data else pd.DataFrame(file_data)

            # Normalize columns
            cols = [str(c).strip().lower().replace(" ", "").replace("_", "") for c in df.columns]
            
            mti_found = any("mti" in col for col in cols)
            atm_index_found = any("atmindex" in col for col in cols)
            flexcube_index_found = any("fctxnid" in col for col in cols)
            fileType = {}        
            if mti_found:
                fileType = {'fileType':"SWITCH","totalRecords":len(df),"validRecords":len(df), "invalidRecords":0, 'fileName':file.filename}
            elif atm_index_found:
                fileType = {'fileType':"ATM","totalRecords":len(df),"validRecords":len(df), "invalidRecords":0, 'fileName':file.filename}
            elif flexcube_index_found:
                fileType = {'fileType':"FLEXCUBE","totalRecords":len(df),"validRecords":len(df), "invalidRecords":0, 'fileName':file.filename}
            
            if fileType:
                save_result = await BulkUploadService.saveUploadedFile(db, fileType)
                df.columns = [str(col).strip().lower() for col in df.columns]
                normalized_data = df.to_dict(orient="records")

                if save_result['status'] == 'success':
                    if fileType['fileType'] == "ATM":
                        saveAtmResult = await BulkUploadService.saveATMFileData(db, normalized_data, save_result['insertedId'])
                        return {"data": fileType,"result": saveAtmResult,  "message": "ATM file uploded"}
                    elif fileType['fileType'] == "SWITCH":
                        saveSwitchresult = await BulkUploadService.saveSwitchFileData(db, normalized_data, save_result['insertedId'])
                        return {"data": fileType,"result": saveSwitchresult,  "message": "Switch file uploded"}
                    elif fileType['fileType'] == "FLEXCUBE":
                        saveSwitchresult = await BulkUploadService.saveFlexCubeFileData(db, normalized_data, save_result['insertedId'])
                        return {"data": fileType,"result": saveSwitchresult,  "message": "Switch file uploded"}
                    return { "message": "Not FOund"}
                else:
                    return {"data": fileType,"result": save_result,  "message": "file uploded with errors"}
            else:
                return {"data": fileType, "message": "Could not determine file type based on column patterns."}

        except Exception as e:
            return {"file_type": "ERROR", "error": str(e)}
        

    @staticmethod
    async def get_file_list(db, offset: int, limit: int):
        return BulkUploadService.get_file_list(db, offset, limit)
    
    @staticmethod
    async def getAtmTransactionsMatchingCount(db):
        return BulkUploadService.getAtmTransactionsMatchingCount(db)
    
    @staticmethod
    async def getAtmTransactionsMatchingDetails(db, offset, limit, type):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,  # default thread pool
            BulkUploadService.getAtmTransactionsMatchingDetails,
            db, offset, limit, type
        )
        return result
    
    @staticmethod
    async def getAtmTransactionsNotMatchingDetails(db, offset, limit, type):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,  # default thread pool
            BulkUploadService.getAtmTransactionsNotMatchingDetails,
            db, offset, limit, type
        )
        return result
    
     
    @staticmethod
    async def getAtmTransactionsPartiallyMatchingDetails(db, offset, limit):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,  # default thread pool
            BulkUploadService.getAtmTransactionsPartiallyMatchingDetails,
            db, offset, limit
        )
        print('result', len(result))
        return result


