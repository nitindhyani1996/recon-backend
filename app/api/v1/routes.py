from fastapi import APIRouter, Body, Depends, Request, UploadFile, File
from sqlalchemy.orm import Session
from app.controllers.MatchingRuleController import MatchingRuleController
from app.controllers.SampleController import SampleController
from app.controllers.FileUpload import FileUpload
from app.controllers.TransactionInvestigationController import TransactionInvestigationController
from app.db.database import get_db
from app.controllers.ManualTransactionController import ManualTransactionController
from app.controllers.TxnJournalEntryController import TxnJournalEntryController

router = APIRouter()
fileUploadController = FileUpload()
matchingRuleController = MatchingRuleController()
transactionInvestigationController = TransactionInvestigationController()
manualTransactionController = ManualTransactionController()
txnJournalEntryController = TxnJournalEntryController()

@router.post("/journal-entries")
async def create_journal_entry(
    payload: dict = Body(...),
    db: Session = Depends(get_db)
):
    return await txnJournalEntryController.create_journal_entry(db, payload)

@router.post("/manual-transactions")
async def create_manual_transaction(
    payload: dict = Body(...),
    db: Session = Depends(get_db)
):
    return await manualTransactionController.create_manual_transaction(db, payload)

@router.post("/upload")
async def upload_file_correct(file: UploadFile = File(...), db: Session = Depends(get_db)):
    return await fileUploadController.upload_file(db, file)

@router.post("/bulk-upload")  
async def bulk_upload_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
    return await fileUploadController.upload_file(db, file)

@router.post("/uplaod")  # Keep old typo endpoint for backward compatibility
async def upload_file(file: UploadFile = File(...),db: Session = Depends(get_db)):
    return await fileUploadController.upload_file(db, file)

@router.get("/file-list")
async def getUplaodFileList(offset:int = 0, limit:int= 0, db: Session = Depends(get_db)):
    return await fileUploadController.get_file_list(db, offset, limit)

@router.get("/atm-matching-count")
async def getAtmTransactionsMatchingCount(db: Session = Depends(get_db)):
    return await fileUploadController.getAtmTransactionsMatchingCount(db)

@router.get("/atm-matching")
async def getAtmTransactionsMatchingDetails(offset:int = 0, limit:int= 0,tpye:int=0, db: Session = Depends(get_db)):
    return await fileUploadController.getAtmTransactionsMatchingDetails(db, offset, limit, type)

@router.get("/atm-not-matching")
async def getAtmTransactionsNotMatchingDetails(offset:int = 0, limit:int= 0,tpye:int=0, db: Session = Depends(get_db)):
    return await fileUploadController.getAtmTransactionsNotMatchingDetails(db, offset, limit, type)

@router.get("/atm-partially-matching")
async def getAtmTransactionsPartiallyMatchingDetails(offset:int = 0, limit:int= 0, db: Session = Depends(get_db)):
    return await fileUploadController.getAtmTransactionsPartiallyMatchingDetails(db, offset, limit)



# Matching Rule Builder Apis
@router.get("/matching-source-fields")
async def getMatchingSourceFields(db:Session = Depends(get_db),source:int=0):
    return await matchingRuleController.getMachingSourceFields(db,source)

@router.get("/matching-engine")
async def runMatchingEngine(db:Session = Depends(get_db)):
    return await matchingRuleController.runMatchingEngine(db)

@router.post("/matching-rule")
async def saveMarchingRule(db: Session = Depends(get_db), data: dict = Body(...)):
    # data = await request.json()  # <-- get JSON body
    return await matchingRuleController.saveMarchingRule(db, data)


@router.put("/matching-rule/{rule_id}")
async def updateMatchingRule(rule_id: int,data: dict = Body(...), db: Session = Depends(get_db)):
    return matchingRuleController.updateMatchingRule(db, rule_id, data)
    
# Atm matching API:
@router.get("/recon-atm-matching")
async def getReconAtmTransactionsSummery(db:Session = Depends(get_db)):
    return await matchingRuleController.getReconAtmTransactionsSummery(db)


# Transaction Investigation APIs:
@router.get("/transaction/{rrn}")
async def get_transaction_by_rrn(rrn: str, db: Session = Depends(get_db)):
    """Get transaction data from all sources (ATM, Switch, CBS) by RRN"""
    return await transactionInvestigationController.get_transaction_by_rrn(db, rrn)

@router.get("/atm/transaction/{rrn}")
async def get_atm_transaction_by_rrn(rrn: str, db: Session = Depends(get_db)):
    """Get ATM transaction data by RRN"""
    return await transactionInvestigationController.get_atm_transaction_by_rrn(db, rrn)

@router.get("/switch/transaction/{rrn}")
async def get_switch_transaction_by_rrn(rrn: str, db: Session = Depends(get_db)):
    """Get Switch transaction data by RRN"""
    return await transactionInvestigationController.get_switch_transaction_by_rrn(db, rrn)

@router.get("/cbs/transaction/{rrn}")
async def get_cbs_transaction_by_rrn(rrn: str, db: Session = Depends(get_db)):
    """Get CBS/Flexcube transaction data by RRN"""
    return await transactionInvestigationController.get_cbs_transaction_by_rrn(db, rrn)

@router.get("/clear-data")
def clearReconTables(db:Session = Depends(get_db)):
    return matchingRuleController.clearReconTables(db)

    

@router.get("/db-test")
def db_test(db: Session = Depends(get_db)):
    return {"message": "PostgreSQL connected successfully!"}
