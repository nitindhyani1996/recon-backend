from fastapi import APIRouter, Depends, UploadFile, File
from sqlalchemy.orm import Session
from app.controllers.SampleController import SampleController
from app.controllers.FileUpload import FileUpload
from app.db.database import get_db

router = APIRouter()
fileUploadController = FileUpload()

# @router.post("/upload")
# async def upload_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
#     return await fileUploadController.upload_file(db, file)

@router.post("/uplaod")
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

@router.get("/db-test")
def db_test(db: Session = Depends(get_db)):
    return {"message": "PostgreSQL connected successfully!"}
