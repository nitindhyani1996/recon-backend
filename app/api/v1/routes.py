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

@router.get("/db-test")
def db_test(db: Session = Depends(get_db)):
    return {"message": "PostgreSQL connected successfully!"}
