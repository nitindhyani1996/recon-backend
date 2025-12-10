import json
from app.models.atm_transaction import ATMTransaction
from app.models.Upload import UploadedFile
from sqlalchemy.orm import Session

class BulkUploadService:
    @staticmethod
    async def saveUploadedFile(db: Session, uploadFileData):
        try:
            new_file = UploadedFile(
                file_description= json.dumps(uploadFileData),
                uploaded_by= 1,
                status=1,
            )

            db.add(new_file)
            db.commit()
            db.refresh(new_file)
            return {
                "status": "success",
                "insertedId": new_file.id,
            }

        except Exception as e:
            db.rollback()
            return {
                "status": "error",
                "message": str(e)
            }
        
    @staticmethod
    async def saveATMFileData(db: Session, mapped_df, uploaded_file_id):
        duplicates = []
        new_records = []
        # print('mapped_df',mapped_df)
        for row in mapped_df:
            existing_record = db.query(ATMTransaction).filter(
                ATMTransaction.rrn == row["rrn"],
                ATMTransaction.terminalid == row["terminalid"]
            ).first()

            if existing_record:
                duplicates.append(row)
                continue

            new_records.append(ATMTransaction(
                datetime=row["datetime"],
                terminalid=row["terminalid"],
                location=row["location"],
                atmindex=row["atmindex"],
                pan_masked=row["pan_masked"],
                account_masked=row["account_masked"],
                transactiontype=row["transactiontype"],
                amount=row["amount"],
                currency=row["currency"],
                stan=row["stan"],
                rrn=row["rrn"],
                auth=row["auth"],
                responsecode=row["responsecode"],
                responsedesc=row["responsedesc"],
                uploaded_by= uploaded_file_id,
            ))

        if new_records:
            db.add_all(new_records)
            db.commit()

        return {
            "status": "success",
            "message": f"{len(new_records)} records inserted, {len(duplicates)} duplicates skipped",
            "recordsSaved": len(new_records),
            "duplicateRecords": duplicates
        }
    

    @staticmethod
    async def saveSwitchFileData(db: Session, mapped_df, uploaded_file_id):
        duplicates = []
        new_records = []
        # print('mapped_df',mapped_df)
        for row in mapped_df:
            existing_record = db.query(ATMTransaction).filter(
                ATMTransaction.rrn == row["rrn"],
                ATMTransaction.terminalid == row["terminalid"]
            ).first()

            if existing_record:
                duplicates.append(row)
                continue

            new_records.append(ATMTransaction(
                datetime=row["datetime"],
                terminalid=row["terminalid"],
                location=row["location"],
                atmindex=row["atmindex"],
                pan_masked=row["pan_masked"],
                account_masked=row["account_masked"],
                transactiontype=row["transactiontype"],
                amount=row["amount"],
                currency=row["currency"],
                stan=row["stan"],
                rrn=row["rrn"],
                auth=row["auth"],
                responsecode=row["responsecode"],
                responsedesc=row["responsedesc"],
                uploaded_by= uploaded_file_id,
            ))

        if new_records:
            db.add_all(new_records)
            db.commit()

        return {
            "status": "success",
            "message": f"{len(new_records)} records inserted, {len(duplicates)} duplicates skipped",
            "recordsSaved": len(new_records),
            "duplicateRecords": duplicates
        }
