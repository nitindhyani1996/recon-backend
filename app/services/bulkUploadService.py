import json
from app.models.atm_transaction import ATMTransaction
from app.models.Upload import UploadedFile
from app.models.SwitchTransaction import SwitchTransaction
from app.models.FlexcubeTransaction import FlexcubeTransaction
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
            existing_record = db.query(SwitchTransaction).filter(
                # ATMTransaction.rrn == row["rrn"],
                SwitchTransaction.terminalid == row["terminalid"]
            ).first()
            
            if existing_record:
                duplicates.append(row)
                continue
            new_records.append(SwitchTransaction(
                datetime=row["datetime"],
                direction=row["direction"],
                mti=row["mti"],
                pan_masked=row["pan_masked"],
                processingcode=row["processingcode"],
                amountminor=row["amountminor"],
                currency=row["currency"],
                terminalid=row["terminalid"],
                stan=row["stan"],
                rrn=row["rrn"],
                source=row["source"],
                destination=row["destination"],
                # responsecode=row["responsecode"],
                # authid=row["authid"],
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
    async def saveFlexCubeFileData(db: Session, mapped_df, uploaded_file_id):
        duplicates = []
        new_records = []
        # print('mapped_df',mapped_df)
        for row in mapped_df:
            existing_record = db.query(FlexcubeTransaction).filter(
                # ATMTransaction.rrn == row["rrn"],
                FlexcubeTransaction.fc_txn_id == row["fc_txn_id"]
            ).first()
            
            if existing_record:
                duplicates.append(row)
                continue
            new_records.append(FlexcubeTransaction(
                posted_datetime=row["posteddatetime"],
                fc_txn_id=row["fc_txn_id"],
                rrn=row["rrn"],
                stan=row["stan"],
                account_masked=row["account_masked"],
                dr=row["dr"],
                currency=row["currency"],
                status=row["status"],
                description=row["description"],
                cr=row["cr"],
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
    def get_file_list(db: Session, offset: int, limit: int):
        query = db.query(UploadedFile)

        total = query.count()

        data = (
            query.order_by(UploadedFile.created_at.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )

        return {
            "status": "success",
            "offset": offset,
            "limit": limit,
            "total": total,
            "data": data
        }
    
    
