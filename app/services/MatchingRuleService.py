
import json
from sqlalchemy.orm import Session
from sqlalchemy import desc, select
from app.enums.matching_source import MatchingSource
from app.models.MatchingRule import MatchingRule
from app.models.ReconMatchingSummary import ReconMatchingSummary
from app.models.FlexcubeTransaction import FlexcubeTransaction
from app.models.SwitchTransaction import SwitchTransaction
from app.models.atm_transaction import ATMTransaction

class MatchingRuleService:

    @staticmethod
    async def getMachingSourceFields(db: Session, source):
        try:
            if source == MatchingSource.ATM:
                model = ATMTransaction
            elif source == MatchingSource.SWITCH:
                model = SwitchTransaction
            elif source == MatchingSource.FLEXCUBE:
                model = FlexcubeTransaction

            else:
                return []
            
            return [
                {
                    "column_name": column.name,
                    "type": str(column.type)
                }
                for column in model.__table__.columns
            ]

        except Exception as e:
            db.rollback()
            return {
                "status": "error",
                "message": str(e)
            }
        
    @staticmethod
    def getAllAtmTransactions(db: Session):
        rows = db.query(ATMTransaction).limit(20).all()  # simpler than db.execute
        return [
            {
                "id": row.id,
                "datetime": row.datetime,
                "terminalid": row.terminalid,
                "location": row.location,
                "atmindex": row.atmindex,
                "pan_masked": row.pan_masked,
                "account_masked": row.account_masked,
                "transactiontype": row.transactiontype,
                "amount": float(row.amount) if row.amount else None,
                "currency": row.currency,
                "stan": row.stan,
                "rrn": row.rrn,
                "auth": row.auth,
                "responsecode": row.responsecode,
                "responsedesc": row.responsedesc,
                "uploaded_by": row.uploaded_by,
                "created_at": row.created_at,
                "updated_at": row.updated_at
            } 
            for row in rows
        ]
    
    @staticmethod
    def getAllSwitchTransactions(db: Session):
        rows = db.query(SwitchTransaction).limit(20).all()
        return [
            {
                "id": row.id,
                "datetime": row.datetime,
                "direction": row.direction,
                "mti": row.mti,
                "pan_masked": row.pan_masked,
                "processingcode": row.processingcode,
                "amountminor": float(row.amountminor) if row.amountminor else None,
                "currency": row.currency,
                "stan": row.stan,
                "rrn": row.rrn,
                "terminalid": row.terminalid,
                "source": row.source,
                "destination": row.destination,
                "responsecode": row.responsecode,
                "authid": row.authid,
                "uploaded_by": row.uploaded_by,
                "created_at": row.created_at,
                "updated_at": row.updated_at
            } 
            for row in rows
        ]

    @staticmethod
    def getAllFlexcubeTransactions(db: Session):
        rows = db.query(FlexcubeTransaction).limit(20).all()
        return [
            {
                "id": row.id,
                "posted_datetime": row.posted_datetime,
                "fc_txn_id": row.fc_txn_id,
                "rrn": row.rrn,
                "stan": row.stan,
                "account_masked": row.account_masked,
                "currency": row.currency,
                "status": row.status,
                "description": row.description,
                "uploaded_by": row.uploaded_by,
                "created_at": row.created_at,
                "updated_at": row.updated_at
            } 
            for row in rows
        ]
    
    async def match_three_way_async(ATM_file, Switch_file, Flexcube_file, matching_json):
        matched = []
        partially_matched = []
        unmatched = []

        for group in matching_json["matchCondition"]["matchingGroups"]:
            fields = group["source"]["fields"]
            for atm_row in ATM_file:
                found = False
                for switch_row in Switch_file:
                    atm_switch_match = True
                    for f in fields:
                        a_field = f.get("matching_fieldA")
                        b_field = f.get("matching_fieldB")
                        c_field = f.get("matching_fieldC")
                        cond = f["condition"]

                        if a_field and b_field:
                            val_a = atm_row.get(a_field)
                            val_b = switch_row.get(b_field)
                            if cond == "=" and val_a != val_b:
                                atm_switch_match = False
                            elif cond == ">=" and val_a < val_b:
                                atm_switch_match = False

                    if atm_switch_match:
                        for flex_row in Flexcube_file:
                            flex_match = True
                            for f in fields:
                                b_field = f.get("matching_fieldB")
                                c_field = f.get("matching_fieldC")
                                cond = f["condition"]

                                if b_field and c_field:
                                    val_b = switch_row.get(b_field)
                                    val_c = flex_row.get(c_field)
                                    if cond == "=" and val_b != val_c:
                                        flex_match = False
                                    elif cond == ">=" and val_b < val_c:
                                        flex_match = False

                            # Amount tolerance
                            if matching_json["tolerance"]["allowAmountDiff"]:
                                amount_diff = abs(atm_row.get("Amount", 0) - flex_row.get("DR", 0))
                                if amount_diff > matching_json["tolerance"]["amountDiff"]:
                                    flex_match = False

                            if flex_match:
                                matched.append({"ATM": atm_row, "Switch": switch_row, "Flexcube": flex_row})
                                found = True
                                break
                if not found:
                    partially_matched.append(atm_row)

        return {"matched": matched, "partially_matched": partially_matched, "unmatched": unmatched}
    

    def saveReconMatchingSummary(db: Session, reconMatchingData, ref_no):
        result = db.execute(
            select(ReconMatchingSummary)
            .where(ReconMatchingSummary.recon_reference_number == ref_no)
        )

        record = result.scalar_one_or_none()

        matched_json = json.dumps(reconMatchingData["matched"],default=str)
        partially_json = json.dumps(reconMatchingData["partially_matched"],default=str)
        unmatched_json = json.dumps(reconMatchingData["unmatched"],default=str)

        if record:
            # UPDATE
            record.matched = matched_json
            record.partially_matched = partially_json
            record.un_matched = unmatched_json
            record.added_by = 10
        else:
            # INSERT
            record = ReconMatchingSummary(
                recon_reference_number=ref_no,
                matched=matched_json,
                partially_matched=partially_json,
                un_matched=unmatched_json,
                added_by=10
            )
            db.add(record)

        db.commit()
        db.refresh(record)

        return record
    
    def getReconAtmTransactionsSummery(db: Session):
        result = db.execute(
            select(ReconMatchingSummary)
            .order_by(desc(ReconMatchingSummary.id))
            .limit(1)
        )

        return result.scalar_one_or_none()
    
    def saveMatchingRule(db: Session, reconMatchingData):
        # If reconMatchingData is a Pydantic model, convert to dict
        if hasattr(reconMatchingData, "dict"):
            data = reconMatchingData.dict()
        else:
            data = reconMatchingData

        record = MatchingRule(
            basic_details=data.get("basic"),
            classification=data.get("classification"),
            rule_category=data.get("rule_category"),
            matchcondition=data.get("matchcondition"),
            tolerance=data.get("tolerance"),
            added_by=str(data.get("added_by", 10))  # default to "10" if not provided
        )

        db.add(record)
        db.commit()
        db.refresh(record)

        return record
    

    async def updateMatchingRule(db: Session, rule_id: int, data: dict):
        # Find the record by id
        record = db.query(MatchingRule).filter(MatchingRule.id == rule_id).first()
        if not record:
            return None

        # Update fields if they exist in data
        if "basic" in data:
            record.basic_details = data["basic"]
        if "classification" in data:
            record.classification = data["classification"]
        if "rule_category" in data:
            record.rule_category = data["rule_category"]
        if "matchCondition" in data:
            record.match_condition = data["matchCondition"]
        if "tolerance" in data:
            record.tolerance = data["tolerance"]
        if "added_by" in data:
            record.added_by = str(data["added_by"])

        db.commit()
        db.refresh(record)

        return record

    

    