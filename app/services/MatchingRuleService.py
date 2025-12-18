
import datetime
import json
from locale import normalize
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

    def getMatchingRuleJson(db: Session, userId=10, category=1):
        rows = (
            db.query(MatchingRule)
            .filter(
                MatchingRule.added_by == str(userId),
                MatchingRule.rule_category == str(category)
            )
            .order_by(MatchingRule.id.desc())
            .limit(20)
            .all()
        )

        return [
            {
                "id": row.id,
                "basic_details": row.basic_details,
                "classification": row.classification,
                "rule_category": row.rule_category,
                "matchcondition": row.matchcondition,
                "tolerance": row.tolerance,
                "added_by": row.added_by
            }
            for row in rows
        ]


    
    async def match_three_way_async(ATM_file, Switch_file, Flexcube_file, matching_json):
        matched = []
        partially_matched = []
        unmatched = []

        matching_groups = matching_json["matchCondition"].get("matchingGroups", [])
        tolerance_cfg = matching_json.get("tolerance", {})

        def normalize(val):
            """Normalize values for comparison"""
            if val is None:
                return ""
            return str(val).strip().upper()

        for atm_row in ATM_file:
            atm_matched = False
            atm_partial = False
            last_switch_row = None

            for switch_row in Switch_file:
                atm_switch_ok = True

                # 🔹 STEP 1: ATM ↔ SWITCH COMPARISON
                for group in matching_groups:
                    fields = group.get("fields", [])

                    for f in fields:
                        a = f.get("matching_fieldA")
                        b = f.get("matching_fieldB")
                        c = f.get("matching_fieldC")

                        # Only compare A↔B if both A and B exist (and C doesn't)
                        if a and b and not c:
                            if normalize(atm_row.get(a)) != normalize(switch_row.get(b)):
                                atm_switch_ok = False
                                break

                    if not atm_switch_ok:
                        break

                if not atm_switch_ok:
                    continue

                # ATM matched with Switch
                atm_partial = True
                last_switch_row = switch_row

                # 🔹 STEP 2: SWITCH ↔ FLEXCUBE COMPARISON
                for flex_row in Flexcube_file:
                    switch_flex_ok = True

                    for group in matching_groups:
                        fields = group.get("fields", [])

                        for f in fields:
                            b = f.get("matching_fieldB")
                            c = f.get("matching_fieldC")
                            a = f.get("matching_fieldA")

                            # Only compare B↔C if both B and C exist (and A doesn't)
                            if b and c and not a:
                                if normalize(switch_row.get(b)) != normalize(flex_row.get(c)):
                                    switch_flex_ok = False
                                    break

                        if not switch_flex_ok:
                            break

                    # 🔹 STEP 3: AMOUNT TOLERANCE CHECK
                    if switch_flex_ok and tolerance_cfg.get("allowAmountDiff") == "N":
                        try:
                            atm_amt = float(atm_row.get("amount", 0) or 0)
                            flex_amt = float(flex_row.get("DR", 0) or 0)
                            allowed_diff = float(tolerance_cfg.get("amountDiff", 0))

                            if abs(atm_amt - flex_amt) > allowed_diff:
                                switch_flex_ok = False
                        except (ValueError, TypeError):
                            switch_flex_ok = False

                    # 🔹 FULL MATCH FOUND
                    if switch_flex_ok:
                        matched.append({
                            "ATM": atm_row,
                            "Switch": switch_row,
                            "Flexcube": flex_row
                        })
                        atm_matched = True
                        break

                if atm_matched:
                    break

            # 🔹 FINAL CLASSIFICATION
            if atm_matched:
                continue

            if atm_partial:
                partially_matched.append({
                    "ATM": atm_row,
                    "Switch": last_switch_row,
                    "Flexcube": None
                })
            else:
                unmatched.append({
                    "ATM": atm_row,
                    "Switch": None,
                    "Flexcube": None
                })

        return {
            "matched": matched,
            "partially_matched": partially_matched,
            "unmatched": unmatched
        }





    

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
            matchcondition=data.get("matchCondition"),
            tolerance=data.get("tolerance"),
            added_by=str(data.get("added_by", 10))  # default to "10" if not provided
        )

        db.add(record)
        db.commit()
        db.refresh(record)

        return record
    

    @staticmethod
    def updateMatchingRule(db: Session, rule_id: int, data: dict):
        record = (
            db.query(MatchingRule)
            .filter(MatchingRule.id == rule_id)
            .first()
        )

        if not record:
            return None

        # Update fields safely
        if "basic" in data:
            record.basic_details = data["basic"]

        if "classification" in data:
            record.classification = data["classification"]

        if "rule_category" in data:
            record.rule_category = str(data["rule_category"])

        if "matchCondition" in data:
            record.matchcondition = data["matchCondition"]  # ✔️ FIXED NAME

        if "tolerance" in data:
            record.tolerance = data["tolerance"]

        if "added_by" in data:
            record.added_by = str(data["added_by"])

        db.commit()
        db.refresh(record)

        return record
    

    def safe_datetime(value):
        if value is None or value == "":
            return None

        if isinstance(value, datetime):
            return value

        # ATM file format: 12/1/2025 9:17
        return datetime.strptime(value.strip(), "%m/%d/%Y %H:%M")

    

    