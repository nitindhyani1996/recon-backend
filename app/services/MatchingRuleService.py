
import datetime
import json
from locale import normalize
from sqlalchemy.orm import Session
from sqlalchemy import desc, select, text
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
            """Normalize values for exact comparison"""
            if val is None:
                return ""
            return str(val).strip().upper()

        def check_exact_match(row1, row2, field1, field2):
            """Check if two fields match exactly"""
            if not field1 and not field2:
                return True  # Skip if field doesn't exist
            return normalize(row1.get(field1)) == normalize(row2.get(field2))

        def check_amount_tolerance(atm_row, flex_row):
            """Check if amount is within tolerance"""
            if tolerance_cfg.get("allowAmountDiff") == "Y":
                return True
                
            try:
                atm_amt = float(atm_row.get("amount", 0) or 0)
                flex_amt = float(flex_row.get("DR", 0) or 0)
                allowed_diff = float(tolerance_cfg.get("amountDiff", 0))
                return abs(atm_amt - flex_amt) <= allowed_diff
            except (ValueError, TypeError):
                return False

        def check_all_groups_match(row1, row2, source_field_key1, source_field_key2):
            """Check if all matching groups match between two sources"""
            for group in matching_groups:
                fields = group.get("fields", [])
                group_matched = True
                
                for f in fields:
                    field1 = f.get(source_field_key1)
                    field2 = f.get(source_field_key2)
                    
                    # Check if both fields exist and match
                    if field1 and field2:
                        if not check_exact_match(row1, row2, field1, field2):
                            group_matched = False
                            break
                
                # If any group fails, return False
                if not group_matched:
                    return False
            
            # All groups must match
            return True

        # Process each ATM row
        for atm_row in ATM_file:
            atm_matched = False
            best_partial_match = None

            # Try to find matching Switch row
            for switch_row in Switch_file:
                # Check ATM ↔ SWITCH matching (fieldA ↔ fieldB)
                atm_switch_match = check_all_groups_match(
                    atm_row, switch_row, 
                    "matching_fieldA", "matching_fieldB"
                )

                if not atm_switch_match:
                    continue

                # ATM matched with Switch, now try to match with Flexcube
                flex_matched = False
                
                for flex_row in Flexcube_file:
                    # Check SWITCH ↔ FLEXCUBE matching (fieldB ↔ fieldC)
                    switch_flex_match = check_all_groups_match(
                        switch_row, flex_row,
                        "matching_fieldB", "matching_fieldC"
                    )

                    # If SWITCH ↔ FLEXCUBE matched, check amount tolerance
                    if switch_flex_match:
                        if check_amount_tolerance(atm_row, flex_row):
                            # ✅ FULL 3-WAY MATCH FOUND
                            matched.append({
                                "ATM": atm_row,
                                "Switch": switch_row,
                                "Flexcube": flex_row
                            })
                            atm_matched = True
                            flex_matched = True
                            break
                        else:
                            # Amount tolerance failed, but other fields matched
                            if not best_partial_match:
                                best_partial_match = {
                                    "ATM": atm_row,
                                    "Switch": switch_row,
                                    "Flexcube": flex_row,
                                    "reason": "Amount tolerance exceeded"
                                }

                if atm_matched:
                    break
                
                # If ATM-Switch matched but no Flexcube match found
                if not flex_matched and not best_partial_match:
                    best_partial_match = {
                        "ATM": atm_row,
                        "Switch": switch_row,
                        "Flexcube": None,
                        "reason": "No matching Flexcube record"
                    }

            # Classify the ATM row
            if atm_matched:
                continue  # Already added to matched
            elif best_partial_match:
                partially_matched.append(best_partial_match)
            else:
                unmatched.append({
                    "ATM": atm_row,
                    "Switch": None,
                    "Flexcube": None,
                    "reason": "No matching Switch record"
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
    
    @staticmethod
    def clear_recon_atm_transaction_summary(db: Session) -> dict:
        try:
            # deleted_rows = db.query(ATMTransaction).delete()
            db.execute(text("""
                TRUNCATE TABLE
                    atm_transactions,
                    flexcube_transactions,
                    switch_transactions,
                    recon_matching_summary,
                    uploaded_files
                RESTART IDENTITY CASCADE;
            """))
            db.commit()

            return {
                "success": True,
                "message": "Data cleared successfully",
            }

        except Exception as e:
            db.rollback()
            return {
                "success": False,
                "message": "Failed to clear reconciliation data",
                "error": str(e)
            }


    

    