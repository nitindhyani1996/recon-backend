
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
        rows = db.query(ATMTransaction).all()  # simpler than db.execute
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
        rows = db.query(SwitchTransaction).all()
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
        rows = db.query(FlexcubeTransaction).all()
        return [
            {
                "id": row.id,
                "posted_datetime": row.posted_datetime,
                "fc_txn_id": row.fc_txn_id,
                "rrn": row.rrn,
                "stan": row.stan,
                "account_masked": row.account_masked,
                "dr": float(row.dr) if row.dr else None,
                "cr": float(row.cr) if row.cr else None,
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
                MatchingRule.rule_category == category  # Use integer comparison, not string
            )
            .order_by(MatchingRule.id.desc())
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

        matching_groups = matching_json.get("matchcondition", {}).get("matchingGroups", [])
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

                        # Compare A↔B if both exist
                        if a and b:
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

                            # Compare B↔C if both exist
                            if b and c:
                                switch_val = normalize(switch_row.get(b))
                                flex_val = normalize(flex_row.get(c))
                                
                                if switch_val != flex_val:
                                    switch_flex_ok = False
                                    break

                        if not switch_flex_ok:
                            break

                    # 🔹 STEP 3: AMOUNT TOLERANCE CHECK
                    if switch_flex_ok and tolerance_cfg.get("allowAmountDiff") == "Y":
                        try:
                            atm_amt = float(atm_row.get("amount", 0) or 0)
                            flex_amt = float(flex_row.get("dr", 0) or 0)  # Fixed: lowercase 'dr'
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
        from app.utils.recon_data_formatter import ReconDataFormatter
        
        result = db.execute(
            select(ReconMatchingSummary)
            .where(ReconMatchingSummary.recon_reference_number == ref_no)
        )

        record = result.scalar_one_or_none()

        # Convert matching results to CSV format for efficient storage
        def safe_amount(value):
            """Safely convert amount to string, handling None and empty values"""
            if value is None or value == "" or str(value).strip() == "None":
                return "0.00"
            try:
                return str(float(value))
            except (ValueError, TypeError):
                return "0.00"
        
        matched_transactions = []
        for match in reconMatchingData.get("matched", []):
            atm = match.get("ATM", {})
            matched_transactions.append({
                "rrn": atm.get("rrn") or "",
                "txn_type": atm.get("transactiontype") or atm.get("transaction_type") or "",
                "terminal_id": atm.get("terminalid") or atm.get("terminal_id") or "",
                "date": str(atm.get("datetime") or atm.get("transaction_datetime") or ""),
                "amount": safe_amount(atm.get("amount")),
                "result": "MATCHED"
            })
        
        partially_transactions = []
        for match in reconMatchingData.get("partially_matched", []):
            atm = match.get("ATM", {})
            partially_transactions.append({
                "rrn": atm.get("rrn") or "",
                "txn_type": atm.get("transactiontype") or atm.get("transaction_type") or "",
                "terminal_id": atm.get("terminalid") or atm.get("terminal_id") or "",
                "date": str(atm.get("datetime") or atm.get("transaction_datetime") or ""),
                "amount": safe_amount(atm.get("amount")),
                "result": "PARTIAL"
            })
        
        unmatched_transactions = []
        for match in reconMatchingData.get("unmatched", []):
            atm = match.get("ATM", {})
            unmatched_transactions.append({
                "rrn": atm.get("rrn") or "",
                "txn_type": atm.get("transactiontype") or atm.get("transaction_type") or "",
                "terminal_id": atm.get("terminalid") or atm.get("terminal_id") or "",
                "date": str(atm.get("datetime") or atm.get("transaction_datetime") or ""),
                "amount": safe_amount(atm.get("amount")),
                "result": "UNMATCHED"
            })
        
        # Format as CSV
        matched_csv = ReconDataFormatter.format_matched_data_csv(matched_transactions)
        partially_csv = ReconDataFormatter.format_matched_data_csv(partially_transactions)
        unmatched_csv = ReconDataFormatter.format_matched_data_csv(unmatched_transactions)

        if record:
            # UPDATE
            record.matched = matched_csv
            record.partially_matched = partially_csv
            record.un_matched = unmatched_csv
            record.added_by = 10
        else:
            # INSERT
            record = ReconMatchingSummary(
                recon_reference_number=ref_no,
                matched=matched_csv,
                partially_matched=partially_csv,
                un_matched=unmatched_csv,
                added_by=10
            )
            db.add(record)

        db.commit()
        db.refresh(record)

        return record
    
    def getReconAtmTransactionsSummery(db: Session):
        from app.utils.recon_data_formatter import ReconDataFormatter
        
        result = db.execute(
            select(ReconMatchingSummary)
            .order_by(desc(ReconMatchingSummary.id))
            .limit(1)
        )

        summary = result.scalar_one_or_none()
        
        if not summary:
            return None
        
        # Convert CSV data to frontend format
        matched_data = ReconDataFormatter.get_frontend_format(summary.matched or "")
        print(matched_data)
        partially_matched_data = ReconDataFormatter.get_frontend_format(summary.partially_matched or "")
        print(partially_matched_data)
        unmatched_data = ReconDataFormatter.get_frontend_format(summary.un_matched or "")
        print(unmatched_data)

        return {
            "recon_reference_number": summary.recon_reference_number,
            "created_at": summary.created_at.isoformat() if summary.created_at else None,
            "updated_at": summary.updated_at.isoformat() if summary.updated_at else None,
            "data": {
                "matched": matched_data,
                "partially_matched": partially_matched_data,
                "unmatched": unmatched_data
            },
            "summary": {
                "total_matched": len(matched_data),
                "total_partial": len(partially_matched_data),
                "total_unmatched": len(unmatched_data),
                "total": len(matched_data) + len(partially_matched_data) + len(unmatched_data)
            }
        }
    
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

    

    