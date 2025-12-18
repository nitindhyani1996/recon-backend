import json

import pandas as pd
from app.models.atm_transaction import ATMTransaction
from app.models.Upload import UploadedFile
from app.models.SwitchTransaction import SwitchTransaction
from app.models.FlexcubeTransaction import FlexcubeTransaction
from sqlalchemy.orm import Session
from sqlalchemy import func, cast, String, select, text, union_all, case
from sqlalchemy.orm import aliased
import numpy as np
from app.services.MatchingRuleService import MatchingRuleService

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
                datetime= (row.get("datetime") or "").strip() or None,
                terminalid=(row.get("terminalid") or "").strip() or None,
                location=(row.get("location") or "").strip() or None,
                atmindex=(row.get("atmindex") or "").strip() or None,
                pan_masked=(row.get("pan_masked") or "").strip() or None,
                account_masked=(row.get("account_masked") or "").strip() or None,
                transactiontype=(row.get("transactiontype") or "").strip() or None,
                amount=row.get("amount") if row.get("amount") not in ("", None) else None,
                currency=(row.get("currency") or "").strip() or None,
                stan=(row.get("stan") or "").strip() or None,
                rrn=(row.get("rrn") or "").strip().replace(" ", "") or None,
                auth=(row.get("auth") or "").strip() or None,
                responsecode=(row.get("responsecode") or "").strip() or None,
                responsedesc=(row.get("responsedesc") or "").strip() or None,
                uploaded_by=uploaded_file_id
            ))


            # new_records.append(ATMTransaction(
            #     datetime= row["datetime"],
            #     terminalid=row["terminalid"],
            #     location=row["location"],
            #     atmindex=row["atmindex"],
            #     pan_masked=row["pan_masked"],
            #     account_masked=row["account_masked"],
            #     transactiontype=row["transactiontype"],
            #     amount=row["amount"],
            #     currency=row["currency"],
            #     stan=row["stan"],
            #     rrn=row["rrn"],
            #     auth=row["auth"],
            #     responsecode=row["responsecode"],
            #     responsedesc=row["responsedesc"],
            #     uploaded_by= uploaded_file_id,
            # ))

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
            # new_records.append(SwitchTransaction(datetime=row.get("datetime"), direction=(row.get("direction") or "").strip() or None, mti=(row.get("mti") or "").strip() or None, pan_masked=(row.get("pan_masked") or "").strip() or None, processingcode=(row.get("processingcode") or "").strip() or None, amountminor=row.get("amountminor") if row.get("amountminor") not in ("", None) else None, currency=(row.get("currency") or "").strip() or None, terminalid=(row.get("terminalid") or "").strip() or None, stan=(row.get("stan") or "").strip() or None, rrn=(row.get("rrn") or "").strip().replace(" ", "") or None, source=(row.get("source") or "").strip() or None, destination=(row.get("destination") or "").strip() or None, uploaded_by=uploaded_file_id))

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
            # new_records.append(FlexcubeTransaction(fc_txn_id=(row.get("fc_txn_id") or "").strip() or None, rrn=(row.get("rrn") or "").strip().replace(" ", "") or None, stan=(row.get("stan") or "").strip() or None, account_masked=(row.get("account_masked") or "").strip() or None, dr=row.get("dr") if row.get("dr") not in ("", None) else None, currency=(row.get("currency") or "").strip() or None, status=(row.get("status") or "").strip() or None, description=(row.get("description") or "").strip() or None, uploaded_by=uploaded_file_id))

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
                # cr=row["cr"],
                uploaded_by= uploaded_file_id,
                # fc_txn_id = row["fc_txn_id"].strip() if row.get("fc_txn_id") else None
                # rrn= row["rrn"].strip() if row.get("rrn") else None
                # stan = row["stan"].strip() if row.get("stan") else None
                # account_masked = row["account_masked"].strip() if row.get("account_masked") else None
                # dr = row["dr"].strip() if row.get("dr") else None
                # currency = row["currency"].strip() if row.get("currency") else None
                # status = row["status"].strip() if row.get("status") else None
                # description = row["description"].strip() if row.get("description") else None
                # # row["cr"].strip() if row.get("cr") else None
                # uploaded_by= uploaded_file_id
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
    
    @staticmethod
    def getAtmTransactionsMatchingCount(db: Session):

        query = """
        WITH combined_rrn AS (
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'ATM' AS source
            FROM atm_transactions
            WHERE rrn IS NOT NULL

            UNION ALL
            
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'SWITCH' AS source
            FROM switch_transactions
            WHERE rrn IS NOT NULL

            UNION ALL
            
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'FLEXCUBE' AS source
            FROM flexcube_transactions
            WHERE rrn IS NOT NULL
        ),
        rrn_summary AS (
            SELECT rrn,
                COUNT(DISTINCT source) AS match_count
            FROM combined_rrn
            GROUP BY rrn
        )
        SELECT 
            SUM(CASE WHEN match_count = 3 THEN 1 ELSE 0 END) AS fully_matched,
            SUM(CASE WHEN match_count = 2 THEN 1 ELSE 0 END) AS partially_matched,
            SUM(CASE WHEN match_count = 1 THEN 1 ELSE 0 END) AS not_matched
        FROM rrn_summary;
        """

        # MUCH CLEANER FIX
        with db.connection() as conn:
            df = pd.read_sql(text(query), conn)

        return {
            "fully_matched": int(df["fully_matched"][0] or 0),
            "partially_matched": int(df["partially_matched"][0] or 0),
            "not_matched": int(df["not_matched"][0] or 0)
        }
    
    @staticmethod
    def getAtmTransactionsMatchingCount(db: Session):

        query = """
        WITH combined_rrn AS (
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'ATM' AS source
            FROM atm_transactions
            WHERE rrn IS NOT NULL

            UNION ALL
            
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'SWITCH' AS source
            FROM switch_transactions
            WHERE rrn IS NOT NULL

            UNION ALL
            
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'FLEXCUBE' AS source
            FROM flexcube_transactions
            WHERE rrn IS NOT NULL
        ),
        rrn_summary AS (
            SELECT rrn,
                COUNT(DISTINCT source) AS match_count
            FROM combined_rrn
            GROUP BY rrn
        )
        SELECT 
            SUM(CASE WHEN match_count = 3 THEN 1 ELSE 0 END) AS fully_matched,
            SUM(CASE WHEN match_count = 2 THEN 1 ELSE 0 END) AS partially_matched,
            SUM(CASE WHEN match_count = 1 THEN 1 ELSE 0 END) AS not_matched
        FROM rrn_summary;
        """

        # MUCH CLEANER FIX
        with db.connection() as conn:
            df = pd.read_sql(text(query), conn)

        return {
            "fully_matched": int(df["fully_matched"][0] or 0),
            "partially_matched": int(df["partially_matched"][0] or 0),
            "not_matched": int(df["not_matched"][0] or 0)
        }
    

    @staticmethod
    def getAtmTransactionsMatchingDetails(db: Session, offset: int = 0, limit: int = 100, type: int = 0):
        """
        Get fully matched RRN records (present in ATM, SWITCH, and FLEXCUBE)
        with all table columns. Returns paginated results.
        """
        query = f"""
        WITH combined_rrn AS (
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'ATM' AS source
            FROM atm_transactions
            WHERE rrn IS NOT NULL

            UNION ALL
            
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'SWITCH' AS source
            FROM switch_transactions
            WHERE rrn IS NOT NULL

            UNION ALL
            
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'FLEXCUBE' AS source
            FROM flexcube_transactions
            WHERE rrn IS NOT NULL
        ),
        rrn_summary AS (
            SELECT
                rrn,
                COUNT(DISTINCT source) AS match_count
            FROM combined_rrn
            GROUP BY rrn
        ),
        fully_matched AS (
            SELECT rrn
            FROM rrn_summary
            WHERE match_count = 3
        ),
        atm_one AS (
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(rrn AS VARCHAR)) ORDER BY id ASC) AS rn
                FROM atm_transactions
            ) t
            WHERE rn = 1
        ),
        switch_one AS (
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(rrn AS VARCHAR)) ORDER BY id ASC) AS rn
                FROM switch_transactions
            ) t
            WHERE rn = 1
        ),
        flex_one AS (
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(rrn AS VARCHAR)) ORDER BY id ASC) AS rn
                FROM flexcube_transactions
            ) t
            WHERE rn = 1
        )
        SELECT 
            f.rrn AS rrn,

            -- ATM columns
            a.id AS atm_id,
            a.datetime AS atm_datetime,
            a.terminalid AS atm_terminalid,
            a.location AS atm_location,
            a.atmindex AS atm_index,
            a.pan_masked AS atm_pan_masked,
            a.account_masked AS atm_account_masked,
            a.transactiontype AS atm_transactiontype,
            a.amount AS atm_amount,
            a.currency AS atm_currency,
            a.stan AS atm_stan,
            a.auth AS atm_auth,
            a.responsecode AS atm_responsecode,
            a.responsedesc AS atm_responsedesc,
            a.uploaded_by AS atm_uploaded_by,

            -- SWITCH columns
            s.id AS switch_id,
            s.datetime AS switch_datetime,
            s.terminalid AS switch_terminalid,
            s.direction AS switch_direction,
            s.mti AS switch_mti,
            s.pan_masked AS switch_pan_masked,
            s.processingcode AS switch_processingcode,
            s.amountminor AS switch_amountminor,
            s.currency AS switch_currency,
            s.stan AS switch_stan,
            s.rrn AS switch_rrn,
            s.source AS switch_source,
            s.destination AS switch_destination,
            s.uploaded_by AS switch_uploaded_by,

            -- FLEXCUBE columns
            fc.id AS fc_id,
            fc.posted_datetime AS fc_posted_datetime,
            fc.fc_txn_id AS fc_txn_id,
            fc.rrn AS fc_rrn,
            fc.stan AS fc_stan,
            fc.account_masked AS fc_account_masked,
            fc.dr AS fc_dr,
            fc.cr AS fc_cr,
            fc.currency AS fc_currency,
            fc.status AS fc_status,
            fc.description AS fc_description,
            fc.uploaded_by AS fc_uploaded_by

        FROM fully_matched f
        LEFT JOIN atm_one a ON TRIM(CAST(a.rrn AS VARCHAR)) = f.rrn
        LEFT JOIN switch_one s ON TRIM(CAST(s.rrn AS VARCHAR)) = f.rrn
        LEFT JOIN flex_one fc ON TRIM(CAST(fc.rrn AS VARCHAR)) = f.rrn
        ORDER BY f.rrn
        LIMIT :limit OFFSET :offset;
        """

        # Use connection from Session and read SQL into Pandas DataFrame
        with db.connection() as conn:
            df = pd.read_sql(text(query), conn, params={"offset": offset, "limit": limit})
        # Replace NaN with None for JSON serialization
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict(orient="records")

    @staticmethod
    def getAtmTransactionsNotMatchingDetails(db: Session, offset: int = 0, limit: int = 100, type: int = 0):
        """
        Get RRN records that are present only in one table (not in other 2 tables)
        Returns paginated results with unified columns for ATM, SWITCH, and FLEXCUBE.
        """
        query = f"""
        SELECT * FROM (
            -- ATM only
            SELECT 'ATM' AS source_table,
                a.rrn::varchar,
                a.id::bigint,
                a.datetime::timestamp,
                a.terminalid::varchar,
                a.location::varchar,
                a.atmindex::int,
                a.pan_masked::varchar,
                a.account_masked::varchar,
                a.transactiontype::varchar,
                a.amount::numeric,
                a.currency::varchar,
                a.stan::varchar,
                a.auth::varchar,
                a.responsecode::varchar,
                a.responsedesc::varchar,
                a.uploaded_by::bigint
            FROM atm_transactions a
            LEFT JOIN switch_transactions s ON TRIM(CAST(a.rrn AS VARCHAR)) = TRIM(CAST(s.rrn AS VARCHAR))
            LEFT JOIN flexcube_transactions fc ON TRIM(CAST(a.rrn AS VARCHAR)) = TRIM(CAST(fc.rrn AS VARCHAR))
            WHERE s.rrn IS NULL AND fc.rrn IS NULL

            UNION ALL

            -- SWITCH only
            SELECT 'SWITCH' AS source_table,
                s.rrn::varchar,
                s.id::bigint,
                s.datetime::timestamp,
                s.terminalid::varchar,
                NULL::varchar AS location,
                NULL::int AS atmindex,
                s.pan_masked::varchar,
                NULL::varchar AS account_masked,
                NULL::varchar AS transactiontype,
                s.amountminor::numeric AS amount,
                s.currency::varchar,
                s.stan::varchar,
                NULL::varchar AS auth,
                NULL::varchar AS responsecode,
                NULL::varchar AS responsedesc,
                s.uploaded_by::bigint
            FROM switch_transactions s
            LEFT JOIN atm_transactions a ON TRIM(CAST(s.rrn AS VARCHAR)) = TRIM(CAST(a.rrn AS VARCHAR))
            LEFT JOIN flexcube_transactions fc ON TRIM(CAST(s.rrn AS VARCHAR)) = TRIM(CAST(fc.rrn AS VARCHAR))
            WHERE a.rrn IS NULL AND fc.rrn IS NULL

            UNION ALL

            -- FLEXCUBE only
            SELECT 'FLEXCUBE' AS source_table,
                fc.rrn::varchar,
                fc.id::bigint,
                fc.posted_datetime::timestamp,
                NULL::varchar AS terminalid,
                NULL::varchar AS location,
                NULL::int AS atmindex,
                NULL::varchar AS pan_masked,
                fc.account_masked::varchar,
                NULL::varchar AS transactiontype,
                (fc.dr + fc.cr)::numeric AS amount,
                fc.currency::varchar,
                fc.stan::varchar,
                NULL::varchar AS auth,
                NULL::varchar AS responsecode,
                NULL::varchar AS responsedesc,
                fc.uploaded_by::bigint
            FROM flexcube_transactions fc
            LEFT JOIN atm_transactions a ON TRIM(CAST(fc.rrn AS VARCHAR)) = TRIM(CAST(a.rrn AS VARCHAR))
            LEFT JOIN switch_transactions s ON TRIM(CAST(fc.rrn AS VARCHAR)) = TRIM(CAST(s.rrn AS VARCHAR))
            WHERE a.rrn IS NULL AND s.rrn IS NULL
        ) t
        ORDER BY rrn
        LIMIT :limit OFFSET :offset;
        """

        # Use connection from Session and read SQL into Pandas DataFrame
        with db.connection() as conn:
            df = pd.read_sql(text(query), conn, params={"offset": offset, "limit": limit})
        
        # Replace NaN/inf with None for JSON serialization
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict(orient="records")
    

    @staticmethod
    def getAtmTransactionsPartiallyMatchingDetails(db: Session, offset: int = 0, limit: int = 100):
        """
        Get ATM RRN records that are partially matched (present in ATM and at least one other table,
        but not in all three tables). Returns paginated results.
        """
        query = f"""
        WITH combined_rrn AS (
            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'ATM' AS source
            FROM atm_transactions
            WHERE rrn IS NOT NULL

            UNION ALL

            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'SWITCH' AS source
            FROM switch_transactions
            WHERE rrn IS NOT NULL

            UNION ALL

            SELECT TRIM(CAST(rrn AS VARCHAR)) AS rrn, 'FLEXCUBE' AS source
            FROM flexcube_transactions
            WHERE rrn IS NOT NULL
        ),
        rrn_summary AS (
            SELECT rrn,
                COUNT(DISTINCT source) AS match_count,
                BOOL_OR(source = 'ATM') AS in_atm,
                BOOL_OR(source = 'SWITCH') AS in_switch,
                BOOL_OR(source = 'FLEXCUBE') AS in_flexcube
            FROM combined_rrn
            GROUP BY rrn
        ),
        atm_partial AS (
            SELECT rrn
            FROM rrn_summary
            WHERE in_atm = TRUE
            AND match_count < 3  -- partially matched
        )
        SELECT a.*
        FROM atm_transactions a
        INNER JOIN atm_partial ap ON TRIM(CAST(a.rrn AS VARCHAR)) = ap.rrn
        ORDER BY a.rrn
        LIMIT :limit OFFSET :offset;
        """

        # Execute SQL and load into DataFrame
        with db.connection() as conn:
            df = pd.read_sql(text(query), conn, params={"offset": offset, "limit": limit})

        # Replace NaN/inf with None for JSON serialization
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict(orient="records")


    
    
    
    
