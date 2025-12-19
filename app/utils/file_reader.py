import csv
import io
import json
import os
import pandas as pd

async def read_file_by_extension(file):
    filename = file.filename
    extension = os.path.splitext(filename)[1].lower()

    # Read file content
    content = await file.read()

    # Convert based on extension
    if extension == ".csv":
        decoded_content = content.decode("utf-8", errors="ignore")
        reader = csv.reader(io.StringIO(decoded_content))
        rows = list(reader)

        columns = rows[0]
        data = [dict(zip(columns, row)) for row in rows[1:]]

    elif extension in [".xlsx", ".xls"]:
        df = pd.read_excel(io.BytesIO(content))
        columns = list(df.columns)
        data = df.to_dict(orient="records")

    elif extension == ".json":
        data = json.loads(content.decode("utf-8", errors="ignore"))
        columns = list(data[0].keys()) if data else []

    elif extension == ".txt":
        decoded_content = content.decode("utf-8", errors="ignore")
        lines = decoded_content.splitlines()
        columns = ["line"]
        data = [{"line": line} for line in lines]

    else:
        raise ValueError("Unsupported file extension")

    return {
        "filename": filename,
        "extension": extension,
        "columns": columns,
        "data": data
    }


def parse_datetime(date_string):
    """
    Parse various date/datetime formats from CSV files.
    Supports formats like:
    - 01/11/2025  11:58:00
    - 01/11/2025 11:58:00
    - 2025-11-01 11:58:00
    - 01-11-2025 11:58:00
    - 2025/11/01 11:58:00
    etc.
    """
    from datetime import datetime
    
    if not date_string or date_string == "" or str(date_string).strip() == "":
        return None
    
    # Clean up the string - remove extra spaces
    date_string = str(date_string).strip()
    
    # Common date format patterns to try
    formats = [
        "%d/%m/%Y %H:%M:%S",      # 01/11/2025  11:58:00 or 01/11/2025 11:58:00
        "%d/%m/%Y  %H:%M:%S",     # 01/11/2025  11:58:00 (double space)
        "%d/%m/%Y %I:%M:%S %p",   # 01/11/2025 11:58:00 AM
        "%Y-%m-%d %H:%M:%S",      # 2025-11-01 11:58:00
        "%d-%m-%Y %H:%M:%S",      # 01-11-2025 11:58:00
        "%Y/%m/%d %H:%M:%S",      # 2025/11/01 11:58:00
        "%d/%m/%Y",               # 01/11/2025 (date only)
        "%Y-%m-%d",               # 2025-11-01 (date only)
        "%d-%m-%Y",               # 01-11-2025 (date only)
        "%Y/%m/%d",               # 2025/11/01 (date only)
        "%d/%m/%Y %H:%M",         # 01/11/2025 11:58
        "%Y-%m-%d %H:%M",         # 2025-11-01 11:58
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    
    # If nothing worked, try pandas to_datetime as a fallback
    try:
        import pandas as pd
        parsed = pd.to_datetime(date_string, errors='coerce')
        if pd.notna(parsed):
            return parsed.to_pydatetime()
    except:
        pass
    
    # If all else fails, return None and log a warning
    print(f"Warning: Could not parse date string: '{date_string}'")
    return None
