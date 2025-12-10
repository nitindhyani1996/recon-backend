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
