# app/config/column_patterns.py

COLUMN_PATTERNS = {
    # "terminal_id": {
    #     "patterns": [
    #         r"^[A-Z0-9]{6,10}$"        # Example: IZY00055
    #     ],
    #     "standard_name": "terminal_id",
    #     "priority": 4,
    # },

    "atm_index": {
        "patterns": [
            r"^\d{1,3}$"               # Example: 2
        ],
        "standard_name": "atm_index",
        "priority": 1,
    },

    # "txn_amount": {
    #     "patterns": [
    #         r"^\d+(\.\d{1,2})?$"        # Example: 500
    #     ],
    #     "standard_name": "txn_amount",
    #     "priority": 7,
    # },

    # "rrn": {
    #     "patterns": [
    #         r"^\d{12}$",                # Example: 251201699076
    #         r"^\d{3}[A-Z]{3,4}\d{6,10}$",
    #         r"^\d{10,16}$",
    #         r"^RRN\s?\d{10,}$"
    #     ],
    #     "standard_name": "rrn",
    #     "priority": 6
    # },

    # "stan": {
    #     "patterns": [
    #         r"^\d{6}$",            # Example: 932822
    #         r"^\d{6,8}$"           # fallback: 6-8 digit STAN
    #     ],
    #     "standard_name": "stan",
    #     "priority": 6,
    # },

    "mti": {
        "patterns": [
            r"^\d{4}$",            # 0200, 0210, 0420 etc.
            r"^(001|002|004)\d{3}$"   # common MTI prefixes
        ],
        "standard_name": "mti",
        "priority": 5,
    },



}
