import re
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple

class SmartColumnMapper:
    """
    Automatically detects column types by analyzing data patterns, 
    NOT by column names. Works with any file structure.
    """

    def __init__(self):
        # Pattern definitions for different column types
        self.column_patterns = {
            'transaction_id': {
                'patterns': [
                    r'^\d{6,15}$',             # Any numeric ID, 6 to 15 digits
                    r'^ATM\d{13,}$',           # ATM2024120300001
                    r'^CBS\d{13,}$',           # CBS20241203000001
                    r'^TXN\d{10,}$',           # TXN001234567890
                    r'^[A-Z]{2,4}\d{10,}$',    # ABC1234567890
                    r'^TID\d{10,}$',           # TID prefix
                ],
                'standard_name': 'transaction_id',
                'priority': 5
            },
            'host_ref_number': {
                'patterns': [
                    r'^\d{12}$',                # 241203091523 (12 digits)
                    r'^2\.41203E\+11$',         # Scientific notation from Excel
                    r'^\d{6}\d{6}$',            # YYMMDDHHMMSS format
                    r'^RRN\d{10,}$',            # RRN prefix
                ],
                'standard_name': 'Host_ref_number',
                'priority': 6
            },
            'account_number': {
                'patterns': [
                    r'^\d{12}$'
                ],
                'standard_name': 'account_number',
                'priority': 4
            },

            'host_ref_number': {
                'patterns': [
                    r'^\d{3}[A-Z]{3,4}\d{6,10}$',   # e.g., 001MBCN220540001
                    r'^\d{10,16}$'                  # fallback: numeric IDs only
                ],
                'standard_name': 'host_ref_number',
                'priority': 4
            },
            'card_number': {
                'patterns': [
                    r'^\d{4}\*{6,8}\d{4}$',    # Masked: 4532********1234
                    r'^\d{4}X{6,8}\d{4}$',     # Masked: 4532XXXXXXXX1234
                    r'^\d{16}$',               # Full card number
                    r'^\d{4}-\d{4}-\d{4}-\d{4}$',  # Formatted card
                ],
                'standard_name': 'card_number',
                'priority': 8
            },
            # 'txn_amount': {
            #     'patterns': [
            #         r'^\d+\.?\d{0,2}$',        # Numeric amounts: 5000, 5000.00
            #         r'^\d{1,10}$',             # Integer amounts
            #         r'^\d+\.\d+$',             # Decimal amounts
            #     ],
            #     'standard_name': 'txn_amount',
            #     'priority': 3
            # },
            'txn_date_time': {
                'patterns': [
                    # Combined date-time patterns
                    r'^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}$',  # 12/3/2024 9:15:23
                    r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$',         # 2024-12-03 09:15:23
                    r'^\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2}$',         # 03-12-2024 09:15:23
                    r'^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}$',         # 12/3/2024 9:15
                    r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',            # ISO format
                ],
                'standard_name': 'txn_date_time',
                'priority': 9
            },
            # 'atm_id': {
            #     'patterns': [
            #         r'^ATM\d{4,8}$',           # ATM1234, ATM12345678
            #         r'^[A-Z]{3}\d{4,8}$',      # BOM1234, HDC5678
            #         r'^\d{4,8}$',              # 1234, 12345678 (if other columns identified)
            #         r'^[A-Z0-9]{4,10}$',       # Alphanumeric ATM IDs
            #     ],
            #     'standard_name': 'atm_id',
            #     'priority': 2
            # },
            'currency_code': {
                'patterns': [
                    r'^[A-Z]{3}$',             # INR, USD, EUR
                    r'^\d{3}$',                # 356 (numeric currency codes)
                ],
                'standard_name': 'currency_code',
                'priority': 1
            },
            'response_code': {
                'patterns': [
                    r'^\d{2}$',                # 00, 51, etc.
                ],
                'standard_name': 'response_code',
                'priority': 1
            }
        }

    def detect_column_type(self, series: pd.Series, col_name: str = "") -> Tuple[Optional[str], float]:
        """
        Detect column type based on DATA PATTERNS, not column name.
        Returns: (column_type, confidence_score)
        """
        # Get non-null sample values (increased sample size)
        sample = series.dropna().astype(str).head(100)
        
        if len(sample) == 0:
            return None, 0.0
        
        # Check if all values are identical (might be currency code or flag)
        if len(sample.unique()) == 1:
            val = str(sample.iloc[0]).strip()
            if re.match(r'^[A-Z]{3}$', val):
                return 'currency_code', 1.0
        
        best_match = None
        best_score = 0.0
        
        for col_type, config in self.column_patterns.items():
            matches = 0
            total_checked = 0
            
            for value in sample:
                value = str(value).strip()
                if not value or value == 'nan':
                    continue
                    
                total_checked += 1
                for pattern in config['patterns']:
                    try:
                        if re.match(pattern, value, re.IGNORECASE):
                            matches += 1
                            break
                    except:
                        continue
            
            if total_checked == 0:
                continue
                
            score = matches / total_checked
            
            # Dynamic threshold based on column type
            if col_type == 'txn_date_time':
                min_threshold = 0.85
            elif col_type in ['transaction_id', 'host_ref_number', 'card_number']:
                min_threshold = 0.80
            elif col_type == 'currency_code':
                min_threshold = 0.95
            else:
                min_threshold = 0.70
            
            # Use priority to break ties
            if score >= min_threshold:
                priority_bonus = config.get('priority', 1) * 0.01
                adjusted_score = score + priority_bonus
                
                if adjusted_score > best_score:
                    best_score = score  # Return actual score, not adjusted
                    best_match = col_type
        
        return best_match, best_score

    def analyze_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze entire dataframe and map columns to standard names.
        Ignores original column names completely.
        """
        column_mapping = {}
        detection_report = []
        unmapped_columns = []
        
        for original_col in df.columns:
            col_type, confidence = self.detect_column_type(df[original_col], original_col)
            
            if col_type and confidence >= 0.7:
                standard_name = self.column_patterns[col_type]['standard_name']
                
                # Handle duplicate mappings
                if standard_name in column_mapping.values():
                    counter = 2
                    while f"{standard_name}_{counter}" in column_mapping.values():
                        counter += 1
                    standard_name = f"{standard_name}_{counter}"
                
                column_mapping[original_col] = standard_name
                
                detection_report.append({
                    'original_column': str(original_col),
                    'detected_type': col_type,
                    'mapped_to': standard_name,
                    'confidence': round(confidence * 100, 2),
                    'sample_values': df[original_col].dropna().head(3).tolist()
                })
            else:
                unmapped_columns.append(str(original_col))
        
        return {
            'column_mapping': column_mapping,
            'detection_report': detection_report,
            'unmapped_columns': unmapped_columns,
            'total_columns': len(df.columns),
            'mapped_columns': len(column_mapping)
        }

    def apply_mapping(self, df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Apply the detected column mapping to rename columns.
        """
        return df.rename(columns=column_mapping)

    def fix_scientific_notation(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fix Excel scientific notation issues.
        """
        fixed_df = df.copy()
        
        for col in fixed_df.columns:
            sample = fixed_df[col].astype(str).head(20)
            if any('E+' in str(val) or 'e+' in str(val) for val in sample):
                try:
                    fixed_df[col] = pd.to_numeric(fixed_df[col], errors='coerce').fillna(0).astype('int64').astype(str)
                    # Pad with zeros for specific lengths (e.g., 12 digits for RRN)
                    if fixed_df[col].str.len().mode()[0] == 12:
                        fixed_df[col] = fixed_df[col].str.zfill(12)
                except:
                    pass
        
        return fixed_df

    def process_uploaded_file(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Complete processing pipeline for uploaded files.
        Returns standardized column names and cleaned data.
        """
        # Step 1: Fix scientific notation
        df = self.fix_scientific_notation(df)
        
        # Step 2: Analyze and detect column types
        analysis = self.analyze_dataframe(df)
        
        # Step 3: Apply mapping
        mapped_df = self.apply_mapping(df, analysis['column_mapping'])
        
        # Step 4: Prepare result
        result = {
            'status': 'success',
            'original_columns': list(df.columns),
            'mapped_columns': list(mapped_df.columns),
            'column_mapping': analysis['column_mapping'],
            'detection_report': analysis['detection_report'],
            'unmapped_columns': analysis['unmapped_columns'],
            'dataframe': mapped_df,
            'data_preview': mapped_df.head(10).to_dict('records')
        }
        
        return result


# Example usage with different file structures
if __name__ == "__main__":
    # Test Case 1: Original column names are wrong
    test_data_1 = {
        "Column1": ["4532********1234", "5425********5678", "6011********9012"],
        "Col_A": ["12/3/2024 9:15:23", "12/3/2024 9:18:45", "12/3/2024 10:05:12"],
        "": ["ATM2024120300001", "ATM2024120300002", "ATM2024120300003"],
        "Value": ["5000", "10000", "2500"],
        "ID": ["ATM001", "ATM002", "ATM003"]
    }
    
    df1 = pd.DataFrame(test_data_1)
    mapper = SmartColumnMapper()
    result = mapper.process_uploaded_file(df1)
    
    print("=" * 80)
    print("SMART COLUMN DETECTION REPORT")
    print("=" * 80)
    print(f"Original columns: {result['original_columns']}")
    print(f"Mapped columns: {result['mapped_columns']}")
    print(f"\nDetection Details:")
    for detection in result['detection_report']:
        print(f"  • '{detection['original_column']}' → '{detection['mapped_to']}' "
              f"({detection['detected_type']}, {detection['confidence']}% confidence)")
    
    if result['unmapped_columns']:
        print(f"\nUnmapped columns: {result['unmapped_columns']}")
    
    print(f"\nMapped DataFrame Preview:")
    print(result['dataframe'].head())