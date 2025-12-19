"""
Utility to format reconciliation matching data for ReconMatchingSummary TEXT columns

This formatter creates structured data matching frontend requirements:
- RRN
- Txn Type
- Terminal Id
- Date
- Amount
- Result

Supports multiple formats:
1. CSV format (for simple storage)
2. JSON format (for rich data)
3. Table format (for display)
"""

import json
from typing import List, Dict, Any, Optional
from datetime import datetime


class ReconDataFormatter:
    """Format matching data for ReconMatchingSummary TEXT columns"""
    
    @staticmethod
    def format_matched_data_csv(transactions: List[Dict[str, Any]]) -> str:
        """
        Format matched transactions as CSV string
        
        Format: RRN|TxnType|TerminalId|Date|Amount|Result
        
        Args:
            transactions: List of transaction dicts with keys:
                - rrn: str
                - txn_type: str (Withdrawal, Deposit, etc.)
                - terminal_id: str
                - date: datetime or str
                - amount: float or Decimal
                - result: str (MATCHED, PARTIAL, UNMATCHED)
                
        Returns:
            CSV string: "RRN1|type|term|date|amt|result;RRN2|..."
        """
        lines = []
        for txn in transactions:
            rrn = str(txn.get('rrn', ''))
            txn_type = str(txn.get('txn_type', 'Unknown'))
            terminal_id = str(txn.get('terminal_id', ''))
            
            # Format date
            date = txn.get('date')
            if isinstance(date, datetime):
                date_str = date.strftime('%Y-%m-%d %H:%M:%S')
            else:
                date_str = str(date) if date else ''
            
            # Format amount
            amount = txn.get('amount', 0)
            if amount is None or amount == "" or str(amount).strip() in ["None", ""]:
                amount_str = "0.00"
            else:
                try:
                    amount_str = f"{float(amount):.2f}"
                except (ValueError, TypeError):
                    amount_str = "0.00"
            
            # Result status
            result = str(txn.get('result', 'UNKNOWN'))
            
            # Join with pipe separator
            line = f"{rrn}|{txn_type}|{terminal_id}|{date_str}|{amount_str}|{result}"
            lines.append(line)
        
        # Join all lines with semicolon
        return ";".join(lines)
    
    @staticmethod
    def parse_matched_data_csv(csv_string: str) -> List[Dict[str, Any]]:
        """
        Parse CSV string back to list of transaction dicts
        
        Args:
            csv_string: "RRN1|type|term|date|amt|result;RRN2|..."
            
        Returns:
            List of transaction dictionaries
        """
        if not csv_string or csv_string.strip() == '':
            return []
        
        transactions = []
        lines = csv_string.split(';')
        
        for line in lines:
            if not line.strip():
                continue
                
            parts = line.split('|')
            if len(parts) != 6:
                continue  # Skip malformed lines
            
            txn = {
                'rrn': parts[0],
                'txn_type': parts[1],
                'terminal_id': parts[2],
                'date': parts[3],
                'amount': float(parts[4]) if parts[4] else 0.0,
                'result': parts[5]
            }
            transactions.append(txn)
        
        return transactions
    
    @staticmethod
    def format_matched_data_json(transactions: List[Dict[str, Any]]) -> str:
        """
        Format matched transactions as JSON string
        
        Args:
            transactions: List of transaction dicts
            
        Returns:
            JSON string with array of objects
        """
        formatted_data = []
        
        for txn in transactions:
            formatted_txn = {
                "rrn": str(txn.get('rrn', '')),
                "txn_type": str(txn.get('txn_type', 'Unknown')),
                "terminal_id": str(txn.get('terminal_id', '')),
                "date": txn.get('date').isoformat() if isinstance(txn.get('date'), datetime) else str(txn.get('date', '')),
                "amount": float(txn.get('amount', 0)),
                "result": str(txn.get('result', 'UNKNOWN'))
            }
            formatted_data.append(formatted_txn)
        
        return json.dumps(formatted_data, ensure_ascii=False)
    
    @staticmethod
    def parse_matched_data_json(json_string: str) -> List[Dict[str, Any]]:
        """
        Parse JSON string back to list of transaction dicts
        
        Args:
            json_string: JSON array string
            
        Returns:
            List of transaction dictionaries
        """
        if not json_string or json_string.strip() == '':
            return []
        
        try:
            return json.loads(json_string)
        except json.JSONDecodeError:
            return []
    
    @staticmethod
    def format_from_atm_transactions(atm_txns: List[Any], result: str = "MATCHED") -> str:
        """
        Format ATM transaction models into CSV string
        
        Args:
            atm_txns: List of ATMTransaction model instances
            result: Result status (MATCHED, PARTIAL, UNMATCHED)
            
        Returns:
            CSV formatted string
        """
        transactions = []
        
        for atm in atm_txns:
            txn = {
                'rrn': atm.rrn,
                'txn_type': atm.transactiontype or 'Unknown',
                'terminal_id': atm.terminalid or '',
                'date': atm.datetime,
                'amount': float(atm.amount) if atm.amount else 0.0,
                'result': result
            }
            transactions.append(txn)
        
        return ReconDataFormatter.format_matched_data_csv(transactions)
    
    @staticmethod
    def format_from_switch_transactions(switch_txns: List[Any], result: str = "MATCHED") -> str:
        """
        Format SWITCH transaction models into CSV string
        
        Args:
            switch_txns: List of SwitchTransaction model instances
            result: Result status (MATCHED, PARTIAL, UNMATCHED)
            
        Returns:
            CSV formatted string
        """
        transactions = []
        
        for switch in switch_txns:
            txn = {
                'rrn': switch.rrn,
                'txn_type': switch.direction or 'Unknown',  # INBOUND/OUTBOUND
                'terminal_id': switch.terminalid or '',
                'date': switch.datetime,
                'amount': float(switch.amountminor) if switch.amountminor else 0.0,
                'result': result
            }
            transactions.append(txn)
        
        return ReconDataFormatter.format_matched_data_csv(transactions)
    
    @staticmethod
    def format_from_flexcube_transactions(flexcube_txns: List[Any], result: str = "MATCHED") -> str:
        """
        Format FLEXCUBE transaction models into CSV string
        
        Args:
            flexcube_txns: List of FlexcubeTransaction model instances
            result: Result status (MATCHED, PARTIAL, UNMATCHED)
            
        Returns:
            CSV formatted string
        """
        transactions = []
        
        for flex in flexcube_txns:
            txn = {
                'rrn': str(flex.rrn),
                'txn_type': flex.status or 'Posted',
                'terminal_id': '',  # FLEXCUBE doesn't have terminal
                'date': flex.posted_datetime,
                'amount': float(flex.dr) if flex.dr else 0.0,
                'result': result
            }
            transactions.append(txn)
        
        return ReconDataFormatter.format_matched_data_csv(transactions)
    
    @staticmethod
    def format_table_display(transactions: List[Dict[str, Any]]) -> str:
        """
        Format transactions as readable table (for debugging/display)
        
        Args:
            transactions: List of transaction dicts
            
        Returns:
            Formatted table string
        """
        if not transactions:
            return "No transactions"
        
        header = f"{'RRN':<15} {'Txn Type':<20} {'Terminal ID':<12} {'Date':<20} {'Amount':<12} {'Result':<10}"
        separator = "-" * len(header)
        
        lines = [header, separator]
        
        for txn in transactions:
            line = (
                f"{txn.get('rrn', ''):<15} "
                f"{txn.get('txn_type', ''):<20} "
                f"{txn.get('terminal_id', ''):<12} "
                f"{str(txn.get('date', '')):<20} "
                f"{txn.get('amount', 0):>11.2f} "
                f"{txn.get('result', ''):<10}"
            )
            lines.append(line)
        
        return "\n".join(lines)
    
    @staticmethod
    def get_frontend_format(csv_string: str) -> List[Dict[str, str]]:
        """
        Convert CSV string to frontend-ready format
        
        Frontend expects:
        [
            {
                "RRN": "251218000001",
                "Txn Type": "Withdrawal",
                "Terminal Id": "IZY00055",
                "Date": "2025-12-18 10:30:00",
                "Amount": "10000.00",
                "Result": "MATCHED"
            },
            ...
        ]
        
        Args:
            csv_string: CSV formatted string from database
            
        Returns:
            List of dicts with exact frontend field names
        """
        transactions = ReconDataFormatter.parse_matched_data_csv(csv_string)
        
        frontend_data = []
        for txn in transactions:
            frontend_txn = {
                "RRN": txn['rrn'],
                "Txn Type": txn['txn_type'],
                "Terminal Id": txn['terminal_id'],
                "Date": txn['date'],
                "Amount": f"{txn['amount']:.2f}",
                "Result": txn['result']
            }
            frontend_data.append(frontend_txn)
        
        return frontend_data


# Example usage and testing
if __name__ == "__main__":
    print("=" * 80)
    print("Recon Data Formatter - Example Usage")
    print("=" * 80)
    
    # Sample data
    sample_transactions = [
        {
            'rrn': '251218000001',
            'txn_type': 'Withdrawal',
            'terminal_id': 'IZY00055',
            'date': datetime(2025, 12, 18, 10, 30, 0),
            'amount': 10000.00,
            'result': 'MATCHED'
        },
        {
            'rrn': '251218000002',
            'txn_type': 'Deposit',
            'terminal_id': 'IZY00012',
            'date': datetime(2025, 12, 18, 14, 45, 30),
            'amount': 5000.50,
            'result': 'MATCHED'
        },
        {
            'rrn': '251218000003',
            'txn_type': 'Balance Inquiry',
            'terminal_id': 'IZY00089',
            'date': datetime(2025, 12, 18, 16, 20, 15),
            'amount': 0.00,
            'result': 'PARTIAL'
        }
    ]
    
    # Format as CSV
    print("\n1. CSV Format (for database storage):")
    print("-" * 80)
    csv_data = ReconDataFormatter.format_matched_data_csv(sample_transactions)
    print(csv_data)
    print(f"\nSize: {len(csv_data)} bytes")
    
    # Parse CSV back
    print("\n2. Parse CSV back to list:")
    print("-" * 80)
    parsed = ReconDataFormatter.parse_matched_data_csv(csv_data)
    for txn in parsed:
        print(txn)
    
    # Format as JSON
    print("\n3. JSON Format (alternative storage):")
    print("-" * 80)
    json_data = ReconDataFormatter.format_matched_data_json(sample_transactions)
    print(json_data)
    print(f"\nSize: {len(json_data)} bytes")
    
    # Frontend format
    print("\n4. Frontend Format:")
    print("-" * 80)
    frontend_data = ReconDataFormatter.get_frontend_format(csv_data)
    for txn in frontend_data:
        print(txn)
    
    # Table display
    print("\n5. Table Display:")
    print("-" * 80)
    table = ReconDataFormatter.format_table_display(parsed)
    print(table)
    
    print("\n" + "=" * 80)
    print("✅ Formatter working correctly!")
    print("=" * 80)
