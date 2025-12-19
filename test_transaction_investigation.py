#!/usr/bin/env python3
"""
Test script for Transaction Investigation API
"""

import requests
import json

BASE_URL = "http://localhost:8000/api/v1"

def print_response(title, response):
    """Pretty print API response"""
    print(f"\n{'='*80}")
    print(f"{title}")
    print(f"{'='*80}")
    print(f"Status Code: {response.status_code}")
    print(f"Response:")
    print(json.dumps(response.json(), indent=2))

def test_get_all_transaction_data(rrn):
    """Test GET /api/v1/transaction/{rrn}"""
    response = requests.get(f"{BASE_URL}/transaction/{rrn}")
    print_response(f"GET All Transaction Data by RRN: {rrn}", response)
    return response.json()

def test_get_atm_transaction(rrn):
    """Test GET /api/v1/atm/transaction/{rrn}"""
    response = requests.get(f"{BASE_URL}/atm/transaction/{rrn}")
    print_response(f"GET ATM Transaction by RRN: {rrn}", response)
    return response.json()

def test_get_switch_transaction(rrn):
    """Test GET /api/v1/switch/transaction/{rrn}"""
    response = requests.get(f"{BASE_URL}/switch/transaction/{rrn}")
    print_response(f"GET Switch Transaction by RRN: {rrn}", response)
    return response.json()

def test_get_cbs_transaction(rrn):
    """Test GET /api/v1/cbs/transaction/{rrn}"""
    response = requests.get(f"{BASE_URL}/cbs/transaction/{rrn}")
    print_response(f"GET CBS Transaction by RRN: {rrn}", response)
    return response.json()

def test_not_found(rrn):
    """Test with non-existent RRN"""
    response = requests.get(f"{BASE_URL}/transaction/{rrn}")
    print_response(f"GET Transaction (Not Found) by RRN: {rrn}", response)
    return response.json()

if __name__ == "__main__":
    print("🚀 Testing Transaction Investigation API")
    print(f"Base URL: {BASE_URL}")
    
    # Test with existing RRN
    test_rrn = "RRN0000001"
    
    # 1. Test getting all transaction data
    result = test_get_all_transaction_data(test_rrn)
    
    if result.get("success"):
        print(f"\n✅ Found transaction in {result['data']['total_sources']} source(s)")
        print(f"   Matching Status: {result['data']['matching_status']}")
        print(f"   Sources: {', '.join(result['data']['sources_found'])}")
    
    # 2. Test individual endpoints
    test_get_atm_transaction(test_rrn)
    test_get_switch_transaction(test_rrn)
    test_get_cbs_transaction(test_rrn)
    
    # 3. Test with non-existent RRN
    test_not_found("NONEXISTENT123")
    
    print(f"\n{'='*80}")
    print("✅ All tests completed!")
    print(f"{'='*80}\n")
