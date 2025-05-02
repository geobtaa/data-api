#!/usr/bin/env python3
"""
Test script for gazetteer API endpoints.

This script provides a comprehensive test suite for the gazetteer API endpoints.
It tests multiple gazetteer sources (GeoNames, Who's on First, BTAA) and provides
detailed output of the test results. The script can be configured to test different
environments by specifying a custom base URL.

Usage:
    python scripts/test_gazetteer_api.py [--base-url URL]
"""

import argparse
import json

import requests


def print_json(data):
    """
    Print JSON data in a pretty format.

    Args:
        data: JSON data to print
    """
    print(json.dumps(data, indent=2))


def test_endpoints(base_url="http://localhost:8000/api/v1"):
    """
    Test all gazetteer API endpoints.

    This function tests the following endpoints:
    1. List all gazetteers
    2. Search GeoNames
    3. Search Who's on First
    4. Get WOF details
    5. Search BTAA
    6. Search all gazetteers

    Args:
        base_url: Base URL for the API (default: http://localhost:8000/api/v1)
    """
    print("\n" + "=" * 80)
    print(f"Testing gazetteer API endpoints at {base_url}")
    print("=" * 80)

    # Test 1: List all gazetteers
    print("\nTest 1: List all gazetteers")
    print("-" * 80)
    response = requests.get(f"{base_url}/gazetteers")
    if response.status_code == 200:
        data = response.json()
        print(f"Success! Found {len(data.get('data', []))} gazetteers")
        print(f"Total records: {data.get('meta', {}).get('total_records', 0)}")
    else:
        print(f"Error: {response.status_code}, {response.text}")

    # Test 2: Search GeoNames
    print("\nTest 2: Search GeoNames")
    print("-" * 80)
    response = requests.get(f"{base_url}/gazetteers/geonames", params={"q": "london", "limit": 5})
    if response.status_code == 200:
        data = response.json()
        results = data.get("data", [])
        total_count = data.get("meta", {}).get("total_count", 0)
        print(f"Success! Found {len(results)} results out of {total_count}")
        if results:
            print("\nSample result:")
            print_json(results[0])
    else:
        print(f"Error: {response.status_code}, {response.text}")

    # Test 3: Search Who's on First
    print("\nTest 3: Search Who's on First")
    print("-" * 80)
    response = requests.get(f"{base_url}/gazetteers/wof", params={"q": "new york", "limit": 5})
    if response.status_code == 200:
        data = response.json()
        results = data.get("data", [])
        total_count = data.get("meta", {}).get("total_count", 0)
        print(f"Success! Found {len(results)} results out of {total_count}")
        if results:
            print("\nSample result:")
            print_json(results[0])
    else:
        print(f"Error: {response.status_code}, {response.text}")

    # Test 4: Get WOF details
    print("\nTest 4: Get WOF details (if previous test returned results)")
    print("-" * 80)
    if results and len(results) > 0:
        wof_id = results[0]["id"]
        response = requests.get(f"{base_url}/gazetteers/wof/{wof_id}")
        if response.status_code == 200:
            data = response.json()
            print(f"Success! Got details for WOF ID {wof_id}")
            print("\nSample attributes:")
            print(f"Name: {data.get('attributes', {}).get('spr', {}).get('name')}")
            print(f"Placetype: {data.get('attributes', {}).get('spr', {}).get('placetype')}")

            # Print counts of related data
            ancestors = data.get("attributes", {}).get("ancestors", [])
            names = data.get("attributes", {}).get("names", [])
            concordances = data.get("attributes", {}).get("concordances", [])

            print(f"Ancestors: {len(ancestors)}")
            print(f"Names: {len(names)}")
            print(f"Concordances: {len(concordances)}")
        else:
            print(f"Error: {response.status_code}, {response.text}")
    else:
        print("Skipping test 4 as no WOF results were returned in test 3")

    # Test 5: Search BTAA
    print("\nTest 5: Search BTAA")
    print("-" * 80)
    response = requests.get(f"{base_url}/gazetteers/btaa", params={"q": "minnesota", "limit": 5})
    if response.status_code == 200:
        data = response.json()
        results = data.get("data", [])
        total_count = data.get("meta", {}).get("total_count", 0)
        print(f"Success! Found {len(results)} results out of {total_count}")
        if results:
            print("\nSample result:")
            print_json(results[0])
    else:
        print(f"Error: {response.status_code}, {response.text}")

    # Test 6: Search all gazetteers
    print("\nTest 6: Search all gazetteers")
    print("-" * 80)
    response = requests.get(f"{base_url}/gazetteers/search", params={"q": "chicago", "limit": 5})
    if response.status_code == 200:
        data = response.json()
        results = data.get("data", [])
        total_count = data.get("meta", {}).get("total_count", 0)
        print(f"Success! Found {len(results)} results out of {total_count}")
        if results:
            # Group results by source
            by_source = {}
            for result in results:
                source = result.get("source", "unknown")
                if source not in by_source:
                    by_source[source] = 0
                by_source[source] += 1

            print("\nResults by source:")
            for source, count in by_source.items():
                print(f"- {source}: {count}")

            print("\nSample result:")
            print_json(results[0])
    else:
        print(f"Error: {response.status_code}, {response.text}")

    print("\n" + "=" * 80)
    print("Tests completed!")
    print("=" * 80)


def main():
    """
    Parse command line arguments and run the API tests.

    This function:
    1. Sets up the argument parser
    2. Parses command line arguments
    3. Executes the API tests with the specified base URL
    """
    parser = argparse.ArgumentParser(description="Test gazetteer API endpoints")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000/api/v1",
        help="Base URL for the API (default: http://localhost:8000/api/v1)",
    )

    args = parser.parse_args()
    test_endpoints(args.base_url)


if __name__ == "__main__":
    main()
