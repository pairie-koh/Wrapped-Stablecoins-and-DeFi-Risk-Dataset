"""
Arbitrum Bridge Status Data Fetcher (Clean Version)
Fetches all incidents between 01-01-2020 and 08-02-2025
Compatible with Statuspage.io API v2
"""

import requests
import json
import csv
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import os
import sys


class ArbitrumStatusFetcher:
    """Fetches and processes Arbitrum status page incidents"""
    
    def __init__(self):
        self.base_url = "https://status.arbitrum.io/api/v2"
        self.start_date = datetime(2020, 1, 1, 0, 0, 0)
        self.end_date = datetime(2025, 8, 2, 23, 59, 59)
        self.session = requests.Session()
        # Update headers to mimic a browser request
        self.session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://status.arbitrum.io/',
            'Origin': 'https://status.arbitrum.io'
        })
    
    def test_connection(self) -> bool:
        """
        Test if the API endpoint is accessible
        
        Returns:
            True if connection successful, False otherwise
        """
        print("Testing API connection...")
        test_urls = [
            f"{self.base_url}/incidents.json",
            "https://status.arbitrum.io/api/v2/status.json",
            "https://status.arbitrum.io/api/v2/summary.json"
        ]
        
        for url in test_urls:
            try:
                print(f"  Testing: {url}")
                response = self.session.get(url, timeout=10)
                print(f"    Status Code: {response.status_code}")
                print(f"    Content-Type: {response.headers.get('content-type', 'Not specified')}")
                
                if response.status_code == 200:
                    # Try to parse as JSON
                    try:
                        data = response.json()
                        print(f"    Response type: JSON")
                        print(f"    Keys in response: {list(data.keys())[:5] if isinstance(data, dict) else 'List response'}")
                        return True
                    except json.JSONDecodeError:
                        print(f"    Response type: Not JSON (possibly HTML)")
                        print(f"    First 200 chars: {response.text[:200]}")
                else:
                    print(f"    Error: HTTP {response.status_code}")
                    
            except requests.exceptions.Timeout:
                print(f"    Error: Request timeout")
            except requests.exceptions.ConnectionError:
                print(f"    Error: Connection failed")
            except Exception as e:
                print(f"    Error: {str(e)}")
        
        return False
    
    def fetch_incidents_page(self, page: int = 1, per_page: int = 100) -> Dict[str, Any]:
        """
        Fetch a single page of incidents with better error handling
        
        Args:
            page: Page number (starts at 1)
            per_page: Number of items per page (max 100)
            
        Returns:
            Dictionary containing incidents data
        """
        url = f"{self.base_url}/incidents.json"
        params = {
            'page': page,
            'per_page': per_page
        }
        
        try:
            response = self.session.get(url, params=params, timeout=15)
            
            # Debug information
            if page == 1:
                print(f"  URL: {response.url}")
                print(f"  Status Code: {response.status_code}")
                print(f"  Content-Type: {response.headers.get('content-type', 'Not specified')}")
            
            response.raise_for_status()
            
            # Check if response is JSON
            content_type = response.headers.get('content-type', '')
            if 'application/json' not in content_type:
                print(f"Warning: Expected JSON but got {content_type}")
                print(f"Response preview: {response.text[:500]}")
                
            # Try to parse JSON
            try:
                data = response.json()
                return data
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                print(f"Response text: {response.text[:500]}")
                # Return empty structure to continue
                return {'incidents': []}
                
        except requests.exceptions.Timeout:
            print(f"Error: Request timeout for page {page}")
            raise
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error fetching page {page}: {e}")
            print(f"Response text: {response.text[:500] if response else 'No response'}")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            raise
    
    def fetch_all_incidents(self) -> List[Dict[str, Any]]:
        """
        Fetch all incidents with automatic pagination and robust error handling
        
        Returns:
            List of all incidents
        """
        all_incidents = []
        page = 1
        has_more = True
        max_pages = 50  # Safety limit
        
        print("\nFetching incidents from Arbitrum status page...")
        
        while has_more and page <= max_pages:
            try:
                print(f"  Fetching page {page}...")
                data = self.fetch_incidents_page(page)
                
                # Handle both dict and list responses
                if isinstance(data, dict):
                    incidents = data.get('incidents', [])
                elif isinstance(data, list):
                    incidents = data
                else:
                    print(f"  Unexpected data type: {type(data)}")
                    incidents = []
                
                if incidents and len(incidents) > 0:
                    all_incidents.extend(incidents)
                    print(f"  Page {page}: {len(incidents)} incidents found")
                    
                    # Check if there are more pages
                    if len(incidents) < 100:
                        has_more = False
                    else:
                        page += 1
                else:
                    print(f"  Page {page}: No more incidents")
                    has_more = False
                
                # Rate limiting
                if has_more:
                    time.sleep(0.5)
                    
            except Exception as e:
                print(f"  Failed to fetch page {page}: {e}")
                # Try to continue with next page
                page += 1
                if page > 3:  # Stop after 3 consecutive failures
                    has_more = False
        
        print(f"\nTotal incidents fetched: {len(all_incidents)}")
        return all_incidents
    
    def filter_incidents_by_date(self, incidents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter incidents by date range
        
        Args:
            incidents: List of incident dictionaries
            
        Returns:
            Filtered list of incidents
        """
        filtered = []
        for incident in incidents:
            try:
                created_at_str = incident.get('created_at', '')
                if created_at_str:
                    # Handle different date formats
                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    if self.start_date <= created_at <= self.end_date:
                        filtered.append(incident)
            except (ValueError, AttributeError) as e:
                print(f"Warning: Could not parse date for incident {incident.get('id', 'unknown')}: {e}")
                continue
        return filtered
    
    def is_bridge_related(self, incident: Dict[str, Any]) -> bool:
        """
        Check if incident is bridge-related based on keywords
        
        Args:
            incident: Incident dictionary
            
        Returns:
            True if bridge-related, False otherwise
        """
        bridge_keywords = [
            'bridge', 'deposit', 'withdrawal', 'cross-chain', 'crosschain',
            'l1', 'l2', 'mainnet', 'ethereum', 'sequencer', 'batch',
            'validator', 'congestion', 'delay', 'stuck', 'pending',
            'outage', 'degraded', 'downtime', 'slow', 'issue', 'maintenance'
        ]
        
        # Build search text from incident fields
        search_text = f"{incident.get('name', '')} {incident.get('impact', '')} {incident.get('status', '')}".lower()
        
        # Check incident updates for bridge-related content
        updates_text = ""
        if incident.get('incident_updates'):
            updates_text = " ".join([
                update.get('body', '') 
                for update in incident.get('incident_updates', [])
            ]).lower()
        
        full_text = f"{search_text} {updates_text}"
        
        return any(keyword in full_text for keyword in bridge_keywords)
    
    def calculate_duration(self, incident: Dict[str, Any]) -> Optional[int]:
        """
        Calculate incident duration in minutes
        
        Args:
            incident: Incident dictionary
            
        Returns:
            Duration in minutes or None if ongoing
        """
        if not incident.get('resolved_at'):
            return None
        
        try:
            created_at = datetime.fromisoformat(incident['created_at'].replace('Z', '+00:00'))
            resolved_at = datetime.fromisoformat(incident['resolved_at'].replace('Z', '+00:00'))
            duration = resolved_at - created_at
            return int(duration.total_seconds() / 60)
        except (ValueError, KeyError) as e:
            print(f"Warning: Could not calculate duration for incident {incident.get('id', 'unknown')}: {e}")
            return None
    
    def process_incident(self, incident: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process and structure the incident data
        
        Args:
            incident: Raw incident dictionary
            
        Returns:
            Structured incident data
        """
        return {
            'id': incident.get('id'),
            'name': incident.get('name', 'Unknown'),
            'status': incident.get('status', 'unknown'),
            'impact': incident.get('impact', 'none'),
            'created_at': incident.get('created_at'),
            'updated_at': incident.get('updated_at'),
            'resolved_at': incident.get('resolved_at'),
            'shortlink': incident.get('shortlink'),
            'scheduled_for': incident.get('scheduled_for'),
            'scheduled_until': incident.get('scheduled_until'),
            'duration_minutes': self.calculate_duration(incident),
            'components': incident.get('components', []),
            'updates': [
                {
                    'id': update.get('id'),
                    'status': update.get('status'),
                    'body': update.get('body'),
                    'created_at': update.get('created_at'),
                    'updated_at': update.get('updated_at')
                }
                for update in incident.get('incident_updates', [])
            ]
        }
    
    def generate_statistics(self, incidents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate statistics from incidents
        
        Args:
            incidents: List of processed incidents
            
        Returns:
            Statistics dictionary
        """
        stats = {
            'total_incidents': len(incidents),
            'by_impact': {
                'none': 0,
                'minor': 0,
                'major': 0,
                'critical': 0
            },
            'by_status': {},
            'total_downtime_minutes': 0,
            'average_resolution_time_minutes': 0,
            'incidents_by_month': {},
            'bridge_related': 0
        }
        
        resolved_count = 0
        total_resolution_time = 0
        
        for incident in incidents:
            # Count by impact
            impact = incident.get('impact', 'none')
            if impact in stats['by_impact']:
                stats['by_impact'][impact] += 1
            
            # Count by status
            status = incident.get('status', 'unknown')
            stats['by_status'][status] = stats['by_status'].get(status, 0) + 1
            
            # Calculate total downtime
            duration = incident.get('duration_minutes')
            if duration is not None:
                stats['total_downtime_minutes'] += duration
                total_resolution_time += duration
                resolved_count += 1
            
            # Count by month
            if incident.get('created_at'):
                try:
                    created_at = datetime.fromisoformat(incident['created_at'].replace('Z', '+00:00'))
                    month_key = created_at.strftime('%Y-%m')
                    stats['incidents_by_month'][month_key] = stats['incidents_by_month'].get(month_key, 0) + 1
                except:
                    pass
            
            # Check if bridge-related
            if self.is_bridge_related(incident):
                stats['bridge_related'] += 1
        
        # Calculate average resolution time
        if resolved_count > 0:
            stats['average_resolution_time_minutes'] = round(total_resolution_time / resolved_count)
        
        return stats
    
    def save_results_json(self, results: Dict[str, Any], filename: Optional[str] = None):
        """
        Save results to JSON file
        
        Args:
            results: Results dictionary to save
            filename: Optional custom filename
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"arbitrum_incidents_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"\nResults saved to: {filename}")
        except Exception as e:
            print(f"Error saving JSON results: {e}")
    
    def export_to_csv(self, incidents: List[Dict[str, Any]], filename: Optional[str] = None):
        """
        Export incidents to CSV format
        
        Args:
            incidents: List of incidents
            filename: Optional custom filename
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"arbitrum_incidents_{timestamp}.csv"
        
        headers = [
            'ID', 'Name', 'Status', 'Impact', 'Created At', 
            'Resolved At', 'Duration (minutes)', 'Bridge Related', 'Link'
        ]
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                
                for incident in incidents:
                    row = [
                        incident.get('id'),
                        incident.get('name'),
                        incident.get('status'),
                        incident.get('impact'),
                        incident.get('created_at'),
                        incident.get('resolved_at', 'N/A'),
                        incident.get('duration_minutes', 'Ongoing'),
                        'Yes' if self.is_bridge_related(incident) else 'No',
                        incident.get('shortlink', '')
                    ]
                    writer.writerow(row)
            
            print(f"CSV file saved to: {filename}")
        except Exception as e:
            print(f"Error saving CSV file: {e}")
    
    def run(self) -> Dict[str, Any]:
        """
        Main execution function with improved error handling
        
        Returns:
            Complete results with incidents and statistics
        """
        print("=" * 60)
        print("Arbitrum Bridge Status Data Fetcher")
        print(f"Date Range: {self.start_date.isoformat()} to {self.end_date.isoformat()}")
        print("=" * 60)
        
        try:
            # Test connection first
            if not self.test_connection():
                print("\nWarning: Could not verify API connection")
                print("Attempting to fetch data anyway...\n")
            
            # Fetch all incidents
            all_incidents = self.fetch_all_incidents()
            
            if not all_incidents:
                print("\nNo incidents were fetched. Possible causes:")
                print("  1. The API endpoint might have changed")
                print("  2. There might be no incidents in the system")
                print("  3. The API might require authentication")
                print("\nCreating empty result files...")
            
            # Filter by date range
            filtered_incidents = self.filter_incidents_by_date(all_incidents)
            print(f"\nIncidents within date range: {len(filtered_incidents)}")
            
            # Process incidents
            processed_incidents = [
                self.process_incident(incident) 
                for incident in filtered_incidents
            ]
            
            # Filter bridge-related incidents
            bridge_incidents = [
                incident for incident in processed_incidents 
                if self.is_bridge_related(incident)
            ]
            print(f"Bridge-related incidents: {len(bridge_incidents)}")
            
            # Generate statistics
            stats = self.generate_statistics(processed_incidents)
            
            # Prepare final results
            results = {
                'metadata': {
                    'fetched_at': datetime.now().isoformat(),
                    'date_range': {
                        'start': self.start_date.isoformat(),
                        'end': self.end_date.isoformat()
                    },
                    'source': 'https://status.arbitrum.io/api/v2/incidents.json',
                    'total_fetched': len(all_incidents),
                    'filtered_count': len(filtered_incidents)
                },
                'statistics': stats,
                'all_incidents': processed_incidents,
                'bridge_related_incidents': bridge_incidents
            }
            
            # Save results
            self.save_results_json(results)
            self.export_to_csv(processed_incidents)
            
            # Display summary
            self.print_summary(stats)
            
            return results
            
        except Exception as e:
            print(f"\nError in main execution: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def print_summary(self, stats: Dict[str, Any]):
        """
        Print a summary of the statistics
        
        Args:
            stats: Statistics dictionary
        """
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Total Incidents: {stats['total_incidents']}")
        print(f"Bridge-Related: {stats['bridge_related']}")
        print(f"Total Downtime: {stats['total_downtime_minutes']:,} minutes")
        print(f"Average Resolution Time: {stats['average_resolution_time_minutes']} minutes")
        
        if stats['total_incidents'] > 0:
            print("\nImpact Breakdown:")
            for impact, count in stats['by_impact'].items():
                if count > 0:
                    print(f"  {impact.capitalize()}: {count}")
            
            print("\nStatus Breakdown:")
            for status, count in sorted(stats['by_status'].items()):
                if count > 0:
                    print(f"  {status.capitalize()}: {count}")
            
            # Show top 5 months with most incidents
            if stats['incidents_by_month']:
                print("\nTop 5 Months by Incident Count:")
                sorted_months = sorted(
                    stats['incidents_by_month'].items(), 
                    key=lambda x: x[1], 
                    reverse=True
                )[:5]
                for month, count in sorted_months:
                    print(f"  {month}: {count} incidents")


def main():
    """Main entry point"""
    print("Starting Arbitrum Status Fetcher...")
    print(f"Python version: {sys.version}")
    print(f"Requests version: {requests.__version__}")
    
    fetcher = ArbitrumStatusFetcher()
    
    try:
        results = fetcher.run()
        
        print("\n" + "=" * 60)
        print("Fetch completed!")
        print("Check the generated JSON and CSV files for detailed data.")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user")
        return 1
    except Exception as e:
        print(f"\nFailed to fetch incidents: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())