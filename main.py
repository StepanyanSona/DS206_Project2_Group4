"""
Main entry point for the dimensional data pipeline.
Parses command-line arguments and executes the dimensional data flow.
"""
import argparse
import sys
from datetime import datetime
from pipeline_dimensional_data.flow import DimensionalDataFlow


def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Execute dimensional data pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--start_date',
        type=str,
        required=True,
        help='Start date for fact table ingestion (format: YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--end_date',
        type=str,
        required=True,
        help='End date for fact table ingestion (format: YYYY-MM-DD)'
    )
    
    return parser.parse_args()


def validate_date(date_string: str) -> bool:
    """
    Validate date string format.
    
    Args:
        date_string: Date string to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def main():
    """Main function to execute the pipeline."""
    # Parse arguments
    args = parse_arguments()
    
    # Validate date formats
    if not validate_date(args.start_date):
        print(f"Error: Invalid start_date format: {args.start_date}. Expected format: YYYY-MM-DD")
        sys.exit(1)
    
    if not validate_date(args.end_date):
        print(f"Error: Invalid end_date format: {args.end_date}. Expected format: YYYY-MM-DD")
        sys.exit(1)
    
    # Validate date range
    start_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    if start_dt > end_dt:
        print(f"Error: start_date ({args.start_date}) must be before or equal to end_date ({args.end_date})")
        sys.exit(1)
    
    # Create and execute the flow
    try:
        flow = DimensionalDataFlow()
        result = flow.exec(start_date=args.start_date, end_date=args.end_date)
        
        if result.get('success', False):
            print(f"Pipeline executed successfully! Execution ID: {result.get('execution_id')}")
            sys.exit(0)
        else:
            print(f"Pipeline execution failed! Execution ID: {result.get('execution_id')}")
            print(f"Error: {result.get('error', 'Unknown error')}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
