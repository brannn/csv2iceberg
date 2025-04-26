"""
Dry run example for SQL Batcher.

This example demonstrates how to use SQL Batcher's dry run mode to collect
and analyze SQL statements without actually executing them.
"""
import logging

from sql_batcher import SQLBatcher
from sql_batcher.query_collector import ListQueryCollector


# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def main():
    """Run the dry run example."""
    # Create a query collector
    collector = ListQueryCollector()
    
    # Create a batcher in dry run mode with a 50KB size limit
    batcher = SQLBatcher(max_bytes=50_000, dry_run=True)
    
    # Define a large number of statements
    statements = []
    for i in range(1, 1001):
        statements.append(
            f"INSERT INTO logs VALUES ({i}, 'Log message {i}', CURRENT_TIMESTAMP)"
        )
    
    print(f"Generated {len(statements)} INSERT statements")
    
    # Define metadata for the queries
    metadata = {
        "operation": "insert",
        "table": "logs",
        "description": "Batch insert of log entries"
    }
    
    # Process statements without executing them
    total_processed = batcher.process_statements(
        statements,
        lambda x: None,  # This won't be called in dry run mode
        query_collector=collector,
        metadata=metadata
    )
    
    print(f"Processed {total_processed} statements in dry run mode")
    
    # Get the collected queries
    collected_queries = collector.get_queries()
    print(f"Collected {len(collected_queries)} batches")
    
    # Analyze the batches
    print("\nBatch analysis:")
    for i, query_info in enumerate(collected_queries):
        query = query_info["query"]
        batch_metadata = query_info["metadata"]
        
        # Count statements in this batch (by counting semicolons)
        statement_count = query.count(";")
        
        # Get batch size in bytes
        batch_size = len(query.encode("utf-8"))
        
        print(f"Batch {i+1}:")
        print(f"  Size: {batch_size} bytes")
        print(f"  Statements: {statement_count}")
        print(f"  First statement: {query.split(';')[0]}")
        if batch_metadata:
            print(f"  Metadata: {batch_metadata}")
        print()


if __name__ == "__main__":
    main()