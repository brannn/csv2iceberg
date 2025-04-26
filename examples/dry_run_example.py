"""
Dry run example for SQL Batcher.

This example demonstrates how to use SQL Batcher's dry run mode to collect
and analyze SQL statements without actually executing them.
"""
from sql_batcher import SQLBatcher
from sql_batcher.query_collector import ListQueryCollector


def main():
    """Run the dry run example."""
    print("SQL Batcher - Dry Run Example")
    print("=============================")
    
    # Create a query collector to collect SQL statements
    collector = ListQueryCollector()
    
    # Generate some sample INSERT statements
    print("\nGenerating sample INSERT statements...")
    table_name = "products"
    statements = []
    
    # Generate statements for different product categories
    categories = ["Electronics", "Books", "Clothing", "Home", "Food"]
    
    for category in categories:
        # Add 20 products per category
        for i in range(1, 21):
            product_id = f"{category[0:3].upper()}-{i:03d}"
            product_name = f"{category} Item {i}"
            price = 10.99 + (i * 2.50)
            stock = i * 5
            
            # Create the SQL statement
            statements.append(
                f"INSERT INTO {table_name} (product_id, name, category, price, stock) "
                f"VALUES ('{product_id}', '{product_name}', '{category}', {price:.2f}, {stock})"
            )
    
    print(f"Generated {len(statements)} INSERT statements")
    
    # Create a batcher in dry run mode
    batcher = SQLBatcher(max_bytes=5_000, dry_run=True)
    
    # Process the statements in dry run mode
    print("\nProcessing statements in dry run mode...")
    
    # We don't need a real execute function in dry run mode,
    # but we still need to provide a callback
    def dummy_execute(sql):
        # This function will not be called in dry run mode
        pass
    
    total_processed = batcher.process_statements(
        statements=statements,
        execute_callback=dummy_execute,
        query_collector=collector
    )
    
    print(f"Processed {total_processed} statements")
    
    # Get the collected queries
    batched_queries = collector.get_queries()
    print(f"Collected {len(batched_queries)} SQL batches")
    
    # Analyze the batches
    print("\nBatch analysis:")
    for i, query_info in enumerate(batched_queries):
        query = query_info["query"]
        metadata = query_info["metadata"]
        
        # Count statements in this batch
        statement_count = query.count("INSERT INTO")
        
        # Get the size in bytes
        query_size = len(query.encode("utf-8"))
        
        print(f"Batch {i+1}:")
        print(f"  Statements: {statement_count}")
        print(f"  Size: {query_size} bytes")
        print(f"  Metadata: {metadata}")
        
        # Show a preview of the first batch
        if i == 0:
            preview = query[:250] + "..." if len(query) > 250 else query
            print(f"  Preview: {preview}")
    
    # Calculate average batch size
    if batched_queries:
        total_statements = sum(q["query"].count("INSERT INTO") for q in batched_queries)
        avg_statements_per_batch = total_statements / len(batched_queries)
        print(f"\nAverage statements per batch: {avg_statements_per_batch:.1f}")
    
    print("\nExample completed successfully!")


if __name__ == "__main__":
    main()