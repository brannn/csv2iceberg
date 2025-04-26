"""
PostgreSQL adapter example for SQL Batcher.

This example demonstrates how to use SQL Batcher with PostgreSQL,
taking advantage of PostgreSQL-specific features like JSONB, COPY commands,
and transaction management.

Requirements:
- PostgreSQL database
- psycopg2-binary package: pip install psycopg2-binary
- sql-batcher[postgresql]: pip install "sql-batcher[postgresql]"
"""

from typing import List, Tuple
import os
import time
import logging
import argparse
import json

from sql_batcher import SQLBatcher
from sql_batcher.adapters.postgresql import PostgreSQLAdapter
from sql_batcher.query_collector import ListQueryCollector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_connection_params():
    """Get PostgreSQL connection parameters from environment or defaults."""
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", 5432)),
        "database": os.environ.get("PGDATABASE", "postgres"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", "postgres"),
    }


def setup_database(adapter: PostgreSQLAdapter) -> None:
    """
    Set up the example database structure.
    
    Args:
        adapter: PostgreSQL adapter instance
    """
    logger.info("Setting up database tables...")
    
    # Create products table with PostgreSQL-specific features
    adapter.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        description TEXT,
        price DECIMAL(10, 2) NOT NULL,
        category VARCHAR(50),
        tags TEXT[],
        metadata JSONB DEFAULT '{}',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Create indices for performance
    adapter.execute("""
    CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
    """)
    
    adapter.execute("""
    CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);
    """)
    
    # Create GIN index for JSONB and array data
    adapter.execute("""
    CREATE INDEX IF NOT EXISTS idx_products_metadata ON products USING GIN(metadata);
    """)
    
    adapter.execute("""
    CREATE INDEX IF NOT EXISTS idx_products_tags ON products USING GIN(tags);
    """)
    
    # Create orders table
    adapter.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        customer_name VARCHAR(100) NOT NULL,
        customer_email VARCHAR(255) NOT NULL,
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(20) DEFAULT 'pending',
        total_amount DECIMAL(12, 2) NOT NULL,
        shipping_address JSONB,
        items JSONB
    )
    """)
    
    # For fresh demo, truncate tables
    adapter.execute("TRUNCATE TABLE products, orders CASCADE")
    
    logger.info("Database setup complete")


def generate_product_data(num_products: int) -> List[Tuple]:
    """
    Generate sample product data.
    
    Args:
        num_products: Number of products to generate
        
    Returns:
        List of product data tuples
    """
    categories = ["Electronics", "Clothing", "Books", "Home", "Food", "Sports"]
    tag_sets = [
        ["new", "featured", "sale"],
        ["clearance", "limited"],
        ["organic", "vegan", "gluten-free"],
        ["imported", "handmade", "luxury"],
        ["eco-friendly", "sustainable", "recycled"]
    ]
    
    products = []
    for i in range(1, num_products + 1):
        # Generate sample product data
        product_id = i
        name = f"Product {i}"
        description = f"This is a detailed description for product {i}"
        price = round((i % 100) * 1.25 + 9.99, 2)
        category = categories[i % len(categories)]
        
        # Create array of tags
        tags = tag_sets[i % len(tag_sets)]
        if i % 5 == 0:
            tags.append("bestseller")
        
        # Create sample metadata as JSONB
        metadata = {
            "weight": (i % 10) + 0.5,
            "dimensions": {
                "width": (i % 10) + 5,
                "height": (i % 15) + 2,
                "depth": (i % 8) + 1
            },
            "in_stock": i % 3 != 0,
            "supplier_id": f"SUP{i % 50 + 1:03d}",
            "rating": (i % 5) + 1
        }
        
        # Convert metadata to JSON string
        metadata_json = json.dumps(metadata)
        
        # Add to products list (for COPY bulk insert)
        products.append((
            product_id,
            name,
            description,
            price,
            category,
            tags,
            metadata_json
        ))
    
    return products


def generate_order_data(num_orders: int, max_product_id: int) -> List[str]:
    """
    Generate sample order SQL statements.
    
    Args:
        num_orders: Number of orders to generate
        max_product_id: Maximum product ID (to reference existing products)
        
    Returns:
        List of SQL INSERT statements for orders
    """
    statements = []
    
    for i in range(1, num_orders + 1):
        customer_name = f"Customer {i}"
        customer_email = f"customer{i}@example.com"
        
        # Generate a random total between $10 and $500
        total_amount = round((i % 49) * 10 + 10.99, 2)
        
        # Create shipping address JSON
        shipping_address = {
            "street": f"{i % 999 + 100} Main St",
            "city": f"City {i % 50 + 1}",
            "state": f"State {i % 50 + 1}",
            "zip": f"{i % 90000 + 10000}",
            "country": "USA"
        }
        
        # Create order items
        num_items = (i % 5) + 1
        items = []
        for j in range(num_items):
            product_id = (i + j) % max_product_id + 1
            items.append({
                "product_id": product_id,
                "quantity": (j % 3) + 1,
                "price": round((product_id % 100) * 1.25 + 9.99, 2)
            })
        
        # Create the INSERT statement with JSON values
        statement = f"""
        INSERT INTO orders (
            customer_name, customer_email, total_amount, 
            shipping_address, items, status
        ) VALUES (
            '{customer_name}', 
            '{customer_email}', 
            {total_amount}, 
            '{json.dumps(shipping_address)}'::jsonb, 
            '{json.dumps(items)}'::jsonb,
            '{'complete' if i % 4 == 0 else 'pending' if i % 4 == 1 else 'shipped' if i % 4 == 2 else 'cancelled'}'
        )
        """
        
        statements.append(statement)
    
    return statements


def run_postgresql_example(args: argparse.Namespace) -> None:
    """
    Run the PostgreSQL example with SQL Batcher.
    
    Args:
        args: Command-line arguments
    """
    connection_params = get_connection_params()
    
    # Log connection info (without password)
    safe_params = {k: v for k, v in connection_params.items() if k != "password"}
    logger.info(f"Connecting to PostgreSQL database with parameters: {safe_params}")
    
    # Create a PostgreSQL adapter with advanced features
    adapter = PostgreSQLAdapter(
        connection_params=connection_params,
        isolation_level="read_committed",
        application_name="sql-batcher-example"
    )
    
    try:
        # Set up the database
        setup_database(adapter)
        
        # Generate product data for bulk insertion with COPY
        num_products = args.num_products
        logger.info(f"Generating {num_products} products for bulk insert via COPY...")
        products = generate_product_data(num_products)
        
        # Use PostgreSQL's COPY command for efficient bulk loading
        start_time = time.time()
        adapter.use_copy_for_bulk_insert(
            table_name="products",
            column_names=["id", "name", "description", "price", "category", "tags", "metadata"],
            data=products
        )
        elapsed = time.time() - start_time
        
        logger.info(f"Inserted {len(products)} products using COPY in {elapsed:.2f} seconds "
                  f"({len(products)/elapsed:.1f} rows/sec)")
        
        # Generate order data as SQL statements
        num_orders = args.num_orders
        logger.info(f"Generating {num_orders} order INSERT statements...")
        order_statements = generate_order_data(num_orders, num_products)
        
        # Create a query collector for analyzing batches
        collector = ListQueryCollector()
        
        # Create a batcher with a 1MB size limit
        batcher = SQLBatcher(max_bytes=args.batch_size)
        
        # Begin a transaction for the batched inserts
        adapter.begin_transaction()
        
        try:
            # Process all order statements
            start_time = time.time()
            total_processed = batcher.process_statements(
                order_statements, 
                adapter.execute,
                query_collector=collector
            )
            elapsed = time.time() - start_time
            
            # Commit the transaction
            adapter.commit_transaction()
            
            # Log batch statistics
            batches = collector.get_queries()
            logger.info(f"Inserted {total_processed} orders in {elapsed:.2f} seconds "
                      f"({total_processed/elapsed:.1f} rows/sec)")
            logger.info(f"Required {len(batches)} batches, averaging "
                      f"{sum(len(b['query'].encode('utf-8')) for b in batches) / len(batches) / 1024:.1f} KB per batch")
            
        except Exception as e:
            # Rollback on error
            adapter.rollback_transaction()
            logger.error(f"Error processing orders: {str(e)}")
            raise
        
        # Run some analytical queries to demonstrate PostgreSQL features
        logger.info("\nRunning analytical queries using PostgreSQL features...")
        
        # Query: Get top product categories by price
        results = adapter.execute("""
        SELECT 
            category, 
            COUNT(*) as product_count, 
            ROUND(AVG(price)::numeric, 2) as avg_price,
            SUM(price) as total_value
        FROM products
        GROUP BY category
        ORDER BY total_value DESC
        """)
        
        logger.info("Top product categories:")
        for row in results:
            logger.info(f"  {row[0]}: {row[1]} products, avg price: ${row[2]}, total value: ${row[3]:.2f}")
        
        # Query: Find products with specific tags using array operators
        results = adapter.execute("""
        SELECT COUNT(*) 
        FROM products 
        WHERE 'bestseller' = ANY(tags)
        """)
        bestseller_count = results[0][0]
        logger.info(f"\nProducts tagged as bestsellers: {bestseller_count}")
        
        # Query: Use JSONB operators to find orders with status updates
        results = adapter.execute("""
        SELECT 
            status, 
            COUNT(*) as order_count,
            ROUND(SUM(total_amount)::numeric, 2) as total_revenue
        FROM orders
        GROUP BY status
        ORDER BY total_revenue DESC
        """)
        
        logger.info("\nOrder status breakdown:")
        for row in results:
            logger.info(f"  {row[0]}: {row[1]} orders, total revenue: ${row[2]}")
        
        # Query: Complex JSON traversal with joins
        results = adapter.execute("""
        SELECT 
            p.category,
            COUNT(DISTINCT o.id) as order_count,
            SUM(oi.quantity) as total_items_sold
        FROM 
            orders o,
            jsonb_to_recordset(o.items) as oi(product_id int, quantity int)
        JOIN 
            products p ON p.id = oi.product_id
        GROUP BY 
            p.category
        ORDER BY 
            total_items_sold DESC
        """)
        
        logger.info("\nItems sold by category (from order items JSON):")
        for row in results:
            logger.info(f"  {row[0]}: {row[1]} orders, {row[2]} items sold")
        
        # Run an EXPLAIN ANALYZE to show query plan
        if args.explain:
            logger.info("\nQuery execution plan for complex JSON query:")
            plan = adapter.explain_analyze("""
            SELECT 
                p.category,
                COUNT(DISTINCT o.id) as order_count,
                SUM(oi.quantity) as total_items_sold
            FROM 
                orders o,
                jsonb_to_recordset(o.items) as oi(product_id int, quantity int)
            JOIN 
                products p ON p.id = oi.product_id
            GROUP BY 
                p.category
            ORDER BY 
                total_items_sold DESC
            """)
            
            for line in plan:
                logger.info(f"  {line[0]}")
        
    finally:
        # Close the connection
        adapter.close()
        logger.info("Example completed")


def main():
    """Run the PostgreSQL example."""
    parser = argparse.ArgumentParser(description='SQL Batcher PostgreSQL Example')
    parser.add_argument('--num-products', type=int, default=1000,
                        help='Number of products to generate')
    parser.add_argument('--num-orders', type=int, default=500,
                        help='Number of orders to generate')
    parser.add_argument('--batch-size', type=int, default=1_000_000,
                        help='Maximum batch size in bytes')
    parser.add_argument('--explain', action='store_true',
                        help='Run EXPLAIN ANALYZE on queries')
    
    args = parser.parse_args()
    
    try:
        run_postgresql_example(args)
    except Exception as e:
        logger.error(f"Error in PostgreSQL example: {str(e)}")
        raise


if __name__ == "__main__":
    main()