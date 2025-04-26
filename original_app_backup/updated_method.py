    def _write_batch_to_iceberg_sql(self, batch_data, mode: str, dry_run: bool = False, query_collector = None) -> None:
        """
        High-performance method to write batch data using optimized SQL INSERT statements with SQLBatcher.
        
        Args:
            batch_data: Batch of data to write (Polars DataFrame)
            mode: Write mode (append or overwrite)
            dry_run: If True, collect queries without executing them
            query_collector: QueryCollector instance for storing queries in dry run mode
        """
        logger.info(f"Using optimized SQL INSERT method with SQLBatcher for batch of {len(batch_data)} rows")
        
        try:
            # Get column names from the dataframe
            columns = batch_data.columns
            quoted_columns = [f'"{col}"' for col in columns]
            column_names_str = ", ".join(quoted_columns)
            
            # Base SQL part
            base_sql = f"INSERT INTO {self.catalog}.{self.schema}.{self.table} ({column_names_str}) VALUES "
            
            # Create a SQL batcher instance - Trino has a limit of ~1,000,000 characters
            # We'll use 900,000 as a safe limit
            sql_batcher = SQLBatcher(max_bytes=900000, dry_run=dry_run)
            
            # Define a callback function for the batcher to use
            def execute_callback(query_sql):
                self.trino_client.execute_query(query_sql)
            
            # Format all rows in the batch
            formatted_rows = []
            for row in batch_data.rows(named=True):
                # Format row values
                row_values = []
                
                for col in columns:
                    val = row[col]
                    if val is None:
                        row_values.append("NULL")
                    elif isinstance(val, bool):
                        row_values.append("TRUE" if val else "FALSE")
                    elif isinstance(val, (int, float)):
                        row_values.append(str(val))
                    elif isinstance(val, datetime.datetime):
                        # Format timestamp properly for Trino
                        row_values.append(f"TIMESTAMP '{val}'")
                    elif isinstance(val, datetime.date):
                        row_values.append(f"DATE '{val}'")
                    else:
                        # Handle string values with proper escaping
                        str_val = str(val).replace("'", "''")
                        row_values.append(f"'{str_val}'")
                
                formatted_rows.append(f"({', '.join(row_values)})")
            
            # Prepare SQL INSERT statements
            insert_statements = []
            
            # Prepare single INSERT with multiple rows using VALUES (...), (...), ...
            values_sql = ", ".join(formatted_rows)
            complete_insert_sql = f"{base_sql}{values_sql}"
            insert_statements.append(complete_insert_sql)
            
            # Define metadata for the query collector
            metadata = {
                "type": "DML",
                "row_count": len(formatted_rows),
                "table_name": f"{self.catalog}.{self.schema}.{self.table}"
            }
            
            # Process all statements using the batcher
            rows_processed = 0
            if dry_run and query_collector:
                # In dry run mode
                rows_processed = sql_batcher.process_statements(
                    insert_statements,
                    execute_callback,
                    query_collector,
                    metadata
                )
                logger.info(f"[DRY RUN] Would insert {len(batch_data)} rows to {self.catalog}.{self.schema}.{self.table}")
            else:
                # Normal execution
                try:
                    rows_processed = sql_batcher.process_statements(
                        insert_statements,
                        execute_callback
                    )
                    logger.info(f"Successfully inserted {len(batch_data)} rows to {self.catalog}.{self.schema}.{self.table} using SQL batcher")
                except Exception as e:
                    logger.error(f"Error during SQL INSERT: {str(e)}", exc_info=True)
                    raise RuntimeError(f"Failed to write data to Iceberg table: {str(e)}")
            
        except Exception as e:
            logger.error(f"Error in SQL INSERT method: {str(e)}", exc_info=True)
            raise RuntimeError(f"Failed to write data using SQL INSERT: {str(e)}")