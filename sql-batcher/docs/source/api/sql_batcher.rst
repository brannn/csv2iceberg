############
Core Module
############

.. py:module:: sql_batcher

SQLBatcher Class
==============

.. py:class:: SQLBatcher(max_bytes=1000000, delimiter=';', dry_run=False, size_func=None)

   Core class for batching SQL statements based on size limits.
   
   :param max_bytes: Maximum batch size in bytes
   :type max_bytes: int
   :param delimiter: SQL statement delimiter
   :type delimiter: str
   :param dry_run: If True, simulate execution without actually running SQL
   :type dry_run: bool
   :param size_func: Custom function to calculate statement size
   :type size_func: callable, optional
   
   .. py:attribute:: max_bytes
   
      Maximum size of a batch in bytes.
   
   .. py:attribute:: delimiter
   
      SQL statement delimiter character.
   
   .. py:attribute:: dry_run
   
      Whether the batcher operates in dry run mode.
   
   .. py:attribute:: current_batch
   
      List of statements in the current batch.
   
   .. py:attribute:: current_size
   
      Current size of the batch in bytes.
   
   .. py:method:: add_statement(statement)
   
      Add a SQL statement to the current batch.
      
      :param statement: SQL statement to add
      :type statement: str
      :return: True if batch is full and should be flushed, False otherwise
      :rtype: bool
   
   .. py:method:: reset()
   
      Reset the current batch.
   
   .. py:method:: flush(callback, query_collector=None, metadata=None)
   
      Execute all statements in the batch using the provided callback.
      
      :param callback: Function to execute each statement
      :type callback: callable
      :param query_collector: Object to collect queries in dry run mode
      :type query_collector: object, optional
      :param metadata: Additional metadata to associate with queries
      :type metadata: dict, optional
      :return: Number of statements executed
      :rtype: int
   
   .. py:method:: process_statements(statements, callback, query_collector=None, metadata=None)
   
      Process multiple SQL statements, batching them as needed.
      
      :param statements: List of SQL statements to execute
      :type statements: list[str]
      :param callback: Function to execute each statement
      :type callback: callable
      :param query_collector: Object to collect queries in dry run mode
      :type query_collector: object, optional
      :param metadata: Additional metadata to associate with queries
      :type metadata: dict, optional
      :return: Total number of statements processed
      :rtype: int

Example Usage
===========

Basic usage example:

.. code-block:: python

   from sql_batcher import SQLBatcher
   
   # Create a batcher
   batcher = SQLBatcher(max_bytes=500000)
   
   # Define statements
   statements = [
       "INSERT INTO users VALUES (1, 'Alice')",
       "INSERT INTO users VALUES (2, 'Bob')"
   ]
   
   # Define execution callback
   def execute_sql(sql):
       print(f"Executing SQL: {sql}")
       # In a real scenario, this would execute using your DB connection
   
   # Process statements
   batcher.process_statements(statements, execute_sql)