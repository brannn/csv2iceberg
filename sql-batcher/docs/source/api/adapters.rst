################
Adapters Module
################

.. py:module:: sql_batcher.adapters

SQLAdapter Base Class
===================

.. py:class:: sql_batcher.adapters.base.SQLAdapter

   Abstract base class defining the interface for all database adapters.
   
   .. py:method:: execute(sql)
      :abstractmethod:
   
      Execute a SQL statement and return results.
      
      :param sql: SQL statement to execute
      :type sql: str
      :return: Results from the query
      :rtype: list[tuple]
   
   .. py:method:: get_max_query_size()
      :abstractmethod:
   
      Return the maximum query size in bytes.
      
      :return: Maximum query size
      :rtype: int
   
   .. py:method:: close()
      :abstractmethod:
   
      Close database connection and clean up resources.
   
   .. py:method:: begin_transaction()
   
      Begin a database transaction (if supported).
   
   .. py:method:: commit_transaction()
   
      Commit the current transaction (if supported).
   
   .. py:method:: rollback_transaction()
   
      Rollback the current transaction (if supported).

Generic Adapter
=============

.. py:class:: sql_batcher.adapters.generic.GenericAdapter(connection, max_query_size=1000000, fetch_results=True)

   Generic adapter for DBAPI 2.0 compliant database connections.
   
   :param connection: DBAPI-compliant database connection
   :type connection: object
   :param max_query_size: Maximum query size in bytes
   :type max_query_size: int
   :param fetch_results: Whether to fetch and return results
   :type fetch_results: bool
   
   .. py:method:: execute(sql)
   
      Execute a SQL statement using the provided connection.
      
      :param sql: SQL statement to execute
      :type sql: str
      :return: Results from the query
      :rtype: list[tuple]
   
   .. py:method:: get_max_query_size()
   
      Return the maximum query size in bytes.
      
      :return: Maximum query size
      :rtype: int
   
   .. py:method:: close()
   
      Close the database connection.
   
   .. py:method:: begin_transaction()
   
      Begin a database transaction.
   
   .. py:method:: commit_transaction()
   
      Commit the current transaction.
   
   .. py:method:: rollback_transaction()
   
      Rollback the current transaction.

Trino Adapter
===========

.. py:class:: sql_batcher.adapters.trino.TrinoAdapter(host, port=443, user='admin', password=None, catalog='hive', schema='default', http_scheme='https', role=None, auth=None, max_query_size=16777216, headers=None, verify=True, dry_run=False)

   Adapter for Trino database connections.
   
   :param host: Trino host name
   :type host: str
   :param port: Trino port number
   :type port: int
   :param user: Username for authentication
   :type user: str
   :param password: Password for authentication
   :type password: str, optional
   :param catalog: Catalog name
   :type catalog: str
   :param schema: Schema name
   :type schema: str
   :param http_scheme: HTTP scheme (http or https)
   :type http_scheme: str
   :param role: Trino role
   :type role: str, optional
   :param auth: Authentication object
   :type auth: object, optional
   :param max_query_size: Maximum query size in bytes
   :type max_query_size: int
   :param headers: Additional HTTP headers
   :type headers: dict, optional
   :param verify: Whether to verify SSL certificates
   :type verify: bool
   :param dry_run: Whether to simulate without actual connections
   :type dry_run: bool
   
   .. py:method:: execute(sql)
   
      Execute a SQL statement on Trino.
      
      :param sql: SQL statement to execute
      :type sql: str
      :return: Results from the query
      :rtype: list[tuple]
   
   .. py:method:: get_max_query_size()
   
      Return the maximum query size in bytes.
      
      :return: Maximum query size
      :rtype: int
   
   .. py:method:: close()
   
      Close the Trino connection.

Spark Adapter
===========

.. py:class:: sql_batcher.adapters.spark.SparkAdapter(spark_session, return_dataframe=False, max_query_size=1000000, fetch_limit=None)

   Adapter for Spark SQL using PySpark.
   
   :param spark_session: Spark session object
   :type spark_session: pyspark.sql.SparkSession
   :param return_dataframe: Whether to return DataFrames instead of lists
   :type return_dataframe: bool
   :param max_query_size: Maximum query size in bytes
   :type max_query_size: int
   :param fetch_limit: Maximum number of rows to fetch
   :type fetch_limit: int, optional
   
   .. py:method:: execute(sql)
   
      Execute a SQL statement on Spark.
      
      :param sql: SQL statement to execute
      :type sql: str
      :return: Results from the query
      :rtype: list[tuple] or pyspark.sql.DataFrame
   
   .. py:method:: get_max_query_size()
   
      Return the maximum query size in bytes.
      
      :return: Maximum query size
      :rtype: int
   
   .. py:method:: close()
   
      Clean up any resources.

Snowflake Adapter
===============

.. py:class:: sql_batcher.adapters.snowflake.SnowflakeAdapter(connection_params, max_query_size=1048576, auto_close=False, connection_timeout=60, fetch_limit=None)

   Adapter for Snowflake database connections.
   
   :param connection_params: Connection parameters dictionary
   :type connection_params: dict
   :param max_query_size: Maximum query size in bytes
   :type max_query_size: int
   :param auto_close: Whether to automatically close connection
   :type auto_close: bool
   :param connection_timeout: Connection timeout in seconds
   :type connection_timeout: int
   :param fetch_limit: Maximum number of rows to fetch
   :type fetch_limit: int, optional
   
   .. py:method:: execute(sql)
   
      Execute a SQL statement on Snowflake.
      
      :param sql: SQL statement to execute
      :type sql: str
      :return: Results from the query
      :rtype: list[tuple]
   
   .. py:method:: get_max_query_size()
   
      Return the maximum query size in bytes.
      
      :return: Maximum query size
      :rtype: int
   
   .. py:method:: close()
   
      Close the Snowflake connection.
   
   .. py:method:: begin_transaction()
   
      Begin a Snowflake transaction.
   
   .. py:method:: commit_transaction()
   
      Commit the current Snowflake transaction.
   
   .. py:method:: rollback_transaction()
   
      Rollback the current Snowflake transaction.