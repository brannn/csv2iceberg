"""
Main entry point for the web application.
"""
# Import from the csv_to_iceberg package
from csv_to_iceberg.web.app import app

# This file only serves as an entry point for Gunicorn to run the application
# All application logic is in the csv_to_iceberg package