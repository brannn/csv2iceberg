"""
Main entry point for the web application.
"""
# Import from the csv_to_iceberg package
from csv_to_iceberg.web.app import app

# This file serves as an entry point for Gunicorn and for direct Python execution
# All application logic is in the csv_to_iceberg package

if __name__ == "__main__":
    import os
    # Start the development server
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=True
    )