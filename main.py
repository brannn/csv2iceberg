"""
Main entry point for the web application.

This file serves as an entry point for Gunicorn and for direct Python execution.
All application logic is organized in modules by functionality.
"""
# Import from the web package
from web.app import app

# This enables the app to be run directly or via Gunicorn with the same import path
if __name__ == "__main__":
    import os
    # Start the development server
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=True
    )