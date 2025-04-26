"""
Main entry point for the CSV to Iceberg application.
"""
import os
import sys

def main():
    """Main entry point allowing for CLI or web execution."""
    # Check if CLI or web app is requested
    if len(sys.argv) > 1:
        # Import CLI module and run it
        from csv_to_iceberg.cli.commands import cli
        cli()
    else:
        # Import web app module and run it
        from csv_to_iceberg.web.app import app
        # Run the web application
        port = int(os.environ.get("PORT", 5000))
        app.run(host="0.0.0.0", port=port, debug=True)

if __name__ == "__main__":
    main()