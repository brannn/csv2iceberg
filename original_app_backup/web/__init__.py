"""
Web interface for CSV to Iceberg conversion
"""
# Import web application objects when available
try:
    from web.app import app
except ImportError:
    pass