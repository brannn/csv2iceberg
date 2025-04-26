# Restructuring Plan

## Current Structure
```
/
├── csv_to_iceberg/
│   ├── __init__.py
│   ├── cli/
│   ├── connectors/
│   ├── core/
│   ├── storage/
│   ├── utils.py
│   ├── main.py
│   └── web/
├── main.py
├── run.py
└── ...
```

## New Structure
```
/
├── app.py              # Main Flask application
├── cli/                # CLI functionality
│   ├── __init__.py
│   └── commands.py
├── connectors/         # External service connectors
│   ├── __init__.py
│   ├── hive_client.py
│   └── trino_client.py
├── core/               # Core business logic
│   ├── __init__.py
│   ├── conversion_service.py
│   ├── iceberg_writer.py
│   └── schema_inferrer.py
├── storage/            # Storage mechanisms
│   ├── __init__.py
│   ├── job_manager.py
│   ├── lmdb_config_manager.py
│   ├── lmdb_job_store.py
│   └── lmdb_stats.py
├── static/             # CSS, JS, images
├── templates/          # Jinja2 templates
├── utils.py            # Utility functions
├── web/                # Web-specific components
│   ├── __init__.py
│   ├── app.py          # Flask app initialization
│   └── routes.py       # Flask routes
├── cli_main.py         # CLI entry point
└── config.py           # Configuration settings
```

## Migration Steps

1. Create the new directory structure at the root level
2. Move functionality from csv_to_iceberg package to the new root-level structure:
   - Move csv_to_iceberg/cli/ to /cli/
   - Move csv_to_iceberg/connectors/ to /connectors/
   - Move csv_to_iceberg/core/ to /core/
   - Move csv_to_iceberg/storage/ to /storage/
   - Move csv_to_iceberg/web/templates/ to /templates/
   - Move csv_to_iceberg/web/static/ to /static/
   - Move csv_to_iceberg/utils.py to /utils.py
   - Move csv_to_iceberg/web/app.py to /web/app.py
   - Move csv_to_iceberg/web/routes.py to /web/routes.py
3. Update imports in all files to remove the 'csv_to_iceberg.' prefix
4. Create new app.py as the main Flask application entry point
5. Create new cli_main.py as the CLI entry point (renamed from run.py)
6. Create a config.py file for centralized configuration
7. Test to ensure everything works
8. Clean up old files and directories