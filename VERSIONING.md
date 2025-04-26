# Versioning Strategy for SQL Batcher

SQL Batcher follows [Semantic Versioning 2.0.0](https://semver.org/). This document clarifies how we apply semantic versioning principles to this library, and what users can expect from different types of releases.

## Version Format

We use the standard `MAJOR.MINOR.PATCH` format:

- **MAJOR**: Incremented for incompatible API changes
- **MINOR**: Incremented for backwards-compatible new functionality
- **PATCH**: Incremented for backwards-compatible bug fixes

## Version Ranges and Compatibility Guarantees

### 0.x.y (Initial Development)

During the initial development phase (versions below 1.0.0), the public API should not be considered stable. While we strive to maintain compatibility when possible, minor version changes may include breaking changes.

- **0.1.x**: First public release, core functionality implemented
- **0.2.x**: API refinement based on early adoption feedback
- **0.x.0**: Feature additions that may include breaking changes
- **0.x.y (y>0)**: Bug fixes, maintaining compatibility with 0.x.0

### 1.0.0 and beyond (Stable)

After reaching version 1.0.0, we commit to the following:

- **Breaking changes** will only be released with a **major version** increment
- **New features** that don't break backward compatibility will be released with a **minor version** increment
- **Bug fixes** that don't break backward compatibility will be released with a **patch version** increment

## What Constitutes a Breaking Change

Any of the following are considered breaking changes requiring a major version bump:

1. Removing or renaming a public method, class, function, or constant
2. Adding required parameters to existing methods (optional parameters are not breaking)
3. Changing the behavior of existing methods in ways that could cause client code to behave incorrectly
4. Changing the return type or structure of public methods
5. Removing functionality that was previously available
6. Adding stricter type checking or validation for input parameters

## Database Adapters Versioning

Special considerations for database adapters:

1. Adding **new** database adapters is considered a minor version change
2. Updates to existing database adapters that maintain the same interface are considered patch or minor changes
3. Breaking changes to a database adapter's interface require a major version change

## Version Locking Recommendations

For users who want stability, we recommend the following in your `requirements.txt` or `pyproject.toml`:

- For applications: Lock to a specific minor version, e.g., `sql-batcher~=1.2.0` (allows patches but not minor updates)
- For libraries: Use a compatible release specifier, e.g., `sql-batcher>=1.2.0,<2.0.0` (allows compatible updates)

## Development Channels

SQL Batcher offers the following development channels:

- **Stable**: Released versions on PyPI (`pip install sql-batcher`)
- **Development**: Main branch on GitHub (bleeding edge, may be unstable)

## Long-term Support

After reaching version 1.0.0, we will maintain:

- **Feature support**: Latest minor release of the most recent major version
- **Security fixes**: Latest two major versions

Security fixes for older versions will be handled as needed, but we strongly encourage updating to supported versions.

## Deprecation Policy

When functionality needs to be removed or changed:

1. The feature will be marked as deprecated in a minor release
2. Deprecation warnings will be displayed when the deprecated feature is used
3. The deprecated feature will continue to work until the next major release
4. Complete removal will occur in the next major release after deprecation

This provides at least one major version cycle for users to update their code.

## Experimental Features

Features marked as "experimental" in the documentation:

1. Are not subject to the same stability guarantees
2. May change or be removed in any release (even patch releases)
3. Will be clearly documented as experimental

## Version Checking

SQL Batcher provides a way to check version compatibility in code:

```python
from sql_batcher import __version__
from packaging import version

if version.parse(__version__) >= version.parse("1.2.0"):
    # Use features from 1.2.0 or above
else:
    # Use alternative approach
```

## Documentation Versioning

Documentation is versioned alongside the code:

- The `main` branch contains documentation for the latest development version
- Tagged releases contain documentation relevant to that specific release
- ReadTheDocs will provide version selection for all published versions