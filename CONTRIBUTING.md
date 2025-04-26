# Contributing to SQL Batcher

First off, thank you for considering contributing to SQL Batcher! It's people like you that make SQL Batcher such a great tool.

## Code of Conduct

This project and everyone participating in it is governed by the SQL Batcher Code of Conduct. By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## How Can I Contribute?

### Reporting Bugs

This section guides you through submitting a bug report. Following these guidelines helps maintainers and the community understand your report, reproduce the behavior, and find related reports.

Before creating bug reports, please check the existing issues list as you might find out that you don't need to create one. When you are creating a bug report, please include as many details as possible:

* **Use a clear and descriptive title** for the issue to identify the problem.
* **Describe the exact steps which reproduce the problem** in as many details as possible.
* **Provide specific examples to demonstrate the steps**. Include links to files or GitHub projects, or copy-pasteable snippets, which you use in those examples.
* **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
* **Explain which behavior you expected to see instead and why.**
* **Include screenshots and animated GIFs** which show you following the described steps and clearly demonstrate the problem.
* **If the problem is related to performance or memory**, include a CPU profile capture with your report.
* **If the problem wasn't triggered by a specific action**, describe what you were doing before the problem happened.

### Suggesting Enhancements

This section guides you through submitting an enhancement suggestion, including completely new features and minor improvements to existing functionality.

* **Use a clear and descriptive title** for the issue to identify the suggestion.
* **Provide a step-by-step description of the suggested enhancement** in as many details as possible.
* **Provide specific examples to demonstrate the steps** or point to similar examples in other projects.
* **Describe the current behavior** and **explain which behavior you expected to see instead** and why.
* **Explain why this enhancement would be useful** to most SQL Batcher users.
* **List some other applications where this enhancement exists.**

### Pull Requests

The process described here has several goals:

- Maintain SQL Batcher's quality
- Fix problems that are important to users
- Engage the community in working toward the best possible SQL Batcher
- Enable a sustainable system for SQL Batcher's maintainers to review contributions

Please follow these steps to have your contribution considered by the maintainers:

1. Follow all instructions in the template
2. Follow the [styleguides](#styleguides)
3. After you submit your pull request, verify that all [status checks](https://help.github.com/articles/about-status-checks/) are passing

## Styleguides

### Git Commit Messages

* Use the present tense ("Add feature" not "Added feature")
* Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
* Limit the first line to 72 characters or less
* Reference issues and pull requests liberally after the first line
* When only changing documentation, include `[ci skip]` in the commit title

### Python Styleguide

All Python code is linted with:

* [Black](https://black.readthedocs.io/en/stable/) for code formatting
* [isort](https://pycqa.github.io/isort/) for import sorting
* [flake8](https://flake8.pycqa.org/en/latest/) for code style
* [mypy](https://mypy.readthedocs.io/en/stable/) for type checking

To ensure your code meets our style requirements, install the development dependencies and run the format and lint commands:

```bash
# Install development dependencies
pip install -e ".[dev]"

# Format the code
black src/ tests/
isort src/ tests/

# Check for style issues
flake8 src/ tests/
mypy src/ tests/
```

### Documentation Styleguide

* Use [Markdown](https://daringfireball.net/projects/markdown) for documentation.
* Follow the conventions in existing documentation.
* Use descriptive link text instead of "here" or "this link."
* Include examples when adding new features.

## Setting Up Development Environment

To set up a development environment for SQL Batcher:

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```
   git clone https://github.com/your-username/sql-batcher.git
   cd sql-batcher
   ```
3. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
4. Install the package in development mode:
   ```
   pip install -e ".[dev,all]"
   ```
5. Create a branch for your feature:
   ```
   git checkout -b name-of-your-feature
   ```

## Testing

Please read the [TESTING.md](TESTING.md) guide for details on how the test suite is structured and how to add tests for your changes.

Basic test commands:

```bash
# Run core tests only (no database connections required)
python run_full_tests.py --core-only

# Run all available tests (requires database connections)
python run_full_tests.py --all

# Run with test coverage reporting
python run_full_tests.py --coverage
```

## Additional Resources

* [General GitHub documentation](https://help.github.com/)
* [GitHub Pull Request documentation](https://help.github.com/articles/about-pull-requests/)