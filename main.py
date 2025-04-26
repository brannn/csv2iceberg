from flask import Flask, render_template, redirect, url_for

app = Flask(__name__)

@app.route('/')
def index():
    return """
    <html>
    <head>
        <title>SQL Batcher</title>
        <link href="https://cdn.replit.com/agent/bootstrap-agent-dark-theme.min.css" rel="stylesheet">
    </head>
    <body>
        <div class="container mt-5">
            <h1>SQL Batcher</h1>
            <p class="lead">A Python library for efficiently batching SQL statements based on size limits</p>
            <div class="alert alert-info">
                <strong>Status:</strong> Test suite running successfully.
            </div>
            <div class="card mt-4">
                <div class="card-header">
                    <h5 class="card-title">Core Features</h5>
                </div>
                <div class="card-body">
                    <ul>
                        <li>Batch SQL statements based on configurable size limits</li>
                        <li>Support for various database backends via adapters</li>
                        <li>Optimization for column-heavy queries with dynamic batch size adjustment</li>
                        <li>Query collection for analysis and debugging</li>
                    </ul>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)