"""
Flask application for CSV to Iceberg conversion web interface
"""
import os
import logging
from flask import Flask

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app(test_config=None):
    """
    Factory function to create and configure the Flask application.
    
    Args:
        test_config: Configuration to use for testing (optional)
        
    Returns:
        Configured Flask application
    """
    # Create and configure the app
    template_dir = os.path.join(os.path.dirname(__file__), "templates")
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    
    # Ensure we're using the package templates, not root level ones
    app = Flask(__name__, 
                template_folder=template_dir,
                static_folder=static_dir)
    
    # Set default configuration
    app.config.from_mapping(
        SECRET_KEY=os.environ.get("FLASK_SECRET_KEY", "dev"),
        UPLOAD_FOLDER=os.path.join(os.path.dirname(__file__), "uploads"),
        MAX_CONTENT_LENGTH=500 * 1024 * 1024,  # 500MB upload limit
    )
    
    # Ensure the upload folder exists
    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
    
    # Register blueprints
    from web.routes import routes, get_git_info
    app.register_blueprint(routes)
    
    # Add template context processor to provide git version information to all templates
    @app.context_processor
    def inject_git_version():
        git_info = get_git_info()
        branch = git_info.get('branch', 'unknown')
        commit = git_info.get('commit', 'unknown')
        return {'git_version': f"{branch}@{commit}"}
    
    # Log application startup
    logger.info("Application initialized")
    
    return app

# Create the application instance
app = create_app()

if __name__ == "__main__":
    # Run the application
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)