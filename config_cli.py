#!/usr/bin/env python3
"""
Configuration management CLI for CSV to Iceberg conversion
"""
import os
import sys
import logging
import json
from typing import Optional, Dict, Any, List
import click

from config_manager import ConfigManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Sample settings template for new profiles
PROFILE_TEMPLATE = {
    "connection": {
        "trino_host": "localhost",
        "trino_port": 8080,
        "trino_user": "admin",
        "trino_password": None,
        "trino_catalog": "hive",
        "trino_schema": "default",
        "hive_metastore_uri": "localhost:9083"
    },
    "defaults": {
        "delimiter": ",",
        "has_header": True,
        "quote_char": "\"",
        "batch_size": 10000,
        "mode": "append",
        "sample_size": 1000,
        "verbose": False
    },
    "partitioning": {
        "enabled": False,
        "specs": []
    }
}

@click.group()
@click.version_option(version="1.0.0")
@click.option('--config-file', '-c', help='Path to configuration file')
@click.pass_context
def cli(ctx, config_file):
    """
    Manage configuration profiles for CSV to Iceberg converter.
    
    This tool allows you to create, list, view, and delete configuration profiles.
    """
    # Store config manager in context
    ctx.obj = ConfigManager(config_file)

@cli.command()
@click.pass_obj
def list_profiles(config_manager):
    """List all available profiles."""
    profiles = config_manager.get_profiles()
    
    if not profiles:
        click.echo("No profiles found.")
        return
    
    click.echo("Available profiles:")
    for profile in profiles:
        click.echo(f"  - {profile}")

@cli.command()
@click.argument('profile_name')
@click.pass_obj
def view_profile(config_manager, profile_name):
    """View a specific profile."""
    if not config_manager.profile_exists(profile_name):
        click.echo(f"Profile '{profile_name}' not found.")
        return
    
    profile = config_manager.get_profile(profile_name)
    click.echo(json.dumps(profile, indent=2))

@cli.command()
@click.argument('profile_name')
@click.option('--interactive/--template', '-i/-t', default=True, 
              help='Interactive creation or use template')
@click.pass_obj
def create_profile(config_manager, profile_name, interactive):
    """Create a new profile."""
    if config_manager.profile_exists(profile_name):
        overwrite = click.confirm(f"Profile '{profile_name}' already exists. Overwrite?", default=False)
        if not overwrite:
            click.echo("Operation cancelled.")
            return
    
    if interactive:
        # Interactive profile creation
        profile = _create_profile_interactive()
    else:
        # Use template
        profile = PROFILE_TEMPLATE.copy()
        click.echo(f"Created profile with default template. Edit it using 'edit_profile {profile_name}'.")
    
    # Save the profile
    if config_manager.create_profile(profile_name, profile):
        config_manager.save()
        click.echo(f"Profile '{profile_name}' created and saved.")
    else:
        click.echo(f"Failed to create profile '{profile_name}'.")

@cli.command()
@click.argument('profile_name')
@click.pass_obj
def delete_profile(config_manager, profile_name):
    """Delete a profile."""
    if not config_manager.profile_exists(profile_name):
        click.echo(f"Profile '{profile_name}' not found.")
        return
    
    confirm = click.confirm(f"Are you sure you want to delete profile '{profile_name}'?", default=False)
    if not confirm:
        click.echo("Operation cancelled.")
        return
    
    if config_manager.delete_profile(profile_name):
        config_manager.save()
        click.echo(f"Profile '{profile_name}' deleted.")
    else:
        click.echo(f"Failed to delete profile '{profile_name}'.")

@cli.command()
@click.argument('profile_name')
@click.pass_obj
def edit_profile(config_manager, profile_name):
    """Edit an existing profile with your default text editor."""
    if not config_manager.profile_exists(profile_name):
        click.echo(f"Profile '{profile_name}' not found.")
        return
    
    # Get the profile
    profile = config_manager.get_profile(profile_name)
    
    # Create a temporary file
    import tempfile
    import subprocess
    
    with tempfile.NamedTemporaryFile(suffix='.json', mode='w+', delete=False) as temp_file:
        json.dump(profile, temp_file, indent=2)
        temp_file_path = temp_file.name
    
    # Get the default editor
    editor = os.environ.get('EDITOR', 'vim')
    
    try:
        # Open the file in the editor
        subprocess.call([editor, temp_file_path])
        
        # Read the updated content
        with open(temp_file_path, 'r') as f:
            updated_profile = json.load(f)
        
        # Update the profile
        if config_manager.create_profile(profile_name, updated_profile):
            config_manager.save()
            click.echo(f"Profile '{profile_name}' updated.")
        else:
            click.echo(f"Failed to update profile '{profile_name}'.")
    except Exception as e:
        click.echo(f"Error editing profile: {str(e)}")
    finally:
        # Clean up
        try:
            os.unlink(temp_file_path)
        except:
            pass

@cli.command()
@click.argument('profile_name')
@click.argument('output_file', required=False)
@click.pass_obj
def export_profile(config_manager, profile_name, output_file):
    """Export a profile to a file."""
    if not config_manager.profile_exists(profile_name):
        click.echo(f"Profile '{profile_name}' not found.")
        return
    
    profile = config_manager.get_profile(profile_name)
    
    if output_file:
        # Export to file
        try:
            with open(output_file, 'w') as f:
                json.dump(profile, f, indent=2)
            click.echo(f"Profile exported to {output_file}")
        except Exception as e:
            click.echo(f"Error exporting profile: {str(e)}")
    else:
        # Print to stdout
        click.echo(json.dumps(profile, indent=2))

@cli.command()
@click.argument('input_file')
@click.argument('profile_name', required=False)
@click.pass_obj
def import_profile(config_manager, input_file, profile_name):
    """Import a profile from a file."""
    try:
        with open(input_file, 'r') as f:
            profile = json.load(f)
        
        # Use filename as profile name if not specified
        if not profile_name:
            profile_name = os.path.splitext(os.path.basename(input_file))[0]
        
        # Ask for confirmation if profile exists
        if config_manager.profile_exists(profile_name):
            confirm = click.confirm(f"Profile '{profile_name}' already exists. Overwrite?", default=False)
            if not confirm:
                click.echo("Import cancelled.")
                return
        
        # Import the profile
        if config_manager.create_profile(profile_name, profile):
            config_manager.save()
            click.echo(f"Profile '{profile_name}' imported.")
        else:
            click.echo(f"Failed to import profile.")
    except Exception as e:
        click.echo(f"Error importing profile: {str(e)}")

def _create_profile_interactive() -> Dict[str, Any]:
    """Create a profile through interactive prompts."""
    profile: Dict[str, Any] = {
        "connection": {},
        "defaults": {},
        "partitioning": {"enabled": False, "specs": []}
    }
    
    click.echo("Creating a new profile. Press Enter to accept default values.")
    
    # Connection settings
    click.echo("\nConnection Settings:")
    profile["connection"]["trino_host"] = click.prompt("Trino host", default="localhost")
    profile["connection"]["trino_port"] = click.prompt("Trino port", default=8080, type=int)
    profile["connection"]["trino_user"] = click.prompt("Trino user", default="admin")
    use_password = click.confirm("Use Trino password?", default=False)
    profile["connection"]["trino_password"] = click.prompt("Trino password", default="", hide_input=True) if use_password else None
    profile["connection"]["trino_catalog"] = click.prompt("Trino catalog", default="hive")
    profile["connection"]["trino_schema"] = click.prompt("Trino schema", default="default")
    profile["connection"]["hive_metastore_uri"] = click.prompt("Hive metastore URI", default="localhost:9083")
    
    # Default settings
    click.echo("\nDefault Settings:")
    profile["defaults"]["delimiter"] = click.prompt("CSV delimiter", default=",")
    profile["defaults"]["has_header"] = click.confirm("CSV has header?", default=True)
    profile["defaults"]["quote_char"] = click.prompt("CSV quote character", default="\"")
    profile["defaults"]["batch_size"] = click.prompt("Batch size", default=10000, type=int)
    profile["defaults"]["mode"] = click.prompt("Default mode", default="append", type=click.Choice(["append", "overwrite"]))
    profile["defaults"]["sample_size"] = click.prompt("Schema inference sample size", default=1000, type=int)
    profile["defaults"]["verbose"] = click.confirm("Verbose logging?", default=False)
    
    # Partitioning settings
    click.echo("\nPartitioning Settings:")
    profile["partitioning"]["enabled"] = click.confirm("Enable partitioning by default?", default=False)
    
    if profile["partitioning"]["enabled"]:
        specs = []
        while True:
            add_spec = click.confirm("Add a partition specification?", default=True)
            if not add_spec:
                break
            
            spec = click.prompt("Enter partition spec (e.g., 'year(date)' or 'bucket(id, 16)')")
            specs.append(spec)
        
        profile["partitioning"]["specs"] = specs
    
    return profile

if __name__ == "__main__":
    cli(obj=None)