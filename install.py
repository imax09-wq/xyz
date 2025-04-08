#!/usr/bin/env python3
"""
Installation script for setting up the ETL pipeline.

Part of the Sierra Chart ETL Pipeline project.
"""




#!/usr/bin/env python3
"""
Installation script for Sierra Chart ETL Pipeline.
This script automates the setup process, including dependency installation,
configuration creation, and initial setup.
"""

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

# Configuration
REQUIRED_PYTHON_VERSION = (3, 8)
DEPENDENCIES = [
    "fastapi==0.85.0",
    "uvicorn==0.18.3",
    "psutil==5.9.1",
    "numpy==1.23.3",
    "requests==2.28.1",
    "pydantic==1.10.2"
]

REPOSITORY = "https://github.com/your-repo/sierrachart-etl"

# Console colors
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_step(message):
    """Print a step message with formatting."""
    print(f"{Colors.BLUE}{Colors.BOLD}==>{Colors.ENDC} {message}")

def print_success(message):
    """Print a success message with formatting."""
    print(f"{Colors.GREEN}{Colors.BOLD}✓{Colors.ENDC} {message}")

def print_warning(message):
    """Print a warning message with formatting."""
    print(f"{Colors.YELLOW}{Colors.BOLD}!{Colors.ENDC} {message}")

def print_error(message):
    """Print an error message with formatting."""
    print(f"{Colors.RED}{Colors.BOLD}✗{Colors.ENDC} {message}")

def check_python_version():
    """Check if Python version meets requirements."""
    print_step("Checking Python version...")
    
    current_version = sys.version_info
    
    if current_version >= REQUIRED_PYTHON_VERSION:
        print_success(f"Python version {sys.version.split()[0]} is compatible")
        return True
    else:
        print_error(f"Python version {sys.version.split()[0]} is not compatible")
        print(f"Required: Python {REQUIRED_PYTHON_VERSION[0]}.{REQUIRED_PYTHON_VERSION[1]} or higher")
        return False

def create_virtual_environment(target_dir):
    """Create a Python virtual environment."""
    print_step("Creating virtual environment...")
    
    venv_dir = os.path.join(target_dir, "venv")
    
    try:
        subprocess.run(
            [sys.executable, "-m", "venv", venv_dir],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print_success(f"Virtual environment created at: {venv_dir}")
        return venv_dir
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to create virtual environment: {e}")
        print(e.stderr.decode())
        return None

def get_venv_python_path(venv_dir):
    """Get the path to the Python executable in a virtual environment."""
    if os.name == 'nt':  # Windows
        return os.path.join(venv_dir, "Scripts", "python.exe")
    else:  # macOS/Linux
        return os.path.join(venv_dir, "bin", "python")

def install_dependencies(venv_dir):
    """Install required dependencies in the virtual environment."""
    print_step("Installing dependencies...")
    
    venv_pip = get_venv_python_path(venv_dir).replace("python", "pip")
    if os.name == 'nt' and not os.path.exists(venv_pip):
        venv_pip = venv_pip.replace("pip", "pip.exe")
    
    # Create requirements.txt file
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
        f.write("\n".join(DEPENDENCIES))
        requirements_file = f.name
    
    try:
        subprocess.run(
            [venv_pip, "install", "-r", requirements_file],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print_success("Dependencies installed successfully")
        os.unlink(requirements_file)
        return True
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to install dependencies: {e}")
        print(e.stderr.decode())
        os.unlink(requirements_file)
        return False

def create_directories(target_dir):
    """Create necessary directories for the project."""
    print_step("Creating project directories...")
    
    directories = ["logs", "data", "backups"]
    
    for directory in directories:
        dir_path = os.path.join(target_dir, directory)
        os.makedirs(dir_path, exist_ok=True)
        print_success(f"Created directory: {directory}")
    
    return True

def create_config(target_dir, sc_root, db_path):
    """Create the configuration file."""
    print_step("Creating configuration file...")
    
    # Define default configuration
    default_config = {
        "contracts": {
            "ESM25_FUT_CME": {
                "checkpoint_tas": 0,
                "checkpoint_depth": {
                    "date": "",
                    "rec": 0
                },
                "price_adj": 0.01,
                "tas": True,
                "depth": True
            }
        },
        "db_path": db_path,
        "sc_root": sc_root,
        "sleep_int": 1,
        "batch_size": 5000,
        "maintenance_interval": 86400,  # Daily maintenance
        "backup_interval": 604800,      # Weekly backup
        "health_check_interval": 300    # 5-minute health checks
    }
    
    config_path = os.path.join(target_dir, "config.json")
    
    if os.path.exists(config_path):
        print_warning(f"Configuration file already exists at: {config_path}")
        overwrite = input("Do you want to overwrite it? (y/n): ").lower() == 'y'
        
        if not overwrite:
            print("Keeping existing configuration file")
            return config_path
    
    with open(config_path, 'w') as f:
        json.dump(default_config, f, indent=2)
    
    print_success(f"Configuration file created at: {config_path}")
    return config_path

def create_service_config(target_dir, venv_python, config_path, db_path, api_port):
    """Create the service configuration file."""
    print_step("Creating service configuration...")
    
    services_config = {
        "services": [
            {
                "name": "sierrachart_etl",
                "command": [venv_python, "enhanced_etl.py", "1", "--config", config_path],
                "working_dir": target_dir,
                "restart_policy": "always",
                "max_restarts": 10,
                "restart_delay": 10,
                "expected_log_pattern": "Starting ETL pipeline",
                "health_check_cmd": [venv_python, "-c", f"import os; exit(0 if os.path.exists('{db_path}') else 1)"],
                "health_check_interval": 60
            },
            {
                "name": "api_service",
                "command": [venv_python, "api_service.py", "--db-path", db_path, "--host", "0.0.0.0", "--port", str(api_port)],
                "working_dir": target_dir,
                "restart_policy": "always",
                "max_restarts": 10,
                "restart_delay": 10,
                "expected_log_pattern": "Application startup complete",
                "health_check_cmd": ["curl", "-s", f"http://localhost:{api_port}/health"],
                "health_check_interval": 30
            }
        ]
    }
    
    services_path = os.path.join(target_dir, "services.json")
    
    with open(services_path, 'w') as f:
        json.dump(services_config, f, indent=2)
    
    print_success(f"Service configuration created at: {services_path}")
    return services_path

def setup_service_manager(target_dir, venv_python):
    """Set up the service manager for system startup."""
    print_step("Setting up service manager...")
    
    if os.name == 'nt':  # Windows
        # Create a batch file to start the service manager
        bat_path = os.path.join(target_dir, "start_services.bat")
        
        with open(bat_path, 'w') as f:
            f.write(f"@echo off\n")
            f.write(f"cd /d {target_dir}\n")
            f.write(f"{venv_python} service_manager.py start\n")
        
        print_success(f"Created batch file at: {bat_path}")
        
        # Optionally create a Windows scheduled task
        create_task = input("Do you want to create a Windows scheduled task to start on system boot? (y/n): ").lower() == 'y'
        
        if create_task:
            try:
                task_name = "SierraChartETL"
                subprocess.run(
                    ["schtasks", "/create", "/tn", task_name, "/tr", bat_path, "/sc", "onstart", "/ru", "System"],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                print_success(f"Created scheduled task: {task_name}")
            except subprocess.CalledProcessError as e:
                print_error(f"Failed to create scheduled task: {e}")
                print(e.stderr.decode())
    
    else:  # Linux/macOS
        # Create a shell script to start the service manager
        sh_path = os.path.join(target_dir, "start_services.sh")
        
        with open(sh_path, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write(f"cd {target_dir}\n")
            f.write(f"{venv_python} service_manager.py start\n")
        
        # Make it executable
        os.chmod(sh_path, 0o755)
        
        print_success(f"Created shell script at: {sh_path}")
        
        # Systemd service file (Linux)
        if platform.system() == "Linux":
            create_service = input("Do you want to create a systemd service to start on system boot? (y/n): ").lower() == 'y'
            
            if create_service:
                service_file = "/etc/systemd/system/sierrachart-etl.service"
                
                service_content = f"""[Unit]
Description=Sierra Chart ETL Pipeline
After=network.target

[Service]
Type=simple
User={os.getlogin()}
ExecStart={sh_path}
Restart=on-failure
RestartSec=10
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=sierrachart-etl

[Install]
WantedBy=multi-user.target
"""
                
                try:
                    with open(service_file, 'w') as f:
                        f.write(service_content)
                    
                    subprocess.run(
                        ["systemctl", "daemon-reload"],
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    
                    subprocess.run(
                        ["systemctl", "enable", "sierrachart-etl"],
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    
                    print_success("Created and enabled systemd service: sierrachart-etl")
                except (subprocess.CalledProcessError, PermissionError) as e:
                    print_error(f"Failed to create systemd service: {e}")
                    print("You may need to run this script with sudo or create the service manually")
    
    return True

def generate_api_key(target_dir, venv_python, db_path):
    """Generate an initial API key."""
    print_step("Generating API key...")
    
    try:
        process = subprocess.run(
            [venv_python, "api_service.py", "--db-path", db_path, "--generate-admin-key"],
            check=True,
            cwd=target_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        output = process.stdout.decode()
        
        # Find the API key in output
        if "API key" in output and ":" in output:
            key_line = [line for line in output.splitlines() if "API key" in line][0]
            api_key = key_line.split(":", 1)[1].strip()
            
            # Save the key to a file
            key_file = os.path.join(target_dir, "admin_api_key.txt")
            with open(key_file, 'w') as f:
                f.write(f"Admin API Key: {api_key}\n")
                f.write(f"Generated on: {os.uname()} by {os.getlogin()}\n")
                f.write("IMPORTANT: Keep this key secure!\n")
            
            # Set secure permissions
            os.chmod(key_file, 0o600)
            
            print_success(f"Generated admin API key and saved to: {key_file}")
            return True
        else:
            print_warning("Generated API key, but couldn't extract it from output")
            return True
    
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to generate API key: {e}")
        print(e.stderr.decode())
        return False

def main():
    """Main installation function."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Install Sierra Chart ETL Pipeline")
    
    parser.add_argument("--target-dir", required=True, help="Target directory for installation")
    parser.add_argument("--sc-root", required=True, help="Path to Sierra Chart installation")
    parser.add_argument("--db-path", required=True, help="Path to database file")
    parser.add_argument("--api-port", type=int, default=8000, help="Port for API service")
    
    args = parser.parse_args()
    
    # Normalize paths
    target_dir = os.path.abspath(args.target_dir)
    sc_root = os.path.abspath(args.sc_root)
    db_path = os.path.abspath(args.db_path)
    
    # Print installation banner
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 60}")
    print(f"Sierra Chart ETL Pipeline Installer")
    print(f"{'=' * 60}{Colors.ENDC}\n")
    
    # Check requirements
    if not check_python_version():
        return 1
    
    # Create target directory
    os.makedirs(target_dir, exist_ok=True)
    
    # Create virtual environment
    venv_dir = create_virtual_environment(target_dir)
    if not venv_dir:
        return 1
    
    # Get venv Python path
    venv_python = get_venv_python_path(venv_dir)
    
    # Install dependencies
    if not install_dependencies(venv_dir):
        return 1
    
    # Create project directories
    if not create_directories(target_dir):
        return 1
    
    # Create configuration
    config_path = create_config(target_dir, sc_root, db_path)
    
    # Create service configuration
    services_path = create_service_config(target_dir, venv_python, config_path, db_path, args.api_port)
    
    # Set up service manager
    setup_service_manager(target_dir, venv_python)
    
    # Generate initial API key
    generate_api_key(target_dir, venv_python, db_path)
    
    # Print success message
    print(f"\n{Colors.GREEN}{Colors.BOLD}{'=' * 60}")
    print(f"Installation Completed Successfully!")
    print(f"{'=' * 60}{Colors.ENDC}\n")
    
    print(f"To start the services:")
    print(f"  cd {target_dir}")
    print(f"  {venv_python} service_manager.py start\n")
    
    print(f"To check service status:")
    print(f"  {venv_python} service_manager.py status\n")
    
    print(f"API documentation will be available at:")
    print(f"  http://localhost:{args.api_port}/docs\n")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())