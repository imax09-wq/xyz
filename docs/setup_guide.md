# Sierra Chart ETL Pipeline - Setup Instructions

This document explains how to set up the enhanced Sierra Chart ETL Pipeline using the provided setup scripts.

## Getting Started

### 1. Choose Your Setup Script

Choose the appropriate setup script based on your operating system:

- **Linux/macOS**: `setup_project.sh`
- **Windows**: `setup_project.bat`

### 2. Run the Setup Script

#### For Linux/macOS:

1. Save the `setup_project.sh` script to your computer
2. Make it executable: `chmod +x setup_project.sh`
3. Run it: `./setup_project.sh`

#### For Windows:

1. Save the `setup_project.bat` script to your computer
2. Double-click it or run it from Command Prompt

### 3. Follow the Prompts

- Enter the desired project directory when prompted
- The script will create all necessary directories and empty files

## After Setup

After the script completes, you'll have a directory structure like this:

```
sierrachart_etl/
├── error_handling.py
├── checkpointing.py
├── db_manager.py
├── enhanced_parsers.py
├── enhanced_etl.py
├── data_validator.py
├── monitoring.py
├── api_service.py
├── service_manager.py
├── install.py
├── __init__.py
├── config.json
├── requirements.txt
├── README.md
├── logs/
├── data/
├── backups/
├── tests/
│   └── test_core_components.py
└── docs/
    └── setup_guide.md
```

### 4. Paste the Code

Now you need to paste the code you were provided into each of the corresponding files:

1. Open each Python file in the project directory
2. Delete all the template content
3. Paste the code you were provided for that component

Here's what should go in each file:

| File                    | Description                                            |
|-------------------------|--------------------------------------------------------|
| error_handling.py       | Error handling framework with retry mechanisms         |
| checkpointing.py        | Atomic checkpointing system                            |
| db_manager.py           | Database connection management                         |
| enhanced_parsers.py     | File parsers for Sierra Chart data                     |
| enhanced_etl.py         | Main ETL pipeline                                      |
| data_validator.py       | Data validation and quality checks                     |
| monitoring.py           | System monitoring and alerting                         |
| api_service.py          | REST API for algorithmic trading                       |
| service_manager.py      | Service management for 24/7 operation                  |
| install.py              | Installation script                                    |
| tests/test_core_components.py | Unit tests for the system                        |

### 5. Configure the System

Edit the `config.json` file to set:

- `sc_root`: Path to your Sierra Chart installation
- `db_path`: Where you want the database to be stored
- `contracts`: The contracts you want to process

### 6. Install Dependencies

Create a Python virtual environment and install dependencies:

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
venv\Scripts\activate
# On Linux/macOS:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 7. Run the System

Start the ETL pipeline:

```bash
python enhanced_etl.py 1
```

Start the API server:

```bash
python api_service.py --db-path data/tick.db
```

Or use the service manager to start everything:

```bash
python service_manager.py start
```

## Testing

Run the unit tests:

```bash
pytest tests/
```

## Documentation

The full setup guide is in `docs/setup_guide.md`.

## Troubleshooting

If you encounter any issues:

1. Check the logs in the `logs/` directory
2. Make sure Sierra Chart is correctly configured
3. Verify all dependencies are installed
4. Make sure the file paths in `config.json` are correct

Happy trading with your enhanced Sierra Chart ETL Pipeline!