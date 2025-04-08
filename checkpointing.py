#!/usr/bin/env python3
"""
Atomic checkpointing system to track ETL progress and prevent data loss.

Part of the Sierra Chart ETL Pipeline project.
"""



import json
import os
import tempfile
import shutil
import threading
import logging
from typing import Dict, Any, Optional
from error_handling import ETLError, ErrorCategory, create_error_context

logger = logging.getLogger("SierraChartETL.checkpointing")

class AtomicConfigManager:
    """
    Class for managing configuration with atomic updates to prevent corruption during crashes.
    Thread-safe implementation for concurrent access.
    """
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config_lock = threading.RLock()
        self.config_data = None
        self._load_config()
    
    def _load_config(self) -> None:
        """Load the configuration from disk"""
        with self.config_lock:
            try:
                with open(self.config_path, 'r') as f:
                    self.config_data = json.load(f)
            except FileNotFoundError:
                logger.error(f"Configuration file not found: {self.config_path}")
                ctx = create_error_context(
                    component="ConfigManager", 
                    operation="load_config",
                    file_path=self.config_path
                )
                raise ETLError(
                    message=f"Configuration file not found: {self.config_path}",
                    category=ErrorCategory.CONFIG_ERROR,
                    context=ctx
                )
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in configuration file: {str(e)}")
                
                # If a backup file exists, try to restore it
                backup_path = self._get_backup_path()
                if os.path.exists(backup_path):
                    logger.info(f"Attempting to restore from backup: {backup_path}")
                    try:
                        with open(backup_path, 'r') as f:
                            self.config_data = json.load(f)
                        # Save the restored config
                        self._save_config()
                        logger.info("Successfully restored configuration from backup")
                        return
                    except (json.JSONDecodeError, IOError) as backup_error:
                        logger.error(f"Failed to restore from backup: {str(backup_error)}")
                
                ctx = create_error_context(
                    component="ConfigManager", 
                    operation="load_config",
                    file_path=self.config_path,
                    additional_info={"error": str(e)}
                )
                raise ETLError(
                    message=f"Invalid JSON in configuration file: {str(e)}",
                    category=ErrorCategory.CONFIG_ERROR,
                    context=ctx
                )
    
    def _get_backup_path(self) -> str:
        """Get the path for the config backup file"""
        return f"{self.config_path}.bak"
    
    def _save_config(self) -> None:
        """Save the configuration to disk atomically"""
        with self.config_lock:
            # Create a backup of the current config first
            if os.path.exists(self.config_path):
                try:
                    shutil.copy2(self.config_path, self._get_backup_path())
                except IOError as e:
                    logger.error(f"Failed to create config backup: {str(e)}")
            
            # Write to a temporary file first
            fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(self.config_path))
            try:
                with os.fdopen(fd, 'w') as f:
                    json.dump(self.config_data, f, indent=2)
                
                # On Windows, we need to close the file before replacing it
                if os.name == 'nt' and os.path.exists(self.config_path):
                    os.replace(temp_path, self.config_path)
                else:
                    # Atomic replace on Unix
                    os.rename(temp_path, self.config_path)
            
            except Exception as e:
                # Clean up the temp file if something went wrong
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                
                logger.error(f"Failed to save config: {str(e)}")
                ctx = create_error_context(
                    component="ConfigManager", 
                    operation="save_config",
                    file_path=self.config_path,
                    additional_info={"error": str(e)}
                )
                raise ETLError(
                    message=f"Failed to save configuration: {str(e)}",
                    category=ErrorCategory.SYSTEM_ERROR,
                    context=ctx
                )
    
    def get_config(self) -> Dict[str, Any]:
        """Get a copy of the current configuration"""
        with self.config_lock:
            # Return a deep copy to prevent direct modification
            return json.loads(json.dumps(self.config_data))
    
    def update_config(self, updater_func) -> None:
        """
        Update the configuration using a function that receives and modifies the config.
        This ensures atomic updates.
        """
        with self.config_lock:
            try:
                # Let the updater function modify our config data
                updater_func(self.config_data)
                # Save the updated config
                self._save_config()
            except Exception as e:
                logger.error(f"Error updating config: {str(e)}")
                ctx = create_error_context(
                    component="ConfigManager", 
                    operation="update_config",
                    file_path=self.config_path,
                    additional_info={"error": str(e)}
                )
                raise ETLError(
                    message=f"Error updating configuration: {str(e)}",
                    category=ErrorCategory.CONFIG_ERROR,
                    context=ctx
                )

class CheckpointManager:
    """
    Manager for handling checkpoints with atomic updates and validation.
    """
    def __init__(self, config_manager: AtomicConfigManager):
        self.config_manager = config_manager
        self.last_checkpoint_time = {}  # Track last checkpoint time per contract
    
    def get_tas_checkpoint(self, contract_id: str) -> int:
        """Get the Time & Sales checkpoint for a contract"""
        config = self.config_manager.get_config()
        try:
            return config["contracts"][contract_id]["checkpoint_tas"]
        except KeyError:
            logger.error(f"Contract {contract_id} not found in configuration or missing checkpoint_tas")
            ctx = create_error_context(
                component="CheckpointManager", 
                operation="get_tas_checkpoint",
                contract_id=contract_id
            )
            raise ETLError(
                message=f"Contract {contract_id} not found in configuration or missing checkpoint_tas",
                category=ErrorCategory.CONFIG_ERROR,
                context=ctx
            )
    
    def get_depth_checkpoint(self, contract_id: str) -> tuple:
        """Get the Market Depth checkpoint for a contract"""
        config = self.config_manager.get_config()
        try:
            depth_cp = config["contracts"][contract_id]["checkpoint_depth"]
            return depth_cp["date"], depth_cp["rec"]
        except KeyError:
            logger.error(f"Contract {contract_id} not found in configuration or missing depth checkpoint")
            ctx = create_error_context(
                component="CheckpointManager", 
                operation="get_depth_checkpoint",
                contract_id=contract_id
            )
            raise ETLError(
                message=f"Contract {contract_id} not found in configuration or missing depth checkpoint",
                category=ErrorCategory.CONFIG_ERROR,
                context=ctx
            )
    
    def update_tas_checkpoint(self, contract_id: str, new_checkpoint: int) -> None:
        """Update the Time & Sales checkpoint for a contract"""
        def updater(config):
            try:
                current = config["contracts"][contract_id]["checkpoint_tas"]
                # Ensure checkpoint only moves forward
                if new_checkpoint < current:
                    logger.warning(
                        f"Attempted to update TAS checkpoint backwards: {current} -> {new_checkpoint} "
                        f"for contract {contract_id}"
                    )
                    return
                
                config["contracts"][contract_id]["checkpoint_tas"] = new_checkpoint
                logger.debug(f"Updated TAS checkpoint for {contract_id}: {current} -> {new_checkpoint}")
                
                # Update last checkpoint time
                self.last_checkpoint_time[f"{contract_id}_tas"] = os.path.getmtime(self.config_manager.config_path)
            
            except KeyError:
                logger.error(f"Contract {contract_id} not found in configuration")
                ctx = create_error_context(
                    component="CheckpointManager", 
                    operation="update_tas_checkpoint",
                    contract_id=contract_id
                )
                raise ETLError(
                    message=f"Contract {contract_id} not found in configuration",
                    category=ErrorCategory.CONFIG_ERROR,
                    context=ctx
                )
        
        self.config_manager.update_config(updater)
    
    def update_depth_checkpoint(self, contract_id: str, date: str, rec: int) -> None:
        """Update the Market Depth checkpoint for a contract"""
        def updater(config):
            try:
                current_date = config["contracts"][contract_id]["checkpoint_depth"]["date"]
                current_rec = config["contracts"][contract_id]["checkpoint_depth"]["rec"]
                
                # Validate the update
                if current_date and date < current_date:
                    logger.warning(
                        f"Attempted to update depth date backwards: {current_date} -> {date} "
                        f"for contract {contract_id}"
                    )
                    return
                
                if current_date == date and rec < current_rec:
                    logger.warning(
                        f"Attempted to update depth record backwards: {current_rec} -> {rec} "
                        f"for same date {date} for contract {contract_id}"
                    )
                    return
                
                # Update the checkpoint
                config["contracts"][contract_id]["checkpoint_depth"]["date"] = date
                config["contracts"][contract_id]["checkpoint_depth"]["rec"] = rec
                
                logger.debug(
                    f"Updated depth checkpoint for {contract_id}: "
                    f"({current_date}, {current_rec}) -> ({date}, {rec})"
                )
                
                # Update last checkpoint time
                self.last_checkpoint_time[f"{contract_id}_depth"] = os.path.getmtime(self.config_manager.config_path)
            
            except KeyError:
                logger.error(f"Contract {contract_id} not found in configuration")
                ctx = create_error_context(
                    component="CheckpointManager", 
                    operation="update_depth_checkpoint",
                    contract_id=contract_id
                )
                raise ETLError(
                    message=f"Contract {contract_id} not found in configuration",
                    category=ErrorCategory.CONFIG_ERROR,
                    context=ctx
                )
        
        self.config_manager.update_config(updater)
    
    def verify_checkpoint_integrity(self) -> None:
        """
        Verify the integrity of checkpoints for all contracts.
        This helps detect any potential corruption or inconsistencies.
        """
        config = self.config_manager.get_config()
        issues = []
        
        for contract_id, contract_config in config.get("contracts", {}).items():
            # Check TAS checkpoint
            if "checkpoint_tas" not in contract_config:
                issues.append(f"Missing checkpoint_tas for contract {contract_id}")
            elif not isinstance(contract_config["checkpoint_tas"], int):
                issues.append(f"Invalid checkpoint_tas type for contract {contract_id}")
            
            # Check depth checkpoint
            if "checkpoint_depth" not in contract_config:
                issues.append(f"Missing checkpoint_depth for contract {contract_id}")
            else:
                depth_cp = contract_config["checkpoint_depth"]
                if "date" not in depth_cp:
                    issues.append(f"Missing date in checkpoint_depth for contract {contract_id}")
                if "rec" not in depth_cp:
                    issues.append(f"Missing rec in checkpoint_depth for contract {contract_id}")
                elif not isinstance(depth_cp["rec"], int):
                    issues.append(f"Invalid rec type in checkpoint_depth for contract {contract_id}")
        
        if issues:
            for issue in issues:
                logger.error(issue)
            
            ctx = create_error_context(
                component="CheckpointManager", 
                operation="verify_checkpoint_integrity",
                additional_info={"issues": issues}
            )
            raise ETLError(
                message=f"Checkpoint integrity issues found: {len(issues)} issues",
                category=ErrorCategory.CONFIG_ERROR,
                context=ctx
            )


