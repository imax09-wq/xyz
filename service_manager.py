#!/usr/bin/env python3
"""
Service manager for 24/7 operation with automatic restarts.

Part of the Sierra Chart ETL Pipeline project.
"""




import argparse
import json
import logging
import os
import psutil
import signal
import subprocess
import sys
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger("SierraChartETL.service")

class ServiceStatus:
    """Status of a managed service"""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"
    RESTARTING = "restarting"

class ServiceConfig:
    """Configuration for a managed service"""
    def __init__(
        self,
        name: str,
        command: List[str],
        working_dir: Optional[str] = None,
        environment: Optional[Dict[str, str]] = None,
        restart_policy: str = "always",
        max_restarts: int = 10,
        restart_delay: int = 10,
        expected_log_pattern: Optional[str] = None,
        health_check_cmd: Optional[List[str]] = None,
        health_check_interval: int = 30,
        success_exit_codes: List[int] = None,
        startup_timeout: int = 60
    ):
        self.name = name
        self.command = command
        self.working_dir = working_dir or os.getcwd()
        self.environment = environment or {}
        self.restart_policy = restart_policy  # always, on-failure, never
        self.max_restarts = max_restarts
        self.restart_delay = restart_delay  # seconds
        self.expected_log_pattern = expected_log_pattern
        self.health_check_cmd = health_check_cmd
        self.health_check_interval = health_check_interval
        self.success_exit_codes = success_exit_codes or [0]
        self.startup_timeout = startup_timeout  # seconds
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ServiceConfig':
        """Create a ServiceConfig from a dictionary"""
        return cls(
            name=data["name"],
            command=data["command"],
            working_dir=data.get("working_dir"),
            environment=data.get("environment"),
            restart_policy=data.get("restart_policy", "always"),
            max_restarts=data.get("max_restarts", 10),
            restart_delay=data.get("restart_delay", 10),
            expected_log_pattern=data.get("expected_log_pattern"),
            health_check_cmd=data.get("health_check_cmd"),
            health_check_interval=data.get("health_check_interval", 30),
            success_exit_codes=data.get("success_exit_codes", [0]),
            startup_timeout=data.get("startup_timeout", 60)
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "name": self.name,
            "command": self.command,
            "working_dir": self.working_dir,
            "environment": self.environment,
            "restart_policy": self.restart_policy,
            "max_restarts": self.max_restarts,
            "restart_delay": self.restart_delay,
            "expected_log_pattern": self.expected_log_pattern,
            "health_check_cmd": self.health_check_cmd,
            "health_check_interval": self.health_check_interval,
            "success_exit_codes": self.success_exit_codes,
            "startup_timeout": self.startup_timeout
        }

class ManagedService:
    """A service managed by the service manager"""
    def __init__(self, config: ServiceConfig):
        self.config = config
        self.process = None
        self.status = ServiceStatus.STOPPED
        self.last_start_time = None
        self.last_stop_time = None
        self.restart_count = 0
        self.health_check_thread = None
        self.log_file = None
        self.log_path = f"logs/{config.name}.log"
        self.stdout_path = f"logs/{config.name}.stdout.log"
        self.stderr_path = f"logs/{config.name}.stderr.log"
        self.exit_code = None
        self.pid = None
        self.stopping = False
        self.health_check_running = False
    
    def start(self) -> bool:
        """Start the service"""
        if self.status in [ServiceStatus.RUNNING, ServiceStatus.STARTING]:
            logger.warning(f"Service {self.config.name} is already {self.status}")
            return True
        
        # Create log directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)
        
        # Open log files
        try:
            self.log_file = open(self.log_path, "a")
            stdout_file = open(self.stdout_path, "a")
            stderr_file = open(self.stderr_path, "a")
            
            # Log startup
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            startup_msg = f"\n[{timestamp}] === Starting service {self.config.name} ===\n"
            self.log_file.write(startup_msg)
            self.log_file.flush()
        except Exception as e:
            logger.error(f"Failed to open log files for {self.config.name}: {str(e)}")
            return False
        
        try:
            self.status = ServiceStatus.STARTING
            self.last_start_time = time.time()
            
            # Prepare environment
            env = os.environ.copy()
            env.update(self.config.environment)
            
            # Start process
            logger.info(f"Starting service {self.config.name}: {' '.join(self.config.command)}")
            
            self.process = subprocess.Popen(
                self.config.command,
                cwd=self.config.working_dir,
                env=env,
                stdout=stdout_file,
                stderr=stderr_file,
                universal_newlines=True,
                bufsize=1  # Line buffered
            )
            
            self.pid = self.process.pid
            logger.info(f"Service {self.config.name} started with PID {self.pid}")
            
            # Wait for startup to complete
            startup_deadline = time.time() + self.config.startup_timeout
            while time.time() < startup_deadline:
                # Check if process is still running
                if self.process.poll() is not None:
                    self.exit_code = self.process.returncode
                    self.status = ServiceStatus.FAILED
                    logger.error(
                        f"Service {self.config.name} failed to start (exit code {self.exit_code})"
                    )
                    return False
                
                # Check for expected log pattern if specified
                if self.config.expected_log_pattern:
                    with open(self.stdout_path, "r") as f:
                        content = f.read()
                        if self.config.expected_log_pattern in content:
                            break
                
                # Otherwise, just wait a bit
                time.sleep(1)
            
            # Start health check if needed
            if self.config.health_check_cmd:
                self.start_health_check()
            
            self.status = ServiceStatus.RUNNING
            return True
        
        except Exception as e:
            logger.error(f"Failed to start service {self.config.name}: {str(e)}")
            self.status = ServiceStatus.FAILED
            return False
    
    def stop(self, timeout: int = 30) -> bool:
        """Stop the service"""
        if self.status not in [ServiceStatus.RUNNING, ServiceStatus.STARTING]:
            logger.warning(f"Service {self.config.name} is not running (status: {self.status})")
            return True
        
        self.stopping = True
        self.status = ServiceStatus.STOPPING
        logger.info(f"Stopping service {self.config.name} (PID {self.pid})")
        
        # Stop health check thread
        self.health_check_running = False
        if self.health_check_thread and self.health_check_thread.is_alive():
            self.health_check_thread.join(5)
        
        # Stop the process
        if self.process:
            try:
                # Send SIGTERM
                self.process.terminate()
                
                # Wait for process to exit
                try:
                    self.process.wait(timeout=timeout)
                    self.exit_code = self.process.returncode
                    logger.info(
                        f"Service {self.config.name} stopped (exit code {self.exit_code})"
                    )
                except subprocess.TimeoutExpired:
                    # Process didn't exit, send SIGKILL
                    logger.warning(
                        f"Service {self.config.name} did not exit after {timeout}s, sending SIGKILL"
                    )
                    self.process.kill()
                    self.process.wait(timeout=5)
                    self.exit_code = self.process.returncode
                    logger.info(
                        f"Service {self.config.name} killed (exit code {self.exit_code})"
                    )
            
            except Exception as e:
                logger.error(f"Error stopping service {self.config.name}: {str(e)}")
                self.status = ServiceStatus.FAILED
                return False
        
        # Close log files
        if self.log_file:
            try:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                stop_msg = f"\n[{timestamp}] === Service {self.config.name} stopped ===\n"
                self.log_file.write(stop_msg)
                self.log_file.flush()
                self.log_file.close()
                self.log_file = None
            except:
                pass
        
        self.last_stop_time = time.time()
        self.status = ServiceStatus.STOPPED
        self.stopping = False
        self.process = None
        self.pid = None
        
        return True
    
    def restart(self) -> bool:
        """Restart the service"""
        self.status = ServiceStatus.RESTARTING
        logger.info(f"Restarting service {self.config.name}")
        
        # Stop the service
        self.stop()
        
        # Wait for restart delay
        if self.config.restart_delay > 0:
            time.sleep(self.config.restart_delay)
        
        # Increment restart count
        self.restart_count += 1
        
        # Start the service
        return self.start()
    
    def start_health_check(self) -> None:
        """Start the health check thread"""
        if self.health_check_running:
            return
        
        self.health_check_running = True
        
        self.health_check_thread = threading.Thread(
            target=self._health_check_loop,
            daemon=True
        )
        self.health_check_thread.start()
    
    def _health_check_loop(self) -> None:
        """Health check thread function"""
        logger.info(f"Started health check for service {self.config.name}")
        
        while self.health_check_running:
            # Skip if service is stopping
            if self.stopping:
                time.sleep(1)
                continue
            
            try:
                # Run health check command
                result = subprocess.run(
                    self.config.health_check_cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode != 0:
                    logger.warning(
                        f"Health check failed for service {self.config.name}: "
                        f"exit code {result.returncode}"
                    )
                    logger.debug(f"Health check output: {result.stdout}\n{result.stderr}")
                    
                    # Check if service is still running
                    if not self.stopping and self.status == ServiceStatus.RUNNING:
                        if self.process and self.process.poll() is None:
                            # Process is still running but health check failed
                            logger.error(
                                f"Service {self.config.name} is unhealthy, restarting"
                            )
                            self.restart()
                            # Don't check again for a while
                            time.sleep(max(60, self.config.health_check_interval))
                            continue
            
            except Exception as e:
                logger.error(f"Error in health check for {self.config.name}: {str(e)}")
            
            # Sleep until next check
            time.sleep(self.config.health_check_interval)
    
    def check_process_status(self) -> None:
        """Check the status of the process"""
        if self.status not in [ServiceStatus.RUNNING, ServiceStatus.STARTING] or not self.process:
            return
        
        # Check if process has exited
        if self.process.poll() is not None:
            self.exit_code = self.process.returncode
            logger.warning(
                f"Service {self.config.name} exited unexpectedly with code {self.exit_code}"
            )
            
            # Check if we should restart
            if self.config.restart_policy == "always" or (
                self.config.restart_policy == "on-failure" and
                self.exit_code not in self.config.success_exit_codes
            ):
                if self.restart_count < self.config.max_restarts:
                    logger.info(
                        f"Restarting service {self.config.name} "
                        f"(attempt {self.restart_count + 1}/{self.config.max_restarts})"
                    )
                    self.restart()
                else:
                    logger.error(
                        f"Service {self.config.name} has reached maximum restart attempts "
                        f"({self.config.max_restarts}), not restarting"
                    )
                    self.status = ServiceStatus.FAILED
            else:
                self.status = ServiceStatus.STOPPED
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get service metrics"""
        metrics = {
            "name": self.config.name,
            "status": self.status,
            "pid": self.pid,
            "restart_count": self.restart_count,
            "last_start_time": self.last_start_time,
            "last_stop_time": self.last_stop_time,
            "uptime": None,
            "exit_code": self.exit_code,
            "memory_usage": None,
            "cpu_usage": None
        }
        
        # Calculate uptime
        if self.last_start_time and self.status == ServiceStatus.RUNNING:
            metrics["uptime"] = time.time() - self.last_start_time
        
        # Get resource usage if process is running
        if self.pid and self.status == ServiceStatus.RUNNING:
            try:
                process = psutil.Process(self.pid)
                
                # Memory usage (MB)
                memory_info = process.memory_info()
                metrics["memory_usage"] = memory_info.rss / (1024 * 1024)
                
                # CPU usage (percentage)
                metrics["cpu_usage"] = process.cpu_percent(interval=0.1)
                
                # Add child processes
                metrics["child_processes"] = []
                for child in process.children(recursive=True):
                    try:
                        child_memory = child.memory_info().rss / (1024 * 1024)
                        child_cpu = child.cpu_percent(interval=0.1)
                        
                        metrics["child_processes"].append({
                            "pid": child.pid,
                            "memory_usage": child_memory,
                            "cpu_usage": child_cpu
                        })
                    except:
                        pass
            
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        return metrics

class ServiceManager:
    """Manages multiple services"""
    def __init__(self, config_file: str = None):
        self.services = {}  # name -> ManagedService
        self.config_file = config_file
        self.running = False
        self.check_thread = None
    
    def load_config(self, config_file: str) -> bool:
        """Load configuration from a file"""
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            
            for service_config in config_data["services"]:
                config = ServiceConfig.from_dict(service_config)
                self.add_service(config)
            
            logger.info(f"Loaded {len(self.services)} services from {config_file}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to load configuration from {config_file}: {str(e)}")
            return False
    
    def save_config(self, config_file: str = None) -> bool:
        """Save configuration to a file"""
        if not config_file:
            config_file = self.config_file
        
        if not config_file:
            logger.error("No configuration file specified")
            return False
        
        try:
            config_data = {
                "services": [service.config.to_dict() for service in self.services.values()]
            }
            
            with open(config_file, 'w') as f:
                json.dump(config_data, f, indent=2)
            
            logger.info(f"Saved configuration to {config_file}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to save configuration to {config_file}: {str(e)}")
            return False
    
    def add_service(self, config: ServiceConfig) -> None:
        """Add a service to the manager"""
        if config.name in self.services:
            logger.warning(f"Service {config.name} already exists, replacing")
        
        self.services[config.name] = ManagedService(config)
        logger.info(f"Added service {config.name}")
    
    def remove_service(self, name: str) -> bool:
        """Remove a service from the manager"""
        if name not in self.services:
            logger.warning(f"Service {name} does not exist")
            return False
        
        # Stop the service if it's running
        service = self.services[name]
        if service.status in [ServiceStatus.RUNNING, ServiceStatus.STARTING]:
            service.stop()
        
        # Remove the service
        del self.services[name]
        logger.info(f"Removed service {name}")
        return True
    
    def start_service(self, name: str) -> bool:
        """Start a specific service"""
        if name not in self.services:
            logger.error(f"Service {name} does not exist")
            return False
        
        return self.services[name].start()
    
    def stop_service(self, name: str) -> bool:
        """Stop a specific service"""
        if name not in self.services:
            logger.error(f"Service {name} does not exist")
            return False
        
        return self.services[name].stop()
    
    def restart_service(self, name: str) -> bool:
        """Restart a specific service"""
        if name not in self.services:
            logger.error(f"Service {name} does not exist")
            return False
        
        return self.services[name].restart()
    
    def start_all(self) -> None:
        """Start all services"""
        logger.info("Starting all services")
        
        for name, service in self.services.items():
            service.start()
    
    def stop_all(self) -> None:
        """Stop all services"""
        logger.info("Stopping all services")
        
        for name, service in sorted(
            self.services.items(),
            key=lambda x: x[1].last_start_time or 0,
            reverse=True
        ):
            service.stop()
    
    def start_manager(self) -> None:
        """Start the service manager"""
        if self.running:
            logger.warning("Service manager is already running")
            return
        
        self.running = True
        
        # Start the status check thread
        self.check_thread = threading.Thread(
            target=self._status_check_loop,
            daemon=True
        )
        self.check_thread.start()
        
        logger.info("Service manager started")
    
    def stop_manager(self) -> None:
        """Stop the service manager"""
        if not self.running:
            return
        
        logger.info("Stopping service manager")
        self.running = False
        
        # Wait for check thread to exit
        if self.check_thread and self.check_thread.is_alive():
            self.check_thread.join(5)
        
        logger.info("Service manager stopped")
    
    def _status_check_loop(self) -> None:
        """Background thread for checking service status"""
        while self.running:
            try:
                for name, service in self.services.items():
                    service.check_process_status()
            
            except Exception as e:
                logger.error(f"Error in status check: {str(e)}")
            
            # Sleep for a bit
            time.sleep(2)
    
    def get_service_status(self, name: str = None) -> Dict[str, Any]:
        """Get status of all services or a specific service"""
        if name:
            if name not in self.services:
                logger.error(f"Service {name} does not exist")
                return None
            
            return self.services[name].get_metrics()
        else:
            return {
                name: service.get_metrics()
                for name, service in self.services.items()
            }

class ServiceManagerCLI:
    """Command-line interface for the service manager"""
    def __init__(self):
        self.service_manager = ServiceManager()
    
    def parse_args(self):
        """Parse command line arguments"""
        parser = argparse.ArgumentParser(description="Sierra Chart ETL Service Manager")
        
        # Config file
        parser.add_argument(
            "--config",
            default="services.json",
            help="Path to configuration file"
        )
        
        # Subcommands
        subparsers = parser.add_subparsers(dest="command", help="Command")
        
        # Start command
        start_parser = subparsers.add_parser("start", help="Start services")
        start_parser.add_argument(
            "service",
            nargs="?",
            help="Service to start (all if not specified)"
        )
        
        # Stop command
        stop_parser = subparsers.add_parser("stop", help="Stop services")
        stop_parser.add_argument(
            "service",
            nargs="?",
            help="Service to stop (all if not specified)"
        )
        
        # Restart command
        restart_parser = subparsers.add_parser("restart", help="Restart services")
        restart_parser.add_argument(
            "service",
            nargs="?",
            help="Service to restart (all if not specified)"
        )
        
        # Status command
        status_parser = subparsers.add_parser("status", help="Get service status")
        status_parser.add_argument(
            "service",
            nargs="?",
            help="Service to get status for (all if not specified)"
        )
        
        # Log command
        log_parser = subparsers.add_parser("log", help="View service logs")
        log_parser.add_argument(
            "service",
            help="Service to view logs for"
        )
        log_parser.add_argument(
            "--lines",
            type=int,
            default=100,
            help="Number of lines to show"
        )
        log_parser.add_argument(
            "--follow",
            "-f",
            action="store_true",
            help="Follow log output"
        )
        
        # Create sierrachart_etl config
        create_parser = subparsers.add_parser(
            "create-etl-config",
            help="Create a configuration for the Sierra Chart ETL pipeline"
        )
        create_parser.add_argument(
            "--db-path",
            required=True,
            help="Path to the SQLite database"
        )
        create_parser.add_argument(
            "--sc-root",
            required=True,
            help="Path to Sierra Chart installation"
        )
        create_parser.add_argument(
            "--config-path",
            default="./config.json",
            help="Path to ETL configuration file"
        )
        create_parser.add_argument(
            "--api-port",
            type=int,
            default=8000,
            help="Port for the API server"
        )
        
        return parser.parse_args()
    
    def run(self):
        """Run the CLI"""
        args = self.parse_args()
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("service_manager.log"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Load configuration
        if args.command != "create-etl-config":
            self.service_manager.config_file = args.config
            if os.path.exists(args.config):
                self.service_manager.load_config(args.config)
        
        # Handle command
        if args.command == "start":
            if args.service:
                self.service_manager.start_service(args.service)
            else:
                self.service_manager.start_all()
        
        elif args.command == "stop":
            if args.service:
                self.service_manager.stop_service(args.service)
            else:
                self.service_manager.stop_all()
        
        elif args.command == "restart":
            if args.service:
                self.service_manager.restart_service(args.service)
            else:
                self.service_manager.stop_all()
                time.sleep(2)
                self.service_manager.start_all()
        
        elif args.command == "status":
            self.service_manager.start_manager()
            try:
                if args.service:
                    status = self.service_manager.get_service_status(args.service)
                    if status:
                        self._print_service_status(status)
                else:
                    status = self.service_manager.get_service_status()
                    self._print_all_services_status(status)
            finally:
                self.service_manager.stop_manager()
        
        elif args.command == "log":
            self._view_service_log(args.service, args.lines, args.follow)
        
        elif args.command == "create-etl-config":
            self._create_etl_config(
                db_path=args.db_path,
                sc_root=args.sc_root,
                config_path=args.config_path,
                api_port=args.api_port
            )
        
        # Save configuration if we made changes
        if args.command in ["start", "stop", "restart", "create-etl-config"]:
            self.service_manager.save_config()
    
    def _print_service_status(self, status):
        """Print status of a single service"""
        print(f"Service: {status['name']}")
        print(f"Status: {status['status']}")
        print(f"PID: {status['pid'] or 'N/A'}")
        print(f"Restart count: {status['restart_count']}")
        
        if status['last_start_time']:
            start_time = datetime.fromtimestamp(status['last_start_time']).strftime("%Y-%m-%d %H:%M:%S")
            print(f"Started: {start_time}")
        
        if status['uptime']:
            uptime = timedelta(seconds=int(status['uptime']))
            print(f"Uptime: {uptime}")
        
        if status['memory_usage']:
            print(f"Memory usage: {status['memory_usage']:.2f} MB")
        
        if status['cpu_usage']:
            print(f"CPU usage: {status['cpu_usage']:.2f}%")
        
        if status['exit_code'] is not None:
            print(f"Last exit code: {status['exit_code']}")
    
    def _print_all_services_status(self, status):
        """Print status of all services"""
        if not status:
            print("No services configured")
            return
        
        # Print as a table
        fmt = "{:<20} {:<10} {:<10} {:<15} {:<10} {:<10}"
        print(fmt.format("NAME", "STATUS", "PID", "UPTIME", "MEMORY", "CPU"))
        print("-" * 75)
        
        for name, service_status in status.items():
            uptime_str = "N/A"
            if service_status['uptime']:
                uptime = timedelta(seconds=int(service_status['uptime']))
                uptime_str = str(uptime)
            
            memory_str = "N/A"
            if service_status['memory_usage']:
                memory_str = f"{service_status['memory_usage']:.2f} MB"
            
            cpu_str = "N/A"
            if service_status['cpu_usage']:
                cpu_str = f"{service_status['cpu_usage']:.2f}%"
            
            print(fmt.format(
                name,
                service_status['status'],
                service_status['pid'] or "N/A",
                uptime_str,
                memory_str,
                cpu_str
            ))
    
    def _view_service_log(self, service_name, lines, follow):
        """View logs for a service"""
        log_path = f"logs/{service_name}.log"
        
        if not os.path.exists(log_path):
            print(f"Log file not found: {log_path}")
            return
        
        if follow:
            # Use tail to follow the log
            try:
                subprocess.run(["tail", "-n", str(lines), "-f", log_path])
            except KeyboardInterrupt:
                pass
        else:
            # Just display the last N lines
            try:
                with open(log_path, "r") as f:
                    all_lines = f.readlines()
                    for line in all_lines[-lines:]:
                        print(line, end="")
            except Exception as e:
                print(f"Error reading log file: {str(e)}")
    
    def _create_etl_config(self, db_path, sc_root, config_path, api_port):
        """Create a configuration for the Sierra Chart ETL pipeline"""
        # Create directories
        os.makedirs("logs", exist_ok=True)
        
        # Create ETL configuration if it doesn't exist
        if not os.path.exists(config_path):
            etl_config = {
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
                "batch_size": 1000,
                "maintenance_interval": 86400,  # Daily maintenance
                "backup_interval": 604800,  # Weekly backup
                "health_check_interval": 300  # 5-minute health checks
            }
            
            with open(config_path, 'w') as f:
                json.dump(etl_config, f, indent=2)
            
            logger.info(f"Created ETL configuration: {config_path}")
        
        # Create service configurations
        
        # ETL Service
        etl_service = ServiceConfig(
            name="sierrachart_etl",
            command=["python", "enhanced_etl.py", "1", "--config", config_path],
            working_dir=".",
            restart_policy="always",
            max_restarts=10,
            restart_delay=10,
            expected_log_pattern="Starting ETL pipeline",
            health_check_cmd=["python", "-c", f"import os; exit(0 if os.path.exists('{db_path}') else 1)"],
            health_check_interval=60
        )
        self.service_manager.add_service(etl_service)
        
        # API Service
        api_service = ServiceConfig(
            name="api_service",
            command=["python", "api_service.py", "--db-path", db_path, "--host", "0.0.0.0", "--port", str(api_port)],
            working_dir=".",
            restart_policy="always",
            max_restarts=10,
            restart_delay=10,
            expected_log_pattern="Application startup complete",
            health_check_cmd=["curl", "-s", f"http://localhost:{api_port}/health"],
            health_check_interval=30
        )
        self.service_manager.add_service(api_service)
        
        logger.info("Created ETL and API service configurations")
        print("Created Sierra Chart ETL configuration. You can now start the services with:")
        print("  python service_manager.py start")

if __name__ == "__main__":
    cli = ServiceManagerCLI()
    cli.run()