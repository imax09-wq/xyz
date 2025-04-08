#!/usr/bin/env python3
"""
Monitoring and alerting system to track system health.

Part of the Sierra Chart ETL Pipeline project.
"""



import logging
import time
import json
import os
import socket
import threading
import queue
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Set, Union
import psutil
import requests

from error_handling import ETLError, ErrorCategory, create_error_context

logger = logging.getLogger("SierraChartETL.monitoring")

class MetricsCollector:
    """
    Collects system and application metrics for monitoring.
    """
    def __init__(self, metrics_dir: str = "./metrics"):
        self.metrics_dir = metrics_dir
        self.current_metrics = {
            "system": {},
            "database": {},
            "etl": {},
            "time": time.time()
        }
        self.metrics_history = []
        self.max_history_size = 1440  # Store metrics for 24 hours at 1-minute intervals
        
        # Create metrics directory if it doesn't exist
        os.makedirs(metrics_dir, exist_ok=True)
    
    def collect_system_metrics(self) -> Dict[str, Any]:
        """Collect system-level metrics (CPU, memory, disk, etc.)"""
        metrics = {}
        
        # CPU usage
        metrics["cpu_percent"] = psutil.cpu_percent(interval=0.1)
        metrics["cpu_count"] = psutil.cpu_count()
        
        # Memory usage
        memory = psutil.virtual_memory()
        metrics["memory_total"] = memory.total
        metrics["memory_available"] = memory.available
        metrics["memory_used"] = memory.used
        metrics["memory_percent"] = memory.percent
        
        # Disk usage for the current directory
        disk = psutil.disk_usage(".")
        metrics["disk_total"] = disk.total
        metrics["disk_free"] = disk.free
        metrics["disk_used"] = disk.used
        metrics["disk_percent"] = disk.percent
        
        # Network stats
        network = psutil.net_io_counters()
        metrics["net_bytes_sent"] = network.bytes_sent
        metrics["net_bytes_recv"] = network.bytes_recv
        
        # Process stats
        process = psutil.Process(os.getpid())
        metrics["process_cpu_percent"] = process.cpu_percent(interval=0.1)
        metrics["process_memory_rss"] = process.memory_info().rss
        metrics["process_memory_vms"] = process.memory_info().vms
        metrics["process_threads"] = process.num_threads()
        metrics["process_open_files"] = len(process.open_files())
        
        return metrics
    
    def collect_database_metrics(self, db_path: str) -> Dict[str, Any]:
        """Collect database-specific metrics"""
        metrics = {}
        
        try:
            # Basic file metrics
            if os.path.exists(db_path):
                metrics["db_file_size"] = os.path.getsize(db_path)
                metrics["db_last_modified"] = os.path.getmtime(db_path)
            else:
                metrics["db_file_exists"] = False
                return metrics
            
            # Database queries for more detailed metrics
            conn = sqlite3.connect(db_path)
            
            # Get WAL file size if it exists
            wal_path = f"{db_path}-wal"
            if os.path.exists(wal_path):
                metrics["wal_file_size"] = os.path.getsize(wal_path)
            
            # Get table statistics
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            tables = [row[0] for row in cursor.fetchall()]
            
            table_stats = {}
            for table in tables:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()[0]
                
                table_stats[table] = {
                    "row_count": row_count
                }
                
                # Try to get min/max timestamp if available (assumes a timestamp column)
                try:
                    cursor = conn.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM {table}")
                    min_ts, max_ts = cursor.fetchone()
                    
                    if min_ts is not None and max_ts is not None:
                        table_stats[table]["min_timestamp"] = min_ts
                        table_stats[table]["max_timestamp"] = max_ts
                        table_stats[table]["time_span_hours"] = (max_ts - min_ts) / (3600 * 1000000)  # Convert to hours
                except:
                    pass  # Not all tables may have a timestamp column
            
            metrics["tables"] = table_stats
            
            # Get database stats
            cursor = conn.execute("PRAGMA page_count")
            metrics["page_count"] = cursor.fetchone()[0]
            
            cursor = conn.execute("PRAGMA page_size")
            metrics["page_size"] = cursor.fetchone()[0]
            
            # Calculate theoretical db size
            metrics["theoretical_size"] = metrics["page_count"] * metrics["page_size"]
            
            # Get fragmentation estimate
            if metrics["theoretical_size"] > 0:
                metrics["fragmentation_percent"] = (1 - (metrics["db_file_size"] / metrics["theoretical_size"])) * 100
            
            conn.close()
            
            return metrics
        
        except sqlite3.Error as e:
            logger.error(f"Error collecting database metrics: {str(e)}")
            metrics["error"] = str(e)
            return metrics
    
    def collect_etl_metrics(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Collect ETL pipeline metrics from the stats dictionary"""
        metrics = {}
        
        # Copy provided stats
        for key, value in stats.items():
            metrics[key] = value
        
        # Add derived metrics
        if "start_time" in stats:
            metrics["uptime_seconds"] = time.time() - stats["start_time"]
            
            if "tas_records_processed" in stats and metrics["uptime_seconds"] > 0:
                metrics["tas_records_per_second"] = stats["tas_records_processed"] / metrics["uptime_seconds"]
            
            if "depth_records_processed" in stats and metrics["uptime_seconds"] > 0:
                metrics["depth_records_per_second"] = stats["depth_records_processed"] / metrics["uptime_seconds"]
        
        return metrics
    
    def collect_all_metrics(self, db_path: str, etl_stats: Dict[str, Any]) -> Dict[str, Any]:
        """Collect all metrics"""
        metrics = {
            "system": self.collect_system_metrics(),
            "database": self.collect_database_metrics(db_path),
            "etl": self.collect_etl_metrics(etl_stats),
            "time": time.time()
        }
        
        # Update current metrics
        self.current_metrics = metrics
        
        # Add to history and prune if needed
        self.metrics_history.append(metrics)
        if len(self.metrics_history) > self.max_history_size:
            self.metrics_history = self.metrics_history[-self.max_history_size:]
        
        return metrics
    
    def save_metrics(self, metrics: Dict[str, Any], filename: Optional[str] = None) -> str:
        """
        Save metrics to a file
        
        Args:
            metrics: The metrics to save
            filename: Optional filename, defaults to a timestamp-based name
            
        Returns:
            Path to the saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"metrics_{timestamp}.json"
        
        file_path = os.path.join(self.metrics_dir, filename)
        
        with open(file_path, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        return file_path
    
    def get_latest_metrics(self) -> Dict[str, Any]:
        """Get the most recently collected metrics"""
        return self.current_metrics
    
    def get_metrics_history(self) -> List[Dict[str, Any]]:
        """Get historical metrics"""
        return self.metrics_history

class Alert:
    """Represents a monitoring alert"""
    def __init__(
        self,
        alert_id: str,
        severity: str,
        message: str,
        source: str,
        timestamp: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        self.alert_id = alert_id
        self.severity = severity
        self.message = message
        self.source = source
        self.timestamp = timestamp if timestamp is not None else time.time()
        self.details = details if details is not None else {}
        self.resolved = False
        self.resolved_timestamp = None
    
    def resolve(self) -> None:
        """Mark the alert as resolved"""
        self.resolved = True
        self.resolved_timestamp = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "alert_id": self.alert_id,
            "severity": self.severity,
            "message": self.message,
            "source": self.source,
            "timestamp": self.timestamp,
            "details": self.details,
            "resolved": self.resolved,
            "resolved_timestamp": self.resolved_timestamp
        }

class AlertRule:
    """Rule for generating alerts based on metrics"""
    def __init__(
        self,
        rule_id: str,
        name: str,
        description: str,
        severity: str,
        check_function
    ):
        self.rule_id = rule_id
        self.name = name
        self.description = description
        self.severity = severity
        self.check_function = check_function
        self.last_triggered = None
        self.enabled = True
    
    def check(self, metrics: Dict[str, Any]) -> Optional[Alert]:
        """
        Check if the rule is triggered by current metrics
        
        Args:
            metrics: Current system metrics
            
        Returns:
            Alert if triggered, None otherwise
        """
        if not self.enabled:
            return None
        
        result = self.check_function(metrics)
        if result:
            alert_id = f"{self.rule_id}_{int(time.time())}"
            alert = Alert(
                alert_id=alert_id,
                severity=self.severity,
                message=result,
                source=self.name,
                details={"rule_id": self.rule_id}
            )
            self.last_triggered = time.time()
            return alert
        
        return None

class AlertManager:
    """
    Manages monitoring alerts and notifications
    """
    def __init__(self, alerts_dir: str = "./alerts"):
        self.alerts_dir = alerts_dir
        self.rules = []
        self.active_alerts = {}  # alert_id -> Alert
        self.resolved_alerts = {}  # alert_id -> Alert
        self.alert_history = []  # List of all alerts
        
        # Notification channels
        self.email_config = None
        self.webhook_urls = []
        
        # Create alerts directory if it doesn't exist
        os.makedirs(alerts_dir, exist_ok=True)
        
        # Define default rules
        self._add_default_rules()
    
    def _add_default_rules(self) -> None:
        """Add default monitoring rules"""
        # System rules
        self.add_rule(
            rule_id="high_cpu_usage",
            name="High CPU Usage",
            description="Triggers when CPU usage exceeds 90% for more than 5 minutes",
            severity="warning",
            check_function=lambda metrics: f"High CPU usage: {metrics['system']['cpu_percent']}%" 
                if metrics['system'].get('cpu_percent', 0) > 90 else None
        )
        
        self.add_rule(
            rule_id="high_memory_usage",
            name="High Memory Usage",
            description="Triggers when memory usage exceeds 90%",
            severity="warning",
            check_function=lambda metrics: f"High memory usage: {metrics['system']['memory_percent']}%" 
                if metrics['system'].get('memory_percent', 0) > 90 else None
        )
        
        self.add_rule(
            rule_id="low_disk_space",
            name="Low Disk Space",
            description="Triggers when free disk space falls below 10%",
            severity="critical",
            check_function=lambda metrics: f"Low disk space: {100 - metrics['system']['disk_percent']}% free" 
                if metrics['system'].get('disk_percent', 0) > 90 else None
        )
        
        # ETL rules
        self.add_rule(
            rule_id="high_error_rate",
            name="High Error Rate",
            description="Triggers when error count exceeds threshold",
            severity="critical",
            check_function=lambda metrics: f"High error rate: {metrics['etl'].get('errors', 0)} errors" 
                if metrics['etl'].get('errors', 0) > 10 else None
        )
        
        self.add_rule(
            rule_id="etl_processing_stalled",
            name="ETL Processing Stalled",
            description="Triggers when no new records are processed for a long time",
            severity="critical",
            check_function=lambda metrics: 
                f"ETL processing stalled: No new records in {(time.time() - metrics['etl'].get('last_checkpoint_time', 0)) / 60:.1f} minutes" 
                if (time.time() - metrics['etl'].get('last_checkpoint_time', time.time())) > 900 else None  # 15 minutes
        )
        
        # Database rules
        self.add_rule(
            rule_id="database_size_growing_rapidly",
            name="Database Size Growing Rapidly",
            description="Triggers when database size increases by more than 25% in an hour",
            severity="warning",
            check_function=self._check_db_growth
        )
    
    def _check_db_growth(self, metrics: Dict[str, Any]) -> Optional[str]:
        """Check for rapid database growth"""
        current_size = metrics['database'].get('db_file_size', 0)
        
        # Need at least 2 historical metrics to compare
        if len(self.metrics_collector.metrics_history) < 2:
            return None
        
        # Find a metrics point from about an hour ago
        one_hour_ago = time.time() - 3600
        closest_metrics = None
        
        for m in self.metrics_collector.metrics_history:
            if m['time'] <= one_hour_ago:
                closest_metrics = m
                break
        
        if not closest_metrics:
            return None
        
        old_size = closest_metrics['database'].get('db_file_size', current_size)
        
        # Avoid division by zero
        if old_size == 0:
            return None
        
        growth_pct = ((current_size - old_size) / old_size) * 100
        
        if growth_pct > 25:
            return f"Database growing rapidly: {growth_pct:.1f}% increase in the last hour"
        
        return None
    
    def set_metrics_collector(self, collector: MetricsCollector) -> None:
        """Set the metrics collector for accessing historical metrics"""
        self.metrics_collector = collector
    
    def add_rule(
        self,
        rule_id: str,
        name: str,
        description: str,
        severity: str,
        check_function
    ) -> None:
        """Add a new alert rule"""
        rule = AlertRule(
            rule_id=rule_id,
            name=name,
            description=description,
            severity=severity,
            check_function=check_function
        )
        self.rules.append(rule)
    
    def configure_email(
        self,
        smtp_server: str,
        smtp_port: int,
        username: str,
        password: str,
        sender: str,
        recipients: List[str]
    ) -> None:
        """Configure email notifications"""
        self.email_config = {
            "smtp_server": smtp_server,
            "smtp_port": smtp_port,
            "username": username,
            "password": password,
            "sender": sender,
            "recipients": recipients
        }
    
    def add_webhook(self, url: str) -> None:
        """Add a webhook URL for notifications"""
        self.webhook_urls.append(url)
    
    def check_rules(self, metrics: Dict[str, Any]) -> List[Alert]:
        """
        Check all rules against current metrics
        
        Args:
            metrics: Current system metrics
            
        Returns:
            List of new alerts triggered
        """
        new_alerts = []
        
        for rule in self.rules:
            alert = rule.check(metrics)
            if alert:
                self.active_alerts[alert.alert_id] = alert
                self.alert_history.append(alert)
                new_alerts.append(alert)
        
        return new_alerts
    
    def resolve_alert(self, alert_id: str) -> bool:
        """
        Resolve an active alert
        
        Args:
            alert_id: ID of the alert to resolve
            
        Returns:
            True if the alert was resolved, False if not found
        """
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.resolve()
            
            # Move to resolved alerts
            self.resolved_alerts[alert_id] = alert
            del self.active_alerts[alert_id]
            
            return True
        
        return False
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts"""
        return list(self.active_alerts.values())
    
    def get_resolved_alerts(self, limit: int = 100) -> List[Alert]:
        """Get resolved alerts, up to a limit"""
        alerts = list(self.resolved_alerts.values())
        alerts.sort(key=lambda a: a.timestamp, reverse=True)
        return alerts[:limit]
    
    def save_alerts(self) -> str:
        """
        Save all alerts to a file
        
        Returns:
            Path to the saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"alerts_{timestamp}.json"
        file_path = os.path.join(self.alerts_dir, filename)
        
        all_alerts = {
            "active": [alert.to_dict() for alert in self.active_alerts.values()],
            "resolved": [alert.to_dict() for alert in self.resolved_alerts.values()]
        }
        
        with open(file_path, 'w') as f:
            json.dump(all_alerts, f, indent=2)
        
        return file_path
    
    def send_email_notification(self, alert: Alert) -> bool:
        """
        Send an email notification for an alert
        
        Args:
            alert: The alert to send a notification for
            
        Returns:
            True if the email was sent successfully, False otherwise
        """
        if not self.email_config:
            logger.warning("Email not configured, cannot send notification")
            return False
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['sender']
            msg['To'] = ', '.join(self.email_config['recipients'])
            msg['Subject'] = f"[{alert.severity.upper()}] Sierra Chart ETL Alert: {alert.message}"
            
            body = f"""
            Alert Details:
            --------------
            Severity: {alert.severity}
            Source: {alert.source}
            Message: {alert.message}
            Time: {datetime.fromtimestamp(alert.timestamp).strftime('%Y-%m-%d %H:%M:%S')}
            
            Additional Details:
            ------------------
            {json.dumps(alert.details, indent=2)}
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['username'], self.email_config['password'])
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email notification sent for alert {alert.alert_id}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to send email notification: {str(e)}")
            return False
    
    def send_webhook_notification(self, alert: Alert) -> bool:
        """
        Send a webhook notification for an alert
        
        Args:
            alert: The alert to send a notification for
            
        Returns:
            True if at least one webhook notification was sent successfully
        """
        if not self.webhook_urls:
            logger.warning("No webhooks configured, cannot send notification")
            return False
        
        payload = {
            "alert": alert.to_dict(),
            "timestamp": time.time()
        }
        
        success = False
        
        for url in self.webhook_urls:
            try:
                response = requests.post(
                    url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=10
                )
                
                if response.status_code >= 200 and response.status_code < 300:
                    logger.info(f"Webhook notification sent to {url} for alert {alert.alert_id}")
                    success = True
                else:
                    logger.warning(
                        f"Webhook notification failed for {url}: HTTP {response.status_code}"
                    )
            
            except Exception as e:
                logger.error(f"Failed to send webhook notification to {url}: {str(e)}")
        
        return success
    
    def send_notifications(self, alerts: List[Alert]) -> None:
        """
        Send notifications for multiple alerts
        
        Args:
            alerts: List of alerts to send notifications for
        """
        for alert in alerts:
            # Only send notifications for critical alerts or if there are few alerts
            if alert.severity == "critical" or len(alerts) <= 3:
                self.send_email_notification(alert)
                self.send_webhook_notification(alert)

class MonitoringServer:
    """
    Background server for monitoring the ETL pipeline
    """
    def __init__(
        self,
        db_path: str,
        metrics_interval: int = 60,  # seconds
        alerts_check_interval: int = 300,  # seconds
        metrics_dir: str = "./metrics",
        alerts_dir: str = "./alerts"
    ):
        self.db_path = db_path
        self.metrics_interval = metrics_interval
        self.alerts_check_interval = alerts_check_interval
        
        # Initialize components
        self.metrics_collector = MetricsCollector(metrics_dir)
        self.alert_manager = AlertManager(alerts_dir)
        self.alert_manager.set_metrics_collector(self.metrics_collector)
        
        # ETL stats reference (will be updated by the ETL pipeline)
        self.etl_stats = {}
        
        # Control flags
        self.running = False
        self.metrics_thread = None
        self.alerts_thread = None
    
    def set_etl_stats(self, stats: Dict[str, Any]) -> None:
        """Set the ETL stats reference"""
        self.etl_stats = stats
    
    def start(self) -> None:
        """Start the monitoring server"""
        if self.running:
            logger.warning("Monitoring server is already running")
            return
        
        self.running = True
        
        # Start metrics collection thread
        self.metrics_thread = threading.Thread(
            target=self._metrics_loop,
            daemon=True
        )
        self.metrics_thread.start()
        
        # Start alerts check thread
        self.alerts_thread = threading.Thread(
            target=self._alerts_loop,
            daemon=True
        )
        self.alerts_thread.start()
        
        logger.info("Monitoring server started")
    
    def stop(self) -> None:
        """Stop the monitoring server"""
        if not self.running:
            return
        
        self.running = False
        
        # Wait for threads to complete
        if self.metrics_thread:
            self.metrics_thread.join(timeout=5)
        
        if self.alerts_thread:
            self.alerts_thread.join(timeout=5)
        
        # Save final state
        self.metrics_collector.save_metrics(
            self.metrics_collector.get_latest_metrics(),
            "final_metrics.json"
        )
        self.alert_manager.save_alerts()
        
        logger.info("Monitoring server stopped")
    
    def _metrics_loop(self) -> None:
        """Background thread for collecting metrics"""
        last_save = time.time()
        save_interval = 3600  # Save metrics to disk every hour
        
        while self.running:
            try:
                # Collect metrics
                metrics = self.metrics_collector.collect_all_metrics(
                    self.db_path,
                    self.etl_stats
                )
                
                # Save metrics periodically
                current_time = time.time()
                if current_time - last_save >= save_interval:
                    self.metrics_collector.save_metrics(metrics)
                    last_save = current_time
                
                # Sleep until next collection
                time.sleep(self.metrics_interval)
            
            except Exception as e:
                logger.error(f"Error in metrics collection: {str(e)}")
                time.sleep(self.metrics_interval)
    
    def _alerts_loop(self) -> None:
        """Background thread for checking alerts"""
        while self.running:
            try:
                # Get latest metrics
                metrics = self.metrics_collector.get_latest_metrics()
                
                # Check alert rules
                new_alerts = self.alert_manager.check_rules(metrics)
                
                # Send notifications for new alerts
                if new_alerts:
                    self.alert_manager.send_notifications(new_alerts)
                
                # Sleep until next check
                time.sleep(self.alerts_check_interval)
            
            except Exception as e:
                logger.error(f"Error in alerts check: {str(e)}")
                time.sleep(self.alerts_check_interval)
    
    def configure_email_alerts(
        self,
        smtp_server: str,
        smtp_port: int,
        username: str,
        password: str,
        sender: str,
        recipients: List[str]
    ) -> None:
        """Configure email alerts"""
        self.alert_manager.configure_email(
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            username=username,
            password=password,
            sender=sender,
            recipients=recipients
        )
    
    def add_webhook(self, url: str) -> None:
        """Add a webhook URL for notifications"""
        self.alert_manager.add_webhook(url)
    
    def get_active_alerts(self) -> List[Dict[str, Any]]:
        """Get all active alerts"""
        return [alert.to_dict() for alert in self.alert_manager.get_active_alerts()]
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get overall system status"""
        metrics = self.metrics_collector.get_latest_metrics()
        active_alerts = self.alert_manager.get_active_alerts()
        
        # Determine overall status
        if any(alert.severity == "critical" for alert in active_alerts):
            status = "critical"
        elif any(alert.severity == "warning" for alert in active_alerts):
            status = "warning"
        else:
            status = "ok"
        
        # Calculate uptime
        uptime = time.time() - metrics['etl'].get('start_time', time.time())
        
        return {
            "status": status,
            "active_alerts_count": len(active_alerts),
            "uptime_seconds": uptime,
            "last_metrics_time": metrics['time'],
            "processing_rate": {
                "tas_records_per_second": metrics['etl'].get('tas_records_per_second', 0),
                "depth_records_per_second": metrics['etl'].get('depth_records_per_second', 0)
            },
            "system_resources": {
                "cpu_percent": metrics['system'].get('cpu_percent', 0),
                "memory_percent": metrics['system'].get('memory_percent', 0),
                "disk_percent": metrics['system'].get('disk_percent', 0)
            }
        }

if __name__ == "__main__":
    # Example usage
    import argparse
    import signal
    import sys
    
    parser = argparse.ArgumentParser(description="Sierra Chart ETL Monitoring")
    parser.add_argument("db_path", help="Path to the database file")
    parser.add_argument("--metrics-interval", type=int, default=60, help="Metrics collection interval in seconds")
    parser.add_argument("--alerts-interval", type=int, default=300, help="Alerts check interval in seconds")
    
    args = parser.parse_args()
    
    # Initialize monitoring server
    monitor = MonitoringServer(
        db_path=args.db_path,
        metrics_interval=args.metrics_interval,
        alerts_check_interval=args.alerts_interval
    )
    
    # Set up signal handler for graceful shutdown
    def signal_handler(sig, frame):
        print("Shutting down monitoring server...")
        monitor.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start monitoring
    monitor.start()
    
    print("Monitoring server running. Press Ctrl+C to stop.")
    
    # Keep main thread alive
    while True:
        time.sleep(1)