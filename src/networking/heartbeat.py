"""
Health Heartbeat System for Node Monitoring

This module implements the heartbeat system that allows nodes to send periodic
"I'm alive" messages and monitor the health of other nodes in the network.
"""

import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Callable, Dict, Any
from enum import Enum

from .node_info import NodeInfo, NodeStatus, HeartbeatMessage, SystemCapacity
from .registry import NodeRegistry


class HeartbeatStatus(Enum):
    """Heartbeat service status."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


class HeartbeatService:
    """
    Service that manages sending and receiving heartbeat messages.
    
    This service runs in the background and:
    1. Periodically sends heartbeat messages to the registry
    2. Monitors and cleans up stale nodes
    3. Provides callbacks for heartbeat events
    """
    
    def __init__(self, 
                 node: NodeInfo,
                 registry: NodeRegistry,
                 heartbeat_interval: float = 10.0,
                 cleanup_interval: float = 30.0,
                 node_timeout: float = 60.0):
        """
        Initialize the heartbeat service.
        
        Args:
            node: Local node information
            registry: Registry to send heartbeats to
            heartbeat_interval: Interval between heartbeats in seconds
            cleanup_interval: Interval between cleanup operations in seconds
            node_timeout: Time after which nodes are considered stale
        """
        self.node = node
        self.registry = registry
        self.heartbeat_interval = heartbeat_interval
        self.cleanup_interval = cleanup_interval
        self.node_timeout = node_timeout
        
        self._status = HeartbeatStatus.STOPPED
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._cleanup_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        
        # Callbacks
        self._heartbeat_sent_callbacks: list[Callable[[HeartbeatMessage], None]] = []
        self._node_discovered_callbacks: list[Callable[[NodeInfo], None]] = []
        self._node_lost_callbacks: list[Callable[[str], None]] = []
        
        # Statistics
        self._stats = {
            "heartbeats_sent": 0,
            "heartbeats_failed": 0,
            "nodes_cleaned": 0,
            "last_heartbeat": None,
            "last_cleanup": None,
            "start_time": None
        }
        
        # Logger
        self.logger = logging.getLogger(f"{__name__}.{node.node_id[:8]}")
    
    def add_heartbeat_sent_callback(self, callback: Callable[[HeartbeatMessage], None]):
        """Add a callback for when heartbeats are sent."""
        with self._lock:
            self._heartbeat_sent_callbacks.append(callback)
    
    def add_node_discovered_callback(self, callback: Callable[[NodeInfo], None]):
        """Add a callback for when new nodes are discovered."""
        with self._lock:
            self._node_discovered_callbacks.append(callback)
    
    def add_node_lost_callback(self, callback: Callable[[str], None]):
        """Add a callback for when nodes are lost (go stale)."""
        with self._lock:
            self._node_lost_callbacks.append(callback)
    
    def _notify_heartbeat_sent(self, heartbeat: HeartbeatMessage):
        """Notify callbacks that a heartbeat was sent."""
        for callback in self._heartbeat_sent_callbacks:
            try:
                callback(heartbeat)
            except Exception as e:
                self.logger.warning(f"Heartbeat sent callback error: {e}")
    
    def _notify_node_discovered(self, node: NodeInfo):
        """Notify callbacks that a node was discovered."""
        for callback in self._node_discovered_callbacks:
            try:
                callback(node)
            except Exception as e:
                self.logger.warning(f"Node discovered callback error: {e}")
    
    def _notify_node_lost(self, node_id: str):
        """Notify callbacks that a node was lost."""
        for callback in self._node_lost_callbacks:
            try:
                callback(node_id)
            except Exception as e:
                self.logger.warning(f"Node lost callback error: {e}")
    
    def start(self) -> bool:
        """
        Start the heartbeat service.
        
        Returns:
            True if started successfully, False otherwise
        """
        with self._lock:
            if self._status == HeartbeatStatus.RUNNING:
                self.logger.warning("Heartbeat service is already running")
                return True
            
            if self._status in [HeartbeatStatus.STARTING, HeartbeatStatus.STOPPING]:
                self.logger.warning(f"Heartbeat service is {self._status.value}, cannot start")
                return False
            
            try:
                self._status = HeartbeatStatus.STARTING
                self._stop_event.clear()
                
                # Register the node initially
                if not self.registry.register_node(self.node):
                    self.logger.error("Failed to register node in registry")
                    self._status = HeartbeatStatus.ERROR
                    return False
                
                # Start the heartbeat thread
                self._heartbeat_thread = threading.Thread(
                    target=self._heartbeat_loop,
                    name=f"heartbeat-{self.node.node_id[:8]}",
                    daemon=True
                )
                self._heartbeat_thread.start()
                
                # Start the cleanup thread
                self._cleanup_thread = threading.Thread(
                    target=self._cleanup_loop,
                    name=f"cleanup-{self.node.node_id[:8]}",
                    daemon=True
                )
                self._cleanup_thread.start()
                
                self._status = HeartbeatStatus.RUNNING
                self._stats["start_time"] = datetime.utcnow().isoformat()
                
                self.logger.info(f"Heartbeat service started for node {self.node.node_id[:8]}")
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to start heartbeat service: {e}")
                self._status = HeartbeatStatus.ERROR
                return False
    
    def stop(self, timeout: float = 5.0) -> bool:
        """
        Stop the heartbeat service.
        
        Args:
            timeout: Maximum time to wait for threads to stop
            
        Returns:
            True if stopped successfully, False otherwise
        """
        with self._lock:
            if self._status == HeartbeatStatus.STOPPED:
                return True
            
            if self._status != HeartbeatStatus.RUNNING:
                self.logger.warning(f"Cannot stop service in {self._status.value} state")
                return False
            
            try:
                self._status = HeartbeatStatus.STOPPING
                self._stop_event.set()
                
                # Wait for threads to stop
                if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                    self._heartbeat_thread.join(timeout)
                
                if self._cleanup_thread and self._cleanup_thread.is_alive():
                    self._cleanup_thread.join(timeout)
                
                # Unregister the node
                self.registry.unregister_node(self.node.node_id)
                
                self._status = HeartbeatStatus.STOPPED
                self.logger.info(f"Heartbeat service stopped for node {self.node.node_id[:8]}")
                return True
                
            except Exception as e:
                self.logger.error(f"Error stopping heartbeat service: {e}")
                self._status = HeartbeatStatus.ERROR
                return False
    
    def _heartbeat_loop(self):
        """Main heartbeat loop that runs in a separate thread."""
        self.logger.info("Heartbeat loop started")
        
        while not self._stop_event.is_set():
            try:
                # Update node capacity
                self.node.update_capacity()
                
                # Create heartbeat message
                heartbeat = HeartbeatMessage(
                    node_id=self.node.node_id,
                    timestamp=datetime.utcnow().isoformat(),
                    capacity=self.node.capacity,
                    status=self.node.status,
                    registered_functions=self.node.registered_functions.copy()
                )
                
                # Send heartbeat to registry
                if self.registry.update_node(self.node.node_id, heartbeat):
                    self._stats["heartbeats_sent"] += 1
                    self._stats["last_heartbeat"] = heartbeat.timestamp
                    self._notify_heartbeat_sent(heartbeat)
                    self.logger.debug(f"Heartbeat sent: {heartbeat.timestamp}")
                else:
                    self._stats["heartbeats_failed"] += 1
                    self.logger.warning("Failed to send heartbeat to registry")
                
            except Exception as e:
                self.logger.error(f"Error in heartbeat loop: {e}")
                self._stats["heartbeats_failed"] += 1
            
            # Wait for the next heartbeat interval
            self._stop_event.wait(self.heartbeat_interval)
        
        self.logger.info("Heartbeat loop stopped")
    
    def _cleanup_loop(self):
        """Cleanup loop that removes stale nodes."""
        self.logger.info("Cleanup loop started")
        
        while not self._stop_event.is_set():
            try:
                # Clean up stale nodes
                removed_count = self.registry.cleanup_stale_nodes(self.node_timeout)
                
                if removed_count > 0:
                    self._stats["nodes_cleaned"] += removed_count
                    self.logger.info(f"Cleaned up {removed_count} stale nodes")
                    
                    # Notify about lost nodes (simplified - in reality we'd track which ones)
                    for i in range(removed_count):
                        self._notify_node_lost(f"stale_node_{i}")
                
                self._stats["last_cleanup"] = datetime.utcnow().isoformat()
                
            except Exception as e:
                self.logger.error(f"Error in cleanup loop: {e}")
            
            # Wait for the next cleanup interval
            self._stop_event.wait(self.cleanup_interval)
        
        self.logger.info("Cleanup loop stopped")
    
    def get_status(self) -> HeartbeatStatus:
        """Get the current status of the heartbeat service."""
        return self._status
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get heartbeat service statistics."""
        with self._lock:
            stats = self._stats.copy()
            stats["status"] = self._status.value
            stats["node_id"] = self.node.node_id
            stats["heartbeat_interval"] = self.heartbeat_interval
            stats["cleanup_interval"] = self.cleanup_interval
            stats["node_timeout"] = self.node_timeout
            
            # Calculate uptime
            if stats["start_time"]:
                try:
                    start_time = datetime.fromisoformat(stats["start_time"])
                    uptime = datetime.utcnow() - start_time
                    stats["uptime_seconds"] = uptime.total_seconds()
                except Exception:
                    stats["uptime_seconds"] = 0
            else:
                stats["uptime_seconds"] = 0
            
            return stats
    
    def update_node_functions(self, function_names: list[str]):
        """
        Update the list of functions registered on this node.
        
        Args:
            function_names: List of function names available on this node
        """
        with self._lock:
            self.node.registered_functions = function_names.copy()
            self.logger.debug(f"Updated node functions: {function_names}")
    
    def update_node_status(self, status: NodeStatus):
        """
        Update the status of this node.
        
        Args:
            status: New status for the node
        """
        with self._lock:
            old_status = self.node.status
            self.node.status = status
            self.logger.info(f"Node status changed from {old_status.value} to {status.value}")


class HeartbeatMonitor:
    """
    Monitor for tracking heartbeats from multiple nodes.
    
    This class provides utilities for monitoring the health of nodes
    based on their heartbeat patterns.
    """
    
    def __init__(self, registry: NodeRegistry):
        """
        Initialize the heartbeat monitor.
        
        Args:
            registry: Registry to monitor
        """
        self.registry = registry
        self._node_histories: Dict[str, list[datetime]] = {}
        self._lock = threading.RLock()
    
    def record_heartbeat(self, node_id: str, timestamp: Optional[datetime] = None):
        """
        Record a heartbeat for a node.
        
        Args:
            node_id: ID of the node
            timestamp: Timestamp of the heartbeat (defaults to now)
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        with self._lock:
            if node_id not in self._node_histories:
                self._node_histories[node_id] = []
            
            self._node_histories[node_id].append(timestamp)
            
            # Keep only the last 100 heartbeats
            if len(self._node_histories[node_id]) > 100:
                self._node_histories[node_id] = self._node_histories[node_id][-100:]
    
    def get_node_health(self, node_id: str) -> Dict[str, Any]:
        """
        Get health information for a specific node.
        
        Args:
            node_id: ID of the node
            
        Returns:
            Dictionary with health information
        """
        with self._lock:
            if node_id not in self._node_histories:
                return {"status": "unknown", "heartbeat_count": 0}
            
            heartbeats = self._node_histories[node_id]
            if not heartbeats:
                return {"status": "unknown", "heartbeat_count": 0}
            
            now = datetime.utcnow()
            last_heartbeat = heartbeats[-1]
            time_since_last = now - last_heartbeat
            
            # Count recent heartbeats (last 5 minutes)
            recent_threshold = now - timedelta(minutes=5)
            recent_heartbeats = [hb for hb in heartbeats if hb > recent_threshold]
            
            # Determine health status
            if time_since_last.total_seconds() < 30:
                status = "healthy"
            elif time_since_last.total_seconds() < 60:
                status = "warning"
            else:
                status = "unhealthy"
            
            return {
                "status": status,
                "heartbeat_count": len(heartbeats),
                "recent_heartbeats": len(recent_heartbeats),
                "last_heartbeat": last_heartbeat.isoformat(),
                "seconds_since_last": time_since_last.total_seconds()
            }
    
    def get_overall_health(self) -> Dict[str, Any]:
        """Get overall health statistics for all monitored nodes."""
        with self._lock:
            total_nodes = len(self._node_histories)
            if total_nodes == 0:
                return {"total_nodes": 0, "healthy": 0, "warning": 0, "unhealthy": 0}
            
            health_counts = {"healthy": 0, "warning": 0, "unhealthy": 0, "unknown": 0}
            
            for node_id in self._node_histories:
                health = self.get_node_health(node_id)
                status = health["status"]
                health_counts[status] += 1
            
            return {
                "total_nodes": total_nodes,
                **health_counts,
                "health_percentage": health_counts["healthy"] / total_nodes * 100 if total_nodes > 0 else 0
            }