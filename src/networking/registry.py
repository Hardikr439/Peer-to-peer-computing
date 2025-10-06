"""
Node Registry for Peer-to-Peer Resource Discovery

This module implements the node registry that allows devices to register their
presence and discover other nodes in the network. It provides both in-memory
and file-based registry implementations.
"""

import json
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Callable
from abc import ABC, abstractmethod
import os
import sys

# Import fcntl only on Unix-like systems
try:
    import fcntl
    HAS_FCNTL = True
except ImportError:
    HAS_FCNTL = False

from .node_info import NodeInfo, NodeStatus, HeartbeatMessage, create_node_summary


class RegistryError(Exception):
    """Exception raised by registry operations."""
    pass


class NodeRegistry(ABC):
    """Abstract base class for node registries."""
    
    @abstractmethod
    def register_node(self, node: NodeInfo) -> bool:
        """
        Register a node in the registry.
        
        Args:
            node: NodeInfo instance to register
            
        Returns:
            True if registration was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def unregister_node(self, node_id: str) -> bool:
        """
        Unregister a node from the registry.
        
        Args:
            node_id: ID of the node to unregister
            
        Returns:
            True if unregistration was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def update_node(self, node_id: str, heartbeat: HeartbeatMessage) -> bool:
        """
        Update a node's information from a heartbeat.
        
        Args:
            node_id: ID of the node to update
            heartbeat: Heartbeat message with updated information
            
        Returns:
            True if update was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """
        Get information about a specific node.
        
        Args:
            node_id: ID of the node to retrieve
            
        Returns:
            NodeInfo instance if found, None otherwise
        """
        pass
    
    @abstractmethod
    def list_nodes(self, status_filter: Optional[NodeStatus] = None) -> List[NodeInfo]:
        """
        List all nodes in the registry.
        
        Args:
            status_filter: Optional status filter
            
        Returns:
            List of NodeInfo instances
        """
        pass
    
    @abstractmethod
    def cleanup_stale_nodes(self, max_age_seconds: int = 60) -> int:
        """
        Remove stale nodes from the registry.
        
        Args:
            max_age_seconds: Maximum age before considering a node stale
            
        Returns:
            Number of nodes removed
        """
        pass


class InMemoryRegistry(NodeRegistry):
    """In-memory implementation of the node registry."""
    
    def __init__(self):
        """Initialize the in-memory registry."""
        self._nodes: Dict[str, NodeInfo] = {}
        self._lock = threading.RLock()
        self._observers: List[Callable[[str, NodeInfo, str], None]] = []
    
    def add_observer(self, observer: Callable[[str, NodeInfo, str], None]):
        """
        Add an observer for registry events.
        
        Args:
            observer: Function that takes (event_type, node, node_id)
                     event_type can be: 'registered', 'updated', 'unregistered', 'stale_removed'
        """
        with self._lock:
            self._observers.append(observer)
    
    def remove_observer(self, observer: Callable[[str, NodeInfo, str], None]):
        """Remove an observer."""
        with self._lock:
            if observer in self._observers:
                self._observers.remove(observer)
    
    def _notify_observers(self, event_type: str, node: NodeInfo, node_id: str):
        """Notify all observers of an event."""
        for observer in self._observers:
            try:
                observer(event_type, node, node_id)
            except Exception:
                pass  # Don't let observer errors break the registry
    
    def register_node(self, node: NodeInfo) -> bool:
        """Register a node in the registry."""
        with self._lock:
            self._nodes[node.node_id] = node
            self._notify_observers('registered', node, node.node_id)
            return True
    
    def unregister_node(self, node_id: str) -> bool:
        """Unregister a node from the registry."""
        with self._lock:
            if node_id in self._nodes:
                node = self._nodes.pop(node_id)
                self._notify_observers('unregistered', node, node_id)
                return True
            return False
    
    def update_node(self, node_id: str, heartbeat: HeartbeatMessage) -> bool:
        """Update a node's information from a heartbeat."""
        with self._lock:
            if node_id in self._nodes:
                node = self._nodes[node_id]
                node.capacity = heartbeat.capacity
                node.status = heartbeat.status
                node.registered_functions = heartbeat.registered_functions.copy()
                node.update_last_seen()
                self._notify_observers('updated', node, node_id)
                return True
            return False
    
    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Get information about a specific node."""
        with self._lock:
            return self._nodes.get(node_id)
    
    def list_nodes(self, status_filter: Optional[NodeStatus] = None) -> List[NodeInfo]:
        """List all nodes in the registry."""
        with self._lock:
            if status_filter is None:
                return list(self._nodes.values())
            else:
                return [node for node in self._nodes.values() if node.status == status_filter]
    
    def cleanup_stale_nodes(self, max_age_seconds: int = 60) -> int:
        """Remove stale nodes from the registry."""
        removed_count = 0
        with self._lock:
            stale_nodes = []
            for node_id, node in self._nodes.items():
                if node.is_stale(max_age_seconds):
                    stale_nodes.append(node_id)
            
            for node_id in stale_nodes:
                node = self._nodes.pop(node_id)
                self._notify_observers('stale_removed', node, node_id)
                removed_count += 1
        
        return removed_count
    
    def get_statistics(self) -> Dict[str, any]:
        """Get registry statistics."""
        with self._lock:
            total_nodes = len(self._nodes)
            status_counts = {}
            for status in NodeStatus:
                status_counts[status.value] = len([n for n in self._nodes.values() if n.status == status])
            
            return {
                "total_nodes": total_nodes,
                "status_counts": status_counts,
                "node_ids": list(self._nodes.keys())
            }


class FileBasedRegistry(NodeRegistry):
    """File-based implementation of the node registry for persistence."""
    
    def __init__(self, registry_file: Path):
        """
        Initialize the file-based registry.
        
        Args:
            registry_file: Path to the registry file
        """
        self.registry_file = Path(registry_file)
        self._lock = threading.RLock()
        
        # Ensure the directory exists
        self.registry_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize the file if it doesn't exist
        if not self.registry_file.exists():
            self._write_registry_data({})
    
    def _read_registry_data(self) -> Dict[str, Dict]:
        """Read registry data from file with file locking."""
        try:
            with open(self.registry_file, 'r') as f:
                # Lock the file for reading (Unix only)
                if HAS_FCNTL:
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                
                content = f.read().strip()
                if not content:
                    return {}
                
                data = json.loads(content)
                return data
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        except Exception as e:
            raise RegistryError(f"Error reading registry file: {e}")
    
    def _write_registry_data(self, data: Dict[str, Dict]):
        """Write registry data to file with file locking."""
        try:
            with open(self.registry_file, 'w') as f:
                # Lock the file for writing (Unix only)
                if HAS_FCNTL:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            raise RegistryError(f"Error writing registry file: {e}")
    
    def register_node(self, node: NodeInfo) -> bool:
        """Register a node in the registry."""
        with self._lock:
            try:
                data = self._read_registry_data()
                data[node.node_id] = node.to_dict()
                self._write_registry_data(data)
                return True
            except Exception:
                return False
    
    def unregister_node(self, node_id: str) -> bool:
        """Unregister a node from the registry."""
        with self._lock:
            try:
                data = self._read_registry_data()
                if node_id in data:
                    del data[node_id]
                    self._write_registry_data(data)
                    return True
                return False
            except Exception:
                return False
    
    def update_node(self, node_id: str, heartbeat: HeartbeatMessage) -> bool:
        """Update a node's information from a heartbeat."""
        with self._lock:
            try:
                data = self._read_registry_data()
                if node_id in data:
                    node_dict = data[node_id]
                    node = NodeInfo.from_dict(node_dict)
                    
                    # Update with heartbeat data
                    node.capacity = heartbeat.capacity
                    node.status = heartbeat.status
                    node.registered_functions = heartbeat.registered_functions.copy()
                    node.update_last_seen()
                    
                    # Save back to registry
                    data[node_id] = node.to_dict()
                    self._write_registry_data(data)
                    return True
                return False
            except Exception:
                return False
    
    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Get information about a specific node."""
        with self._lock:
            try:
                data = self._read_registry_data()
                if node_id in data:
                    return NodeInfo.from_dict(data[node_id])
                return None
            except Exception:
                return None
    
    def list_nodes(self, status_filter: Optional[NodeStatus] = None) -> List[NodeInfo]:
        """List all nodes in the registry."""
        with self._lock:
            try:
                data = self._read_registry_data()
                nodes = [NodeInfo.from_dict(node_data) for node_data in data.values()]
                
                if status_filter is None:
                    return nodes
                else:
                    return [node for node in nodes if node.status == status_filter]
            except Exception:
                return []
    
    def cleanup_stale_nodes(self, max_age_seconds: int = 60) -> int:
        """Remove stale nodes from the registry."""
        with self._lock:
            try:
                data = self._read_registry_data()
                stale_node_ids = []
                
                for node_id, node_data in data.items():
                    node = NodeInfo.from_dict(node_data)
                    if node.is_stale(max_age_seconds):
                        stale_node_ids.append(node_id)
                
                for node_id in stale_node_ids:
                    del data[node_id]
                
                if stale_node_ids:
                    self._write_registry_data(data)
                
                return len(stale_node_ids)
            except Exception:
                return 0


class DistributedRegistry:
    """
    Distributed registry that combines local registry with discovery of other registries.
    
    This allows for a hybrid approach where nodes can register locally but also
    discover nodes from other registry instances.
    """
    
    def __init__(self, local_registry: NodeRegistry):
        """
        Initialize the distributed registry.
        
        Args:
            local_registry: Local registry instance to use
        """
        self.local_registry = local_registry
        self.remote_registries: Dict[str, str] = {}  # registry_id -> endpoint
        self._lock = threading.RLock()
    
    def add_remote_registry(self, registry_id: str, endpoint: str):
        """Add a remote registry to discover nodes from."""
        with self._lock:
            self.remote_registries[registry_id] = endpoint
    
    def remove_remote_registry(self, registry_id: str):
        """Remove a remote registry."""
        with self._lock:
            if registry_id in self.remote_registries:
                del self.remote_registries[registry_id]
    
    def get_all_nodes(self, status_filter: Optional[NodeStatus] = None) -> List[NodeInfo]:
        """
        Get all nodes from local and remote registries.
        
        Args:
            status_filter: Optional status filter
            
        Returns:
            List of all discovered nodes
        """
        # Start with local nodes
        all_nodes = self.local_registry.list_nodes(status_filter)
        
        # TODO: In a future increment, we'll add remote registry discovery
        # For now, we just return local nodes
        
        return all_nodes
    
    def register_node(self, node: NodeInfo) -> bool:
        """Register a node in the local registry."""
        return self.local_registry.register_node(node)
    
    def unregister_node(self, node_id: str) -> bool:
        """Unregister a node from the local registry."""
        return self.local_registry.unregister_node(node_id)
    
    def update_node(self, node_id: str, heartbeat: HeartbeatMessage) -> bool:
        """Update a node in the local registry."""
        return self.local_registry.update_node(node_id, heartbeat)
    
    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Get a node from local registry (and later, remote registries)."""
        return self.local_registry.get_node(node_id)
    
    def cleanup_stale_nodes(self, max_age_seconds: int = 60) -> int:
        """Cleanup stale nodes from the local registry."""
        return self.local_registry.cleanup_stale_nodes(max_age_seconds)


def create_registry(registry_type: str = "memory", **kwargs) -> NodeRegistry:
    """
    Factory function to create a registry instance.
    
    Args:
        registry_type: Type of registry ("memory" or "file")
        **kwargs: Additional arguments for registry creation
        
    Returns:
        NodeRegistry instance
    """
    if registry_type == "memory":
        return InMemoryRegistry()
    elif registry_type == "file":
        registry_file = kwargs.get("registry_file", "nodes.json")
        return FileBasedRegistry(Path(registry_file))
    else:
        raise ValueError(f"Unknown registry type: {registry_type}")


def registry_health_check(registry: NodeRegistry) -> Dict[str, any]:
    """
    Perform a health check on a registry.
    
    Args:
        registry: Registry to check
        
    Returns:
        Health check results
    """
    try:
        nodes = registry.list_nodes()
        online_nodes = registry.list_nodes(NodeStatus.ONLINE)
        
        return {
            "healthy": True,
            "total_nodes": len(nodes),
            "online_nodes": len(online_nodes),
            "offline_nodes": len(nodes) - len(online_nodes),
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "healthy": False,
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }