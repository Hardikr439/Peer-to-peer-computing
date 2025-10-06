"""
Node Information and Registry Data Structures

This module defines the data structures for representing nodes in the distributed
function runtime network, including their capabilities, status, and metadata.
"""

import json
import socket
import psutil
import platform
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from enum import Enum


class NodeStatus(Enum):
    """Node status enumeration."""
    ONLINE = "online"
    OFFLINE = "offline"
    BUSY = "busy"
    MAINTENANCE = "maintenance"


class NodeRole(Enum):
    """Node role enumeration."""
    WORKER = "worker"           # Can execute functions
    COORDINATOR = "coordinator" # Can coordinate execution
    HYBRID = "hybrid"          # Can do both


@dataclass
class SystemCapacity:
    """System capacity and resource information."""
    cpu_count: int = 0
    cpu_percent: float = 0.0
    memory_total_gb: float = 0.0
    memory_available_gb: float = 0.0
    memory_percent: float = 0.0
    disk_total_gb: float = 0.0
    disk_available_gb: float = 0.0
    disk_percent: float = 0.0
    
    def __post_init__(self):
        """Auto-populate with current system information if not provided."""
        if self.cpu_count == 0:
            self.update_from_system()
    
    def update_from_system(self):
        """Update capacity information from current system state."""
        self.cpu_count = psutil.cpu_count()
        self.cpu_percent = psutil.cpu_percent(interval=0.1)
        
        memory = psutil.virtual_memory()
        self.memory_total_gb = memory.total / (1024**3)
        self.memory_available_gb = memory.available / (1024**3)
        self.memory_percent = memory.percent
        
        disk = psutil.disk_usage('/')
        self.disk_total_gb = disk.total / (1024**3)
        self.disk_available_gb = disk.free / (1024**3)
        self.disk_percent = (disk.used / disk.total) * 100
    
    def get_load_score(self) -> float:
        """
        Calculate a normalized load score (0.0 = no load, 1.0 = fully loaded).
        
        Returns:
            Load score between 0.0 and 1.0
        """
        cpu_load = self.cpu_percent / 100.0
        memory_load = self.memory_percent / 100.0
        disk_load = self.disk_percent / 100.0
        
        # Weighted average (CPU and memory are more important than disk)
        return (cpu_load * 0.5 + memory_load * 0.4 + disk_load * 0.1)
    
    def can_accept_work(self, cpu_threshold: float = 80.0, memory_threshold: float = 90.0) -> bool:
        """
        Check if the node can accept new work based on resource thresholds.
        
        Args:
            cpu_threshold: CPU usage threshold percentage
            memory_threshold: Memory usage threshold percentage
            
        Returns:
            True if node can accept work, False otherwise
        """
        return (self.cpu_percent < cpu_threshold and 
                self.memory_percent < memory_threshold)


@dataclass
class NodeInfo:
    """Complete information about a node in the network."""
    node_id: str
    hostname: str
    ip_address: str
    port: int
    status: NodeStatus = NodeStatus.ONLINE
    role: NodeRole = NodeRole.HYBRID
    capacity: SystemCapacity = field(default_factory=SystemCapacity)
    last_seen: str = ""
    registered_functions: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize fields that depend on other fields."""
        if not self.last_seen:
            self.last_seen = datetime.utcnow().isoformat()
        
        if not self.metadata:
            self.metadata = {
                "platform": platform.system(),
                "platform_version": platform.version(),
                "python_version": platform.python_version(),
                "architecture": platform.machine()
            }
    
    def update_last_seen(self):
        """Update the last seen timestamp to now."""
        self.last_seen = datetime.utcnow().isoformat()
    
    def update_capacity(self):
        """Update the capacity information from current system state."""
        self.capacity.update_from_system()
        self.update_last_seen()
    
    def is_stale(self, max_age_seconds: int = 60) -> bool:
        """
        Check if this node information is stale.
        
        Args:
            max_age_seconds: Maximum age in seconds before considering stale
            
        Returns:
            True if the node info is stale, False otherwise
        """
        try:
            last_seen_time = datetime.fromisoformat(self.last_seen)
            age = datetime.utcnow() - last_seen_time
            return age.total_seconds() > max_age_seconds
        except (ValueError, TypeError):
            return True
    
    def get_endpoint(self) -> str:
        """Get the network endpoint for this node."""
        return f"{self.ip_address}:{self.port}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = asdict(self)
        # Convert enums to their values
        result['status'] = self.status.value
        result['role'] = self.role.value
        return result
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str, indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeInfo':
        """Create NodeInfo from dictionary."""
        # Convert enum values back to enums
        if 'status' in data:
            data['status'] = NodeStatus(data['status'])
        if 'role' in data:
            data['role'] = NodeRole(data['role'])
        
        # Handle capacity field
        if 'capacity' in data and isinstance(data['capacity'], dict):
            data['capacity'] = SystemCapacity(**data['capacity'])
        
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'NodeInfo':
        """Create NodeInfo from JSON string."""
        return cls.from_dict(json.loads(json_str))
    
    @classmethod
    def create_local_node(cls, 
                         port: int,
                         node_id: Optional[str] = None,
                         role: NodeRole = NodeRole.HYBRID) -> 'NodeInfo':
        """
        Create a NodeInfo instance for the local machine.
        
        Args:
            port: Port number for the node
            node_id: Optional node ID (generates one if not provided)
            role: Role of this node
            
        Returns:
            NodeInfo instance for the local machine
        """
        if node_id is None:
            node_id = str(uuid.uuid4())
        
        # Get local IP address
        try:
            # Create a socket to determine the local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))  # Connect to Google DNS
            local_ip = s.getsockname()[0]
            s.close()
        except Exception:
            local_ip = "127.0.0.1"
        
        hostname = socket.gethostname()
        
        return cls(
            node_id=node_id,
            hostname=hostname,
            ip_address=local_ip,
            port=port,
            status=NodeStatus.ONLINE,
            role=role,
            capacity=SystemCapacity()
        )


@dataclass
class HeartbeatMessage:
    """Heartbeat message sent by nodes to indicate they're alive."""
    node_id: str
    timestamp: str
    capacity: SystemCapacity
    status: NodeStatus = NodeStatus.ONLINE
    registered_functions: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = asdict(self)
        result['status'] = self.status.value
        return result
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HeartbeatMessage':
        """Create HeartbeatMessage from dictionary."""
        if 'status' in data:
            data['status'] = NodeStatus(data['status'])
        if 'capacity' in data and isinstance(data['capacity'], dict):
            data['capacity'] = SystemCapacity(**data['capacity'])
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'HeartbeatMessage':
        """Create HeartbeatMessage from JSON string."""
        return cls.from_dict(json.loads(json_str))


class NetworkAddress:
    """Utility class for handling network addresses."""
    
    @staticmethod
    def get_local_ip() -> str:
        """Get the local IP address."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except Exception:
            return "127.0.0.1"
    
    @staticmethod
    def is_port_available(host: str, port: int) -> bool:
        """Check if a port is available on the given host."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            result = s.connect_ex((host, port))
            s.close()
            return result != 0  # Port is available if connection failed
        except Exception:
            return False
    
    @staticmethod
    def find_available_port(host: str = "localhost", start_port: int = 8000, max_attempts: int = 100) -> int:
        """Find an available port starting from start_port."""
        for port in range(start_port, start_port + max_attempts):
            if NetworkAddress.is_port_available(host, port):
                return port
        raise RuntimeError(f"No available port found in range {start_port}-{start_port + max_attempts}")


def create_node_summary(node: NodeInfo) -> Dict[str, Any]:
    """
    Create a summary of node information for logging/display.
    
    Args:
        node: NodeInfo instance
        
    Returns:
        Dictionary with summary information
    """
    return {
        "node_id": node.node_id[:8] + "...",  # Shortened ID
        "hostname": node.hostname,
        "endpoint": node.get_endpoint(),
        "status": node.status.value,
        "role": node.role.value,
        "load_score": round(node.capacity.get_load_score(), 2),
        "cpu_percent": round(node.capacity.cpu_percent, 1),
        "memory_percent": round(node.capacity.memory_percent, 1),
        "functions": len(node.registered_functions),
        "last_seen": node.last_seen
    }