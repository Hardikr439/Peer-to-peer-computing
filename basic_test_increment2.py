"""
Simple test to verify Increment 2 networking components work.
"""

import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_basic_imports():
    """Test that all networking components can be imported."""
    try:
        from networking import (
            NodeInfo, NodeRole, NodeStatus, SystemCapacity,
            InMemoryRegistry, HeartbeatService
        )
        print("✓ All networking imports successful")
        return True
    except Exception as e:
        print(f"✗ Import error: {e}")
        return False

def test_basic_functionality():
    """Test basic functionality of networking components."""
    try:
        from networking import NodeInfo, InMemoryRegistry, SystemCapacity
        
        # Test node creation
        node = NodeInfo.create_local_node(port=8000)
        print(f"✓ Created local node: {node.node_id[:8]}...")
        
        # Test capacity
        capacity = SystemCapacity()
        print(f"✓ System capacity: CPU={capacity.cpu_percent:.1f}%, Memory={capacity.memory_percent:.1f}%")
        
        # Test registry
        registry = InMemoryRegistry()
        result = registry.register_node(node)
        print(f"✓ Registry registration: {result}")
        
        nodes = registry.list_nodes()
        print(f"✓ Registry has {len(nodes)} nodes")
        
        return True
    except Exception as e:
        print(f"✗ Functionality error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run basic tests."""
    print("=== Increment 2 Basic Verification ===")
    
    success = True
    success &= test_basic_imports()
    success &= test_basic_functionality()
    
    if success:
        print("\n✓ All basic tests passed!")
        print("Increment 2 networking components are working correctly.")
    else:
        print("\n✗ Some tests failed!")
    
    return success

if __name__ == "__main__":
    main()