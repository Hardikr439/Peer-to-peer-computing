#!/usr/bin/env python3
"""
Quick Setup Script for Distributed Function Runtime

This script helps with initial setup and verification of the system.
"""

import os
import sys
import subprocess
import socket
import json
from typing import List, Dict

def check_python_version():
    """Check if Python version is compatible."""
    print("🐍 Checking Python version...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 11:
        print(f"   ✅ Python {version.major}.{version.minor}.{version.micro} - Compatible")
        return True
    else:
        print(f"   ❌ Python {version.major}.{version.minor}.{version.micro} - Requires Python 3.11+")
        return False

def install_dependencies():
    """Install required dependencies."""
    print("\n📦 Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("   ✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Failed to install dependencies: {e}")
        return False

def test_imports():
    """Test if all modules can be imported."""
    print("\n🔬 Testing module imports...")
    
    modules_to_test = [
        "psutil",
        "src.core.local_executor",
        "src.networking.node_info",
        "src.communication.rpc_server",
    ]
    
    failed_imports = []
    
    for module in modules_to_test:
        try:
            __import__(module)
            print(f"   ✅ {module}")
        except ImportError as e:
            print(f"   ❌ {module}: {e}")
            failed_imports.append(module)
    
    return len(failed_imports) == 0

def get_local_ip():
    """Get local IP address."""
    try:
        # Connect to a remote address to determine local IP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
        return local_ip
    except Exception:
        return "127.0.0.1"

def check_network_connectivity():
    """Check if network ports are available."""
    print("\n🌐 Checking network connectivity...")
    
    local_ip = get_local_ip()
    print(f"   🔍 Local IP: {local_ip}")
    
    test_ports = [8080, 8081, 8082]
    available_ports = []
    
    for port in test_ports:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('0.0.0.0', port))
                available_ports.append(port)
                print(f"   ✅ Port {port} - Available")
        except OSError:
            print(f"   ⚠️  Port {port} - In use")
    
    if available_ports:
        print(f"   📡 Available ports for demo: {available_ports}")
        return available_ports
    else:
        print("   ❌ No test ports available")
        return []

def create_demo_config():
    """Create a demo configuration file."""
    print("\n⚙️  Creating demo configuration...")
    
    local_ip = get_local_ip()
    
    demo_config = {
        "coordinator": {
            "ip": local_ip,
            "port": 8080,
            "command": f"python main.py --mode coordinator --port 8080 --bind-address 0.0.0.0"
        },
        "worker": {
            "ip": local_ip,
            "port": 8081,
            "command": f"python main.py --mode worker --port 8081 --node-type processing --bind-address 0.0.0.0"
        },
        "client": {
            "target": f"{local_ip}:8080",
            "command": f"python main.py --mode client --target {local_ip}:8080"
        }
    }
    
    try:
        with open("demo_config.json", "w") as f:
            json.dump(demo_config, f, indent=2)
        print("   ✅ Demo configuration saved to 'demo_config.json'")
        return True
    except Exception as e:
        print(f"   ❌ Failed to create demo config: {e}")
        return False

def run_quick_test():
    """Run a quick functionality test."""
    print("\n🧪 Running quick functionality test...")
    
    try:
        # Test local execution
        result = subprocess.run([
            sys.executable, "-c", 
            """
import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))
from core.local_executor import LocalExecutor
from examples.sample_functions import HelloWorldFunction

executor = LocalExecutor()
executor.register_function("hello", HelloWorldFunction())
result = executor.execute_function("hello", {"name": "Setup Test"})
print(f"✅ Test passed: {result.output}")
            """
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("   ✅ Local execution test passed")
            print(f"   📝 Output: {result.stdout.strip()}")
            return True
        else:
            print(f"   ❌ Local execution test failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("   ❌ Test timed out")
        return False
    except Exception as e:
        print(f"   ❌ Test error: {e}")
        return False

def show_next_steps():
    """Show next steps for the user."""
    print("\n🚀 Next Steps:")
    print("="*50)
    
    print("\n1. Single Device Demo:")
    print("   python main.py --mode demo")
    
    print("\n2. Multi-Device Setup:")
    print("   # On Device 1 (Coordinator):")
    print("   python main.py --mode coordinator --port 8080 --bind-address 0.0.0.0")
    
    print("\n   # On Device 2 (Client):")
    print("   python main.py --mode client --target <DEVICE1_IP>:8080")
    
    print("\n3. Interactive Client:")
    print("   python main.py --mode client --target localhost:8080")
    
    print("\n4. Worker Node:")
    print("   python main.py --mode worker --port 8081 --node-type basic")
    
    print("\n📚 Documentation:")
    print("   See README.md for detailed setup instructions")
    
    print("\n🔧 Configuration:")
    print("   Check 'demo_config.json' for example commands")

def main():
    """Main setup function."""
    print("🌟 Distributed Function Runtime - Setup & Verification")
    print("="*60)
    
    all_passed = True
    
    # Check Python version
    if not check_python_version():
        all_passed = False
    
    # Install dependencies
    if all_passed and not install_dependencies():
        all_passed = False
    
    # Test imports
    if all_passed and not test_imports():
        all_passed = False
    
    # Check network
    available_ports = check_network_connectivity()
    if not available_ports:
        print("   ⚠️  Warning: No test ports available, but system may still work")
    
    # Create demo config
    create_demo_config()
    
    # Run quick test
    if all_passed and not run_quick_test():
        all_passed = False
    
    print("\n" + "="*60)
    if all_passed:
        print("🎉 SETUP COMPLETED SUCCESSFULLY!")
        print("✅ All checks passed - System is ready for use")
        show_next_steps()
    else:
        print("❌ SETUP INCOMPLETE")
        print("⚠️  Some issues were detected. Please resolve them before proceeding.")
        print("\nCommon solutions:")
        print("- Upgrade Python to 3.11+")
        print("- Install dependencies: pip install -r requirements.txt")
        print("- Check network connectivity and firewall settings")
    
    print("\n👋 Setup complete!")

if __name__ == "__main__":
    main()