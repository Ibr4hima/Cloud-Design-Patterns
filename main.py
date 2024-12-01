#!/usr/bin/env python3
import os
import time
from aws_resource_manager import AWSResourceManager
from instance_manager import InstanceManager
import json

# MySQL credentials
MYSQL_USER = 'Ibrahima'
MYSQL_PASSWORD = 'Ibr@hima'

# Load service scripts
try:
    with open('proxy.py', 'r') as f:
        PROXY_CODE = f.read()
    with open('trusted_host.py', 'r') as f:
        TRUSTED_HOST_CODE = f.read()
    with open('gatekeeper.py', 'r') as f:
        GATEKEEPER_CODE = f.read()
except FileNotFoundError as e:
    print(f"Error: {str(e)}. Ensure all required service scripts are in the working directory.")
    exit(1)

def print_section(title):
    """Print a formatted section header."""
    print("\n" + "=" * 50)
    print(title)
    print("=" * 50)

def setup_cluster():
    """Setup complete MySQL cluster and all components."""
    print_section("Starting Cluster Setup")

    # Initialize AWS resource manager
    print("\nInitializing AWS resources...")
    aws_manager = AWSResourceManager()

    # Create key pair and security groups
    if not aws_manager.create_key_pair():
        return False, None
    if not aws_manager.create_security_groups():  # Modified to create all security groups
        return False, None

    instances = {}

    # Initialize instance manager
    instance_manager = InstanceManager(aws_manager.get_key_path())

    # Create and setup MySQL instances
    print_section("Setting up MySQL Instances")

    for role in ['Manager', 'Worker-1', 'Worker-2']:
        print(f"\nSetting up {role} instance...")
        # Create instance with mysql security group
        instance = aws_manager.create_instance('micro', role, 'mysql')
        if not instance:
            print(f"Failed to create {role} instance")
            return False, None
        instances[role.lower()] = instance

        # Setup MySQL and Sakila database
        if not instance_manager.setup_mysql_standalone(instance['dns'], MYSQL_USER, MYSQL_PASSWORD):
            print(f"Failed to setup MySQL on {role}")
            return False, None

        # Run sysbench
        print(f"Running sysbench on {role}...")
        if not instance_manager.run_sysbench(instance['dns'], MYSQL_USER, MYSQL_PASSWORD):
            print(f"Warning: Sysbench test failed on {role}")

    # Setup services (Proxy, Trusted Host, Gatekeeper)
    print_section("Setting up Services")

    services = {
        'proxy': {
            'name': 'Proxy',
            'code': PROXY_CODE,
            'args': ['manager', 'worker-1,worker-2', MYSQL_USER, MYSQL_PASSWORD],
            'security_group': 'proxy'
        },
        'trusted_host': {
            'name': 'Trusted-Host',
            'code': TRUSTED_HOST_CODE,
            'args': ['proxy'],
            'security_group': 'trusted-host'
        },
        'gatekeeper': {
            'name': 'Gatekeeper',
            'code': GATEKEEPER_CODE,
            'args': ['trusted_host'],
            'security_group': 'gatekeeper'
        }
    }

    for key, service in services.items():
        print(f"\nSetting up {service['name']} instance...")
        # Create instance with appropriate security group
        instance = aws_manager.create_instance('large', service['name'], service['security_group'])
        if not instance:
            print(f"Failed to create {service['name']} instance")
            return False, None
        instances[key] = instance

        # Deploy the service
        dns_args = [instances[arg]['dns'] if arg in instances else arg for arg in service['args']]
        if not instance_manager.deploy_service(instance['dns'], service['name'], service['code'], dns_args):
            print(f"Failed to deploy {service['name']} service")
            return False, None

    print_section("Cluster Setup Complete")
    return True, instances

def print_cluster_info(instances):
    """Display cluster details and save information in JSON format."""
    print("\n" + "=" * 50)
    print("Cluster Setup Complete")
    print("=" * 50)
    print("\nAll services are running and properly configured.")
    print("\nComponent Port Information:")
    print("- MySQL: Port 3306")
    print("- Proxy:")
    print("  * Direct Hit: Port 3306")
    print("  * Random: Port 3307")
    print("  * Custom: Port 3308")
    print("  * API: Port 5000")
    print("- Trusted Host: Port 5001")
    print("- Gatekeeper: Port 5000")
    print("\nTo run benchmarks, execute: python benchmark.py")

    # Create cluster information in JSON format
    cluster_info = {
        "instances": {
            "manager": {
                "hostname": instances['manager']['dns'],
                "type": "t2.micro",
                "role": "MySQL Manager",
                "port": 3306
            },
            "workers": [
                {
                    "hostname": instances['worker-1']['dns'],
                    "type": "t2.micro",
                    "role": "MySQL Worker 1",
                    "port": 3306
                },
                {
                    "hostname": instances['worker-2']['dns'],
                    "type": "t2.micro",
                    "role": "MySQL Worker 2",
                    "port": 3306
                }
            ],
            "services": {
                "gatekeeper": {
                    "hostname": instances['gatekeeper']['dns'],
                    "type": "t2.large",
                    "role": "Gatekeeper Service",
                    "port": 5000
                },
                "trusted_host": {
                    "hostname": instances['trusted_host']['dns'],
                    "type": "t2.large",
                    "role": "Trusted Host Service",
                    "port": 5001
                },
                "proxy": {
                    "hostname": instances['proxy']['dns'],
                    "type": "t2.large",
                    "role": "Proxy Service",
                    "ports": {
                        "direct": 3306,
                        "random": 3307,
                        "custom": 3308,
                        "api": 5000
                    }
                }
            }
        },
        "security_groups": {
            "mysql": ["22/tcp", "3306/tcp"],
            "proxy": ["22/tcp", "3306/tcp", "3307/tcp", "3308/tcp", "5000/tcp"],
            "trusted_host": ["22/tcp", "5001/tcp"],
            "gatekeeper": ["22/tcp", "5000/tcp"]
        }
    }

    # Save to JSON file
    with open('information.json', 'w') as f:
        json.dump(cluster_info, f, indent=4)

def main():
    """Main function to setup the cluster."""
    try:
        success, instances = setup_cluster()
        if success and instances:
            print_cluster_info(instances)
            print("\nSetup completed successfully!")
        else:
            print("\nSetup failed!")
    except KeyboardInterrupt:
        print("\nSetup interrupted by user!")
    except Exception as e:
        print(f"\nSetup failed with error: {str(e)}")

if __name__ == "__main__":
    main()