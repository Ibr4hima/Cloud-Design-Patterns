import paramiko
import time
import os

class InstanceManager:
    def __init__(self, key_path):
        """Initialize Instance Manager with SSH key path."""
        self.key_path = key_path
        self.port_mappings = {
            'direct': 3306,
            'random': 3307,
            'custom': 3308
        }

    def _get_ssh_connection(self, host):
        """Create and return SSH connection to instance."""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                hostname=host,
                username='ubuntu',
                key_filename=self.key_path,
                timeout=60
            )
            return ssh
        except Exception as e:
            print(f"Error establishing SSH connection to {host}: {str(e)}")
            return None

    def _execute_ssh_commands(self, ssh, commands):
        """Execute a list of SSH commands on the instance."""
        try:
            for cmd in commands:
                print(f"Executing: {cmd}")
                stdin, stdout, stderr = ssh.exec_command(cmd)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    error = stderr.read().decode()
                    print(f"Command failed: {cmd}\nError: {error}")
                    return False
            return True
        except Exception as e:
            print(f"Error executing commands: {str(e)}")
            return False

    def setup_mysql_standalone(self, host, username, password):
        """Setup MySQL standalone instance with Sakila database."""
        try:
            print(f"\nSetting up MySQL on {host}...")
            ssh = self._get_ssh_connection(host)
            if not ssh:
                return False

            # MySQL installation and configuration commands with enhanced security
            setup_commands = [
                'sudo apt-get update -y',
                'sudo apt-get install -y mysql-client',
                'sudo DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server wget',
                'sudo systemctl start mysql',
                'sudo systemctl enable mysql',
                # Secure MySQL installation
                'sudo sh -c \'echo "default_authentication_plugin=mysql_native_password" >> /etc/mysql/mysql.conf.d/mysqld.cnf\'',
                'sudo sh -c \'echo "bind-address = 0.0.0.0" >> /etc/mysql/mysql.conf.d/mysqld.cnf\'',
                'sudo systemctl restart mysql',
                # Configure MySQL users and permissions
                f'sudo mysql -e "ALTER USER \'root\'@\'localhost\' IDENTIFIED BY \'{password}\';"',
                f'sudo mysql -u root -p{password} -e "CREATE USER \'{username}\'@\'%\' IDENTIFIED BY \'{password}\';"',
                f'sudo mysql -u root -p{password} -e "GRANT ALL PRIVILEGES ON *.* TO \'{username}\'@\'%\' WITH GRANT OPTION;"',
                f'sudo mysql -u root -p{password} -e "ALTER USER \'{username}\'@\'%\' IDENTIFIED WITH mysql_native_password BY \'{password}\';"',
                'sudo mysql -u root -p{password} -e "FLUSH PRIVILEGES;"',
                # Configure firewall
                'sudo apt-get install -y ufw',
                'sudo ufw allow 22/tcp',
                'sudo ufw allow 3306/tcp',
                'sudo ufw --force enable'
            ]

            if not self._execute_ssh_commands(ssh, setup_commands):
                return False

            # Install and setup Sakila database
            sakila_commands = [
                'wget https://downloads.mysql.com/docs/sakila-db.tar.gz',
                'tar -xf sakila-db.tar.gz',
                f'sudo mysql -u {username} -p{password} -e "CREATE DATABASE IF NOT EXISTS sakila;"',
                f'sudo mysql -u {username} -p{password} sakila < sakila-db/sakila-schema.sql',
                f'sudo mysql -u {username} -p{password} sakila < sakila-db/sakila-data.sql',
            ]

            print(f"Installing Sakila database on {host}...")
            if not self._execute_ssh_commands(ssh, sakila_commands):
                return False

            ssh.close()
            print(f"MySQL and Sakila setup completed successfully on {host}.")
            return True
        except Exception as e:
            print(f"Error setting up MySQL on {host}: {str(e)}")
            return False

    def deploy_service(self, host, service_name, service_code, args):
        """Deploy a Python service on an instance with enhanced security."""
        try:
            print(f"\nDeploying {service_name} on {host}...")
            ssh = self._get_ssh_connection(host)
            if not ssh:
                return False

            # Enhanced setup commands with security considerations
            setup_commands = [
                'sudo apt-get update -y',
                'sudo apt-get install -y python3-venv python3-pip ufw',
                'python3 -m venv /home/ubuntu/venv',
                '/home/ubuntu/venv/bin/pip install flask requests mysql-connector-python paramiko',
                'mkdir -p /home/ubuntu/app',
                'sudo chown -R ubuntu:ubuntu /home/ubuntu',
                # Configure firewall based on service type
                'sudo ufw allow 22/tcp'  # SSH always allowed
            ]

            # Add service-specific firewall rules
            if service_name.lower() == 'proxy':
                setup_commands.extend([
                    'sudo ufw allow 3306/tcp',  # Direct hit
                    'sudo ufw allow 3307/tcp',  # Random
                    'sudo ufw allow 3308/tcp',  # Custom
                    'sudo ufw allow 5000/tcp'   # API
                ])
            elif service_name.lower() == 'gatekeeper':
                setup_commands.extend([
                    'sudo ufw allow 5000/tcp'   # Flask App
                ])
            elif service_name.lower() == 'trusted-host':
                setup_commands.extend([
                    'sudo ufw allow 5001/tcp'   # Internal API
                ])

            setup_commands.append('sudo ufw --force enable')

            for cmd in setup_commands:
                print(f"Executing: {cmd}")
                stdin, stdout, stderr = ssh.exec_command(cmd)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    error = stderr.read().decode()
                    print(f"Command failed: {cmd}\nError: {error}")
                    return False

            # Upload and configure application
            print("Uploading application code...")
            sftp = ssh.open_sftp()
            app_dir = '/home/ubuntu/app'
            with sftp.file(f'{app_dir}/app.py', 'w') as f:
                f.write(service_code)

            # Create enhanced start script
            start_script = f'''#!/bin/bash
source /home/ubuntu/venv/bin/activate
cd {app_dir}
python3 app.py {" ".join(args)}
'''
            with sftp.file(f'{app_dir}/start.sh', 'w') as f:
                f.write(start_script)

            ssh.exec_command(f'chmod +x {app_dir}/start.sh')

            # Create systemd service with enhanced security
            systemd_service = f'''[Unit]
Description={service_name} Service
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory={app_dir}
Environment=PYTHONUNBUFFERED=1
Environment="PATH=/home/ubuntu/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ExecStart={app_dir}/start.sh
Restart=always
RestartSec=5
StandardOutput=append:/var/log/{service_name.lower()}.log
StandardError=append:/var/log/{service_name.lower()}_error.log
# Enhanced security
NoNewPrivileges=yes
ProtectSystem=full
ProtectHome=read-only
PrivateTmp=true

[Install]
WantedBy=multi-user.target
'''
            stdin, stdout, stderr = ssh.exec_command(f"echo '{systemd_service}' | sudo tee /etc/systemd/system/{service_name}.service")
            stdout.read()
            stderr.read()

            # Start and enable service
            start_commands = [
                'sudo systemctl daemon-reload',
                f'sudo systemctl enable {service_name}',
                f'sudo systemctl restart {service_name}'
            ]

            for cmd in start_commands:
                stdin, stdout, stderr = ssh.exec_command(cmd)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    error = stderr.read().decode()
                    print(f"Command failed: {cmd}\nError: {error}")
                    return False

            time.sleep(5)

            # Verify service status
            print("Verifying service status...")
            stdin, stdout, stderr = ssh.exec_command(f'sudo systemctl status {service_name}')
            status_output = stdout.read().decode()
            if "active (running)" not in status_output:
                print(f"Service {service_name} failed to start. Status:\n{status_output}")
                return False

            ssh.close()
            print(f"{service_name} deployed successfully on {host}.")
            return True
        except Exception as e:
            print(f"Error deploying service {service_name} on {host}: {str(e)}")
            return False

    def run_sysbench(self, host, username, password):
        """Run sysbench benchmark on MySQL instance using sakila database."""
        try:
            print(f"\nRunning Sysbench benchmark on {host}...")
            ssh = self._get_ssh_connection(host)
            if not ssh:
                return False

            # Install sysbench
            install_cmd = 'sudo apt-get install -y sysbench'
            print("\nInstalling sysbench...")
            stdin, stdout, stderr = ssh.exec_command(install_cmd)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                print("Failed to install sysbench")
                return False

            # Prepare benchmark
            print("\nPreparing benchmark...")
            prepare_cmd = (
                f'sysbench /usr/share/sysbench/oltp_read_only.lua '
                f'--tables=1 --table-size=10000 '
                f'--mysql-host=127.0.0.1 '
                f'--mysql-db=sakila '
                f'--mysql-user={username} '
                f'--mysql-password={password} '
                'prepare'
            )
            stdin, stdout, stderr = ssh.exec_command(prepare_cmd)
            prepare_status = stdout.channel.recv_exit_status()

            # Run benchmark
            print("\nExecuting benchmark...")
            run_cmd = (
                f'sysbench /usr/share/sysbench/oltp_read_only.lua '
                f'--tables=1 --table-size=10000 '
                f'--mysql-host=127.0.0.1 '
                f'--mysql-db=sakila '
                f'--mysql-user={username} '
                f'--mysql-password={password} '
                '--threads=4 --time=60 run'
            )
            stdin, stdout, stderr = ssh.exec_command(run_cmd)
            benchmark_output = stdout.read().decode('utf-8')

            # Extract and display only the final statistics
            print("\nSysbench Benchmark Results:")
            print("=" * 50)
            
            # Parse and display relevant sections
            sections = benchmark_output.split('\n\n')
            for section in sections:
                if any(header in section for header in ['SQL statistics:', 'General statistics:', 'Latency (ms):', 'Threads fairness:']):
                    print(section.strip())
                    print() 

            # Display key metrics summary
            summary_metrics = {}
            for line in benchmark_output.split('\n'):
                if 'transactions:' in line and 'per sec' in line:
                    summary_metrics['transactions'] = line.strip()
                elif 'total time:' in line:
                    summary_metrics['total_time'] = line.strip()
                elif any(x in line for x in ['min:', 'avg:', 'max:', '95th percentile:']):
                    if 'Latency (ms):' in line:
                        continue
                    summary_metrics[line.split(':')[0].strip()] = line.strip()

            print("\nKey Performance Metrics:")
            print("=" * 50)
            for metric, value in summary_metrics.items():
                print(value)

            ssh.close()
            print("\nSysbench benchmark completed successfully.")
            return True

        except Exception as e:
            print(f"Error running sysbench on {host}: {str(e)}")
            return False