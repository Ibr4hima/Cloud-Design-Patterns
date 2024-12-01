import boto3
import os
import time
from botocore.exceptions import ClientError

class AWSResourceManager:
    def __init__(self, aws_region='us-east-1'):
        """Initialize AWS Resource Manager."""
        self.ec2 = boto3.client('ec2', region_name=aws_region)
        self.key_name = 'my-keypair'
        self.vpc_id = None
        self.security_groups = {
            'mysql': {'name': 'mysql-sg', 'id': None},
            'proxy': {'name': 'proxy-sg', 'id': None},
            'gatekeeper': {'name': 'gatekeeper-sg', 'id': None},
            'trusted-host': {'name': 'trusted-host-sg', 'id': None}
        }
        self.instance_configs = {
            'micro': {
                'ImageId': 'ami-0885b1f6bd170450c',  # Ubuntu AMI
                'InstanceType': 't2.micro',
                'MinCount': 1,
                'MaxCount': 1
            },
            'large': {
                'ImageId': 'ami-0885b1f6bd170450c',  # Ubuntu AMI
                'InstanceType': 't2.large',
                'MinCount': 1,
                'MaxCount': 1
            }
        }

    def create_key_pair(self):
        """Create an SSH key pair for instances."""
        try:
            # Check if key pair already exists
            try:
                self.ec2.describe_key_pairs(KeyNames=[self.key_name])
                print(f"Using existing key pair: {self.key_name}")
                return True
            except ClientError:
                # Key doesn't exist, create new one
                self.ec2.delete_key_pair(KeyName=self.key_name)
                if os.path.exists(f"{self.key_name}.pem"):
                    os.remove(f"{self.key_name}.pem")
                
                key_pair = self.ec2.create_key_pair(KeyName=self.key_name)
                with open(f"{self.key_name}.pem", 'w') as file:
                    file.write(key_pair['KeyMaterial'])
                os.chmod(f"{self.key_name}.pem", 0o400)
                print(f"Key Pair {self.key_name} Created and Saved locally.")
            return True
        except Exception as e:
            print(f"Error creating key pair: {str(e)}")
            return False

    def _create_security_group(self, group_name, description, rules):
        """Helper method to create or get existing security group."""
        try:
            # Check if security group already exists
            existing_groups = self.ec2.describe_security_groups(
                Filters=[{'Name': 'group-name', 'Values': [group_name]}]
            )
            if existing_groups['SecurityGroups']:
                return existing_groups['SecurityGroups'][0]['GroupId']

            # Create new security group
            sg_response = self.ec2.create_security_group(
                GroupName=group_name,
                Description=description,
                VpcId=self.vpc_id
            )
            group_id = sg_response['GroupId']

            # Add rules
            permissions = []
            for protocol, from_port, to_port, cidr in rules:
                permissions.append({
                    'IpProtocol': protocol,
                    'FromPort': from_port,
                    'ToPort': to_port,
                    'IpRanges': [{'CidrIp': cidr}]
                })

            if permissions:
                self.ec2.authorize_security_group_ingress(
                    GroupId=group_id,
                    IpPermissions=permissions
                )

            return group_id
        except Exception as e:
            print(f"Error creating security group {group_name}: {str(e)}")
            return None

    def create_security_groups(self):
        """Create and configure all required security groups."""
        try:
            # Get VPC
            vpcs = self.ec2.describe_vpcs(Filters=[{'Name': 'isDefault', 'Values': ['true']}])
            if not vpcs['Vpcs']:
                raise Exception("No default VPC found.")
            self.vpc_id = vpcs['Vpcs'][0]['VpcId']

            # Define rules for each security group
            security_group_rules = {
                'mysql': [
                    ('tcp', 22, 22, '0.0.0.0/0'),     # SSH
                    ('tcp', 3306, 3306, '0.0.0.0/0')  # MySQL
                ],
                'proxy': [
                    ('tcp', 22, 22, '0.0.0.0/0'),     # SSH
                    ('tcp', 3306, 3306, '0.0.0.0/0'), # Direct Hit
                    ('tcp', 3307, 3307, '0.0.0.0/0'), # Random
                    ('tcp', 3308, 3308, '0.0.0.0/0'), # Custom
                    ('tcp', 5000, 5000, '0.0.0.0/0')  # API
                ],
                'gatekeeper': [
                    ('tcp', 22, 22, '0.0.0.0/0'),    # SSH
                    ('tcp', 5000, 5000, '0.0.0.0/0') # Flask App
                ],
                'trusted-host': [
                    ('tcp', 22, 22, '0.0.0.0/0'),    # SSH
                    ('tcp', 5001, 5001, '0.0.0.0/0') # Internal API
                ]
            }

            # Create each security group
            for sg_type, rules in security_group_rules.items():
                sg_id = self._create_security_group(
                    self.security_groups[sg_type]['name'],
                    f'Security group for {sg_type}',
                    rules
                )
                if not sg_id:
                    return False
                self.security_groups[sg_type]['id'] = sg_id
                print(f"Security group {self.security_groups[sg_type]['name']} created/updated.")

            return True
        except Exception as e:
            print(f"Error creating security groups: {str(e)}")
            return False

    def create_instance(self, instance_type='micro', name='', role='mysql'):
        """Launch an EC2 instance with appropriate security group."""
        try:
            if role not in self.security_groups:
                raise Exception(f"Invalid role: {role}")
            
            if not self.security_groups[role]['id']:
                raise Exception(f"Security group for {role} not created.")
            
            config = self.instance_configs[instance_type].copy()
            config.update({
                'SecurityGroupIds': [self.security_groups[role]['id']],
                'KeyName': self.key_name,
                'TagSpecifications': [{
                    'ResourceType': 'instance',
                    'Tags': [{'Key': 'Name', 'Value': name}]
                }]
            })

            instances = self.ec2.run_instances(**config)
            instance_id = instances['Instances'][0]['InstanceId']
            print(f"Launching instance: {name}")

            self.ec2.get_waiter('instance_running').wait(InstanceIds=[instance_id])
            time.sleep(60)  # Wait for initialization
            
            instance_info = self.ec2.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0]
            print(f"Instance {name} is running: {instance_info['PublicDnsName']}")
            return {
                'id': instance_id,
                'dns': instance_info['PublicDnsName'],
                'ip': instance_info['PublicIpAddress']
            }
        except Exception as e:
            print(f"Error launching instance {name}: {str(e)}")
            return None

    def get_key_path(self):
        """Get the local path to the SSH key."""
        return f"{self.key_name}.pem"