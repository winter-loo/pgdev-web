#!/bin/bash

set -e

# Build using Docker with Amazon Linux 2023
echo "Building using Amazon Linux 2023..."
docker build -f deploy/Dockerfile.build -t pgdevhub-builder .

# Extract the binary
echo "Extracting binary..."
docker create --name temp pgdevhub-builder
docker cp temp:/output/pgdevhub ./target/release/
docker rm temp

# Create systemd service file
cat << EOF > pgdevhub.service
[Unit]
Description=PostgreSQL Development Web Service
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/services
ExecStart=/home/ec2-user/services/pgdevhub
Restart=always
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
EOF

echo "Build complete. Deploy to EC2 with:"
echo "scp -i your-key.pem target/release/pgdevhub ec2-user@your-ec2-ip:/home/ec2-user/services/pgdevhub/"
echo "scp -i your-key.pem pgdevhub.service ec2-user@your-ec2-ip:/etc/systemd/system/"
echo ""
echo "Then on EC2:"
echo "sudo systemctl daemon-reload"
echo "sudo systemctl enable pgdevhub"
echo "sudo systemctl start pgdevhub"
