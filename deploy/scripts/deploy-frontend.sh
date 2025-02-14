#!/bin/bash

# Build the frontend
cd web
npm install
npm run build

echo "Build complete. To deploy:"
echo "1. Install nginx on your server:"
echo "   sudo apt update && sudo apt install -y nginx"
echo ""
echo "2. Copy files to server:"
echo "   scp -r web/dist/* user@your-frontend-server:/usr/share/nginx/html/"
echo ""
echo "3. For https, see https://certbot.eff.org"
