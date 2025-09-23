#!/bin/bash

# Script to start Airflow
# This script activates the virtual environment and starts Airflow in standalone mode

# Go to project directory (2 levels up)
cd "$(dirname "$0")/../.."

# Activate virtual environment
source venv/bin/activate

# Set Airflow home directory
export AIRFLOW_HOME=$(pwd)/airflow

echo "Starting Airflow..."
echo "Web interface available at: http://localhost:8080"
echo "Credentials:"
echo "  Username: admin"
echo "  Password: $(cat $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated 2>/dev/null | grep -o '"admin": "[^"]*"' | cut -d'"' -f4 || echo 'will be generated on first startup')"
echo ""
echo "Press Ctrl+C to stop Airflow"
echo ""

# Start Airflow
airflow standalone
