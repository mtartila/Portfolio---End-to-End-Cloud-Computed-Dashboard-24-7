#!/bin/bash
LOG_FILE="/root/my_python_project/etl_cron.log"
ERR_FILE="/root/my_python_project/etl_cron_err.log"

echo "$(date) - Starting ETL script" >> $LOG_FILE 2>> $ERR_FILE

# Change to project directory
cd /root/my_python_project || { echo "$(date) - Failed to change directory" >> $ERR_FILE; exit 1; }

# Activate the virtual environment
source portfolio/bin/activate 2>> $ERR_FILE || { echo "$(date) - Failed to activate virtual environment" >> $ERR_FILE; exit 1; }

# Run the ETL script
python3 ETL_Daily_Average.py >> $LOG_FILE 2>> $ERR_FILE

echo "$(date) - Finished ETL script" >> $LOG_FILE 2>> $ERR_FILE
