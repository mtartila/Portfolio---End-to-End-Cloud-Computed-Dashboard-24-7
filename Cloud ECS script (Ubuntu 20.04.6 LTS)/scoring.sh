#!/bin/bash
LOG_FILE="/root/my_python_project/scoring_log.log"
ERR_FILE="/root/my_python_project/scoring_log_err.log"

echo "$(date) - Starting test script" >> $LOG_FILE 2>> $ERR_FILE

# Change to project directory
cd /root/my_python_project || { echo "$(date) - Failed to change directory" >> $ERR_FILE; exit 1; }

# Activate the virtual environment
source portfolio/bin/activate 2>> $ERR_FILE || { echo "$(date) - Failed to activate virtual environment" >> $ERR_FILE; exit 1; }

# Run your Python script
python3 predict_scoring.py >> $LOG_FILE 2>> $ERR_FILE


echo "$(date) - Test script finished" >> $LOG_FILE 2>> $ERR_FILE
