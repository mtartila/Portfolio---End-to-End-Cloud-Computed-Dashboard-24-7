#!/bin/bash
LOG_FILE="/root/my_python_project/test_cron.log"
ERR_FILE="/root/my_python_project/test_cron_err.log"

echo "$(date) - Starting test script" >> $LOG_FILE 2>> $ERR_FILE

# Change to project directory
cd /root/my_python_project || { echo "$(date) - Failed to change directory" >> $ERR_FILE; exit 1; }

# Activate the virtual environment
source portfolio/bin/activate 2>> $ERR_FILE || { echo "$(date) - Failed to activate virtual environment" >> $ERR_FILE; exit 1; }

# Function to check if current time is within the specified range
is_within_time_range() {
    local current_hour=$(date +%H)
    local current_minute=$(date +%M)
    
    # Convert current time to minutes since midnight
    local current_time=$((10#${current_hour} * 60 + 10#${current_minute}))
    
    # Time ranges in minutes since midnight
    local start_time=$((20 * 60 + 30)) # 8:30 PM
    local end_time=$((3 * 60))         # 3:00 AM (next day)

    # Debug log for current time
    echo "$(date) - Current time in minutes since midnight: $current_time" >> $LOG_FILE

    # Check if current time is within the range
    if (( current_time >= start_time || current_time < end_time )); then
        echo "$(date) - Time is within range" >> $LOG_FILE
        return 0
    else
        echo "$(date) - Time is outside of range" >> $LOG_FILE
        return 1
    fi
}

# Infinite loop to run the fetch_finnhub_data.py script every 10 seconds within the specified hours
while true; do
    if is_within_time_range; then
        echo "$(date) - Running fetch_finnhub_data.py" >> $LOG_FILE 2>> $ERR_FILE
        python3 ALIBABA_ECS_Finnhub_Fetch_Stock.py >> $LOG_FILE 2>> $ERR_FILE
        echo "$(date) - Finished running fetch_finnhub_data.py" >> $LOG_FILE 2>> $ERR_FILE
        sleep 600
    else
        echo "$(date) - Outside of scheduled hours, sleeping for 10 minutes" >> $LOG_FILE 2>> $ERR_FILE
        sleep 600 # Sleep for 10 minutes before checking again
    fi
done

echo "$(date) - Test script finished" >> $LOG_FILE 2>> $ERR_FILE
