#!/bin/bash

# setup_mongodb.sh - Script to set up MongoDB db and user with robust error handling, logging, and environment variable loading.
# This script configures MongoDB for the HNSC Omics project, creating a db and user as defined in the .env file.

# Define the logs directory and log file path
LOGDIR="logs"
LOGFILE="$LOGDIR/setup_mongodb.log"

# Ensure the logs directory exists
# - Creates the logs directory if it does not already exist to avoid errors during logging
if [ ! -d "$LOGDIR" ]; then
  mkdir -p "$LOGDIR"
fi

# Rotate the log file if it exceeds 1MB (1048576 bytes)
# - If the log file is too large, rename it to setup_mongodb.log.old
# - Creates a fresh LOGFILE to avoid excessive file growth
if [ -f "$LOGFILE" ] && [ "$(stat -c%s "$LOGFILE")" -gt 1048576 ]; then
  mv "$LOGFILE" "${LOGFILE}.old"
  echo "$(date): Log rotated. Old log moved to ${LOGFILE}.old." >> "$LOGFILE"
fi

# Start the setup process and log all output to LOGFILE
{
  echo "$(date): Starting MongoDB setup process..."

  # Step 1: Check if the .env file exists in the config directory
  # - The .env file is essential as it contains MONGO_DB_NAME, DB_USER, DB_PASSWORD, MONGO_HOST, and MONGO_PORT
  # - If missing, the script exits to prevent running with missing configurations
  if [ ! -f config/.env ]; then
    echo "Error: config/.env file not found. Please ensure the .env file is present and correctly configured."
    exit 1
  fi

  # Step 2: Load environment variables from the .env file
  # - 'grep -v '^#'' excludes lines beginning with # (comments)
  # - 'xargs' helps format the variables correctly for export
  # - If this command fails, the script exits, as the variables are crucial for the setup
  echo "$(date): Loading environment variables from config/.env..."
  # shellcheck disable=SC2046
  export $(grep -v '^#' config/.env | xargs) || {
    echo "Error: Failed to load environment variables from config/.env."
    exit 1
  }

  # Step 3: Check if essential environment variables are loaded
  # - Ensures that MONGO_DB_NAME, DB_USER, DB_PASSWORD, MONGO_HOST, and MONGO_PORT are not empty
  # - If any are missing, exits with an error message since these variables are necessary for setup
  if [[ -z "$MONGO_DB_NAME" || -z "$DB_USER" || -z "$DB_PASSWORD" || -z "$MONGO_HOST" || -z "$MONGO_PORT" ]]; then
    echo "Error: One or more required environment variables (MONGO_DB_NAME, DB_USER, DB_PASSWORD, MONGO_HOST, MONGO_PORT) are missing."
    exit 1
  fi

  # Step 4: Execute MongoDB setup commands using --eval for non-interactive mode
  # - Connects to MongoDB and creates the db and user if they do not exist
  # - Provides feedback if db/user already exists
  echo "$(date): Setting up MongoDB database and user..."

  mongosh --host "$MONGO_HOST" --port "$MONGO_PORT" --eval "
    const db = db.getSiblingDB('$MONGO_DB_NAME');
    if (db.getUser('$DB_USER') === null) {
      db.createUser({
        user: '$DB_USER',
        pwd: '$DB_PASSWORD',
        roles: [{ role: 'readWrite', db: '$MONGO_DB_NAME' }]
      });
      print('User \"$DB_USER\" created with read and write access.');
    } else {
      print('User \"$DB_USER\" already exists, skipping creation.');
    }
  " || {
    echo "Error: Failed to execute MongoDB setup commands."
    exit 1
  }

  # Step 5: Validate MongoDB db and user creation
  # - Confirms the db and user were created successfully by querying MongoDB
  # - If validation fails, exits with an error message to indicate incomplete setup
  echo "$(date): Validating MongoDB database and user creation..."

  # Check if the user exists by querying MongoDB in non-interactive mode
  if mongosh --host "$MONGO_HOST" --port "$MONGO_PORT" --eval "db.getSiblingDB('$MONGO_DB_NAME').getUser('$DB_USER') !== null" | grep -q "true"; then
    echo "MongoDB setup complete. Database '$MONGO_DB_NAME' and user '$DB_USER' have been created and validated."
  else
    echo "Error: MongoDB setup failed. Database '$MONGO_DB_NAME' or user '$DB_USER' was not created successfully."
    exit 1
  fi

} | tee -a "$LOGFILE"  # Log all output to both the console and LOGFILE
