#!/bin/bash

# setup_postgresql.sh
# This script automates the setup process for the PostgreSQL database needed in the HNSC Multi-Omics Database Project.
# It creates a PostgreSQL database, a user with specific privileges, and applies necessary configurations.
# It also logs each step of the setup process, handling environment variables securely, and rotates logs if needed.

# Define the logs directory and log file path
LOGDIR="logs"
LOGFILE="$LOGDIR/setup_postgresql.log"

# Ensure the logs directory exists
# - Creates the logs directory if it does not already exist to avoid errors during logging
if [ ! -d "$LOGDIR" ]; then
  mkdir -p "$LOGDIR"
fi

# Rotate the log file if it exceeds 1MB (1048576 bytes)
# - If the log file is too large, rename it to setup_postgresql.log.old
# - Creates a fresh LOGFILE to avoid excessive file growth
if [ -f "$LOGFILE" ] && [ "$(stat -c%s "$LOGFILE")" -gt 1048576 ]; then
  mv "$LOGFILE" "${LOGFILE}.old"
  echo "$(date): Log rotated. Old log moved to ${LOGFILE}.old." >> "$LOGFILE"
fi

# Start the setup process and log all output to LOGFILE
{
  echo "$(date): Starting PostgreSQL setup process..."

  # Step 1: Check if the .env file exists in the config directory
  # - The .env file is essential as it contains PG_DB_NAME, DB_USER, DB_PASSWORD, PG_HOST, and PG_PORT
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
  # - Ensures that PG_DB_NAME, DB_USER, DB_PASSWORD, PG_HOST, and PG_PORT are not empty
  # - If any are missing, exits with an error message since these variables are necessary for setup
  if [[ -z "$PG_DB_NAME" || -z "$DB_USER" || -z "$DB_PASSWORD" || -z "$PG_HOST" || -z "$PG_PORT" ]]; then
    echo "Error: One or more required environment variables (PG_DB_NAME, DB_USER, DB_PASSWORD, PG_HOST, PG_PORT) are missing."
    exit 1
  fi

  # Step 4: Ensure PostgreSQL service is active
  # - Attempts to start PostgreSQL using 'sudo service postgresql start'
  # - Exits if the service cannot be started, as this indicates an issue with PostgreSQL setup
  echo "$(date): Ensuring PostgreSQL service is active..."
  if ! sudo service postgresql start; then
    echo "Error: Failed to start PostgreSQL service. Ensure PostgreSQL is correctly installed and running."
    exit 1
  fi

  # Step 5: Enable dblink extension (required for database creation within PL/pgSQL)
  # - The dblink extension allows cross-database communication, useful for conditional database creation
  echo "$(date): Enabling dblink extension..."
  sudo -u postgres psql -c "CREATE EXTENSION IF NOT EXISTS dblink;" || {
    echo "Error: Failed to create dblink extension. Check PostgreSQL permissions or configuration."
    exit 1
  }

  # Step 6: Execute database and user setup commands in PostgreSQL
  # - Connects as superuser 'postgres' to create the database and user if they do not exist
  # - Uses DO $$ blocks to conditionally check for database and user existence, preventing duplication
  # - Provides NOTICE statements if database/user already exists, informing the user of each step
  echo "$(date): Setting up PostgreSQL database and user..."
  sudo -u postgres psql <<EOF

  -- Check if the database exists; create it only if it does not
  DO \$\$
  BEGIN
      IF NOT EXISTS (SELECT FROM pg_database WHERE datname = '$PG_DB_NAME') THEN
          PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE $PG_DB_NAME');
      ELSE
          RAISE NOTICE 'Database % already exists, skipping creation.', '$PG_DB_NAME';
      END IF;
  END\$\$;

  -- Check if the user exists; create it only if it does not
  DO \$\$
  BEGIN
      IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$DB_USER') THEN
          EXECUTE format('CREATE USER %I WITH PASSWORD %L', '$DB_USER', '$DB_PASSWORD');
      ELSE
          RAISE NOTICE 'User % already exists, skipping creation.', '$DB_USER';
      END IF;
  END\$\$;

  -- Set user-specific configurations to ensure optimal settings for the project
   -- Ensures character support for internationalization
  ALTER ROLE $DB_USER SET client_encoding TO 'utf8';
  -- Prevents data anomalies during transactions
  ALTER ROLE $DB_USER SET default_transaction_isolation TO 'read committed';
  -- Standardizes timestamps for consistency
  ALTER ROLE $DB_USER SET timezone TO 'UTC';

  -- Grant full access on the database to the user, allowing complete data management capabilities
  GRANT ALL PRIVILEGES ON DATABASE $PG_DB_NAME TO $DB_USER;
EOF

  # Step 7: Validate the creation of the database and user
  # - Confirms that the database and user were created successfully
  # - If validation fails, exits with an error message to indicate setup was incomplete
  echo "$(date): Validating database and user creation..."

  # Check for the database by listing all databases and searching for PG_DB_NAME
  if sudo -u postgres psql -c "\l" | grep -q "$PG_DB_NAME"; then
    echo "Database '$PG_DB_NAME' exists."
  else
    echo "Error: Database '$PG_DB_NAME' was not created successfully."
    exit 1
  fi

  # Check for the user by listing roles and searching for DB_USER
  if sudo -u postgres psql -c "\du" | grep -q "$DB_USER"; then
    echo "User '$DB_USER' exists."
  else
    echo "Error: User '$DB_USER' was not created successfully."
    exit 1
  fi

  # Step 8: Confirm completion
  # - If the script reaches this point, the setup completed successfully
  echo "$(date): PostgreSQL setup complete. Database '$PG_DB_NAME' created with user '$DB_USER'."

} | tee -a "$LOGFILE"  # Log all output to both the console and LOGFILE
