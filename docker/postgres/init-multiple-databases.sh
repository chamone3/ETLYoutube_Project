#! /bin/bash
set -e
set -u

function create_user_and_database() {
    local database=$1
    local user=$2
    local password=$3

    echo "  Creating user '$user' with password '$password'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE USER $user WITH PASSWORD '$password';
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $user;
EOSQL
    echo "Database '$database' and user '$user' created successfully"
}

create_user_and_database $METADATA_DATABASE_NAME $METADATA_DATABASE_USERNAME $METADATA_DATABASE_PASSWORD
create_user_and_database $ELT_DATABASE_NAME $ELT_DATABASE_USERNAME $ELT_DATABASE_PASSWORD
create_user_and_database $CELERY_BACKEND_NAME $CELERY_BACKEND_USERNAME $CELERY_BACKEND_PASSWORD

echo "All databases created successfully."