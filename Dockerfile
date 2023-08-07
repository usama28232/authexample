FROM postgres:latest

# Copy your migration scripts and seed data to the container
COPY init.sql /docker-entrypoint-initdb.d/
COPY seed_data.sql /docker-entrypoint-initdb.d/seed_data.sql
