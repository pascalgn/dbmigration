FROM microsoft/mssql-server-linux:latest

ENV ACCEPT_EULA Y
ENV SA_PASSWORD yohk4taebu0Hu6aYa9mahb0ai

# Copy initialization script
RUN mkdir -p /tmp/setup/
COPY initialize.* /tmp/setup/

# Convert Windows newline endings (CRLF)
RUN sed -i'.bak' 's/\x0D$//' /tmp/setup/*

# Run initialization script
RUN chmod 755 /tmp/setup/initialize.sh && /tmp/setup/initialize.sh && rm -rf /tmp/setup
