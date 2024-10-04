# Choose the same Airflow version as your Helm deployment
FROM apache/airflow:2.9.3

# Set the working directory to AIRFLOW_HOME
WORKDIR /opt/airflow

# Add your plugin directory
COPY plugins/ /opt/airflow/plugins

# Install any necessary dependencies
# Uncomment and modify the next line if you have additional Python packages to install
# RUN pip install --no-cache-dir -r requirements.txt