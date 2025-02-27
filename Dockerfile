# Choose the same Airflow version as your Helm deployment
FROM apache/airflow:2.9.3

# Install any necessary dependencies
# Uncomment and modify the next line if you have additional Python packages to install
# RUN pip install --no-cache-dir -r requirements.txt# Optionally set user and group ID if different from default
ARG AIRFLOW_UID=50000
ARG AIRFLOW_GID=50000

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER ${AIRFLOW_UID}

# Set the working directory to AIRFLOW_HOME
ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME

# RUN mkdir -p /home/airflow/.ssh && \
# chown -R ${AIRFLOW_UID}:${AIRFLOW_GID} /home/airflow/.ssh && \
# chmod 700 /home/airflow/.ssh/

# COPY --chown=${AIRFLOW_UID}:${AIRFLOW_GID} id_rsa /home/airflow/.ssh/id_rsa
# RUN chmod 600 /home/airflow/.ssh/id_rsa

RUN mkdir -p $AIRFLOW_HOME/slurm_scripts

# Upgrade pip and install required Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

# COPY --chown=${AIRFLOW_UID}:${AIRFLOW_GID} ./entrypoint.sh /entrypoint.sh

# RUN chmod +x /entrypoint.sh


#ENTRYPOINT ["/entrypoint.sh"]