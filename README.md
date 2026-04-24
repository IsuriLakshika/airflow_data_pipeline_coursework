# Airflow Weather Data Pipeline

## Quick Start (VS Code Terminal)

### Step 1 – Install Docker Desktop
Download from https://www.docker.com/products/docker-desktop and install.
After install, open Docker Desktop and wait for the whale icon to show "Running".

### Step 2 – Open Project in VS Code
1. Open VS Code
2. File → Open Folder → select this `airflow_pipeline` folder
3. Open the integrated terminal: Terminal → New Terminal (Ctrl+`)

### Step 3 – Start Airflow
```bash
# In VS Code terminal, inside the airflow_pipeline folder:
docker compose up -d
```
Wait ~60 seconds for all containers to start.

### Step 4 – Verify containers
```bash
docker ps
```
You should see: airflow-webserver, airflow-scheduler, airflow-init, postgres

### Step 5 – Open Airflow UI
Open browser → http://localhost:8080
Login: admin / admin

### Step 6 – Enable and trigger the DAG
1. In the Airflow UI find "weather_data_pipeline"
2. Toggle the blue button to ENABLE it
3. Click the ▶ (play) button → "Trigger DAG"
4. Click on the DAG name to see task execution

### Step 7 – Verify data in PostgreSQL
```bash
docker exec -it airflow_pipeline-postgres-1 psql -U airflow -d airflow -c "SELECT * FROM weather_data LIMIT 5;"
```

### Step 8 – Run Jupyter Notebook for Visualization
```bash
pip install -r requirements.txt
cd notebooks
jupyter notebook visualization.ipynb
```
Change host='postgres' to host='localhost' in the notebook before running.

### Stop everything
```bash
docker compose down
```
