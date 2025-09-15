# Airflow-DAG-for-API-ETL-Process
ETL pipeline orchestrated with Apache Airflow running on Docker Compose, designed to extract flight departures from the Aviationstack API, normalize and process the data using pandas, and load the results into a CSV file for analysis and downstream applications.

This repository demonstrates how to run **Apache Airflow** with **Docker Compose** to orchestrate an ETL pipeline that:
- Extracts departures data from the **Aviationstack API** (Rio de Janeiro International Airport – Galeão, `dep_iata=GIG`),
- Transforms and normalizes nested JSON responses with **pandas**,
- Enriches the dataset with codeshare mapping and date/time breakdown,
- Loads the processed data into a CSV file for downstream use.
  
---  
  
## Project Structure    
  
airflow-aviationstack/  
├─ dags/  
│ └─ aviationstack_departures_gig.py # Main DAG  
├─ data/ # Output CSVs (mapped from container)  
├─ logs/ # Airflow logs  
├─ plugins/ # Optional custom plugins  
├─ docker-compose.yml # Docker services definition  
├─ requirements.txt # Python dependencies (pandas, requests)  
└─ .env # Environment variables (API key, etc.)  
    
  
## Prerequisites  
  
- Docker + Docker Compose installed
- A valid **Aviationstack API Key** (free subscription available but limited to 100 results)
  
  
  
## Setup Instructions  

### 1. Clone the repository  
  
```bash
git clone https://github.com/your-username/Airflow-DAG-with-Docker-for-Aviationstack-API-ETL-Process.git
cd Airflow-DAG-with-Docker-for-Aviationstack-API-ETL-Process
```
  
### 2. Create the .env file 
  
Copy .env.example to .env and configure:  
```bash
cp .env.example .env
```
  
Edit .env: 
  
AIRFLOW_UID=50000  
AVIATIONSTACK_API_KEY=your_real_api_key  
TZ=America/Sao_Paulo  
  
### 3. Start Airflow  
  
Initialize the metadata database and bring up services:
```bash 
docker compose up airflow-init
docker compose up -d --force-recreate
```  
  
#### The Airflow UI will be available at:  
http://localhost:8080 (default: user admin, password admin)  

### 4. Install Python dependencies inside containers  
  
If not already baked into the image, install pandas and requests:    
```bash
docker exec -it airflow-airflow-webserver-1 bash -lc "pip install --no-cache-dir pandas==2.2.2 requests==2.32.3"
docker exec -it airflow-airflow-scheduler-1 bash -lc "pip install --no-cache-dir pandas==2.2.2 requests==2.32.3"
```  

### 5. Verify if DAG is loaded  
  
In the Airflow UI, the DAG aviationstack_departures_gig should be listed.  
Unpause it and Trigger DAG manually to execute.  


## API Key Configuration  

The DAG reads the Aviationstack API key from:  
Environment variable AVIATIONSTACK_API_KEY (via .env)  
    

## Output Location  
  
Inside the container:  
/opt/airflow/data/Departures.csv  
  
On the host (via volume mapping in docker-compose.yml):  
./data/Departures.csv  
  

## Troubleshooting  

DAG not visible → check UI (Browse → DAG Import Errors) or container logs.  
  
Missing pandas/requests → run the install commands from step 4.  
  
API key error → ensure .env is loaded or add as Airflow Variable.  
  
CSV not visible on host → confirm ./data:/opt/airflow/data mapping in docker-compose.yml.  

    
---

## License  
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.  
  
For a ETL pipeline using Cron Job, take a look at [Aviationstack API ETL with Daily Cron Job](https://github.com/rodolfoplng/Portfolio/blob/main/Aviationstack%20ETL%20Cron.md)  
  
Notebook with the API extraction and analysis: [API Request and Extraction from Aviationstack](https://github.com/rodolfoplng/Portfolio/blob/main/API%20Request%20and%20Extraction%20Aviationstack.ipynb)  
  
For more projects, check out my [Portfolio Repository](https://github.com/rodolfoplng/Portfolio)  














