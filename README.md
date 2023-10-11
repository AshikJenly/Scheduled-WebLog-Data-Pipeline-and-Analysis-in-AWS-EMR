```markdown
# Weekly Scheduled WebLog Data Pipeline and Analysis in AWS EMR

Processing weblog data to extract insights using AWS EMR cluster computing service with Apache Spark, and scheduling weekly log analysis of data from Amazon S3.

## Tools Used

### Big Data Tools
- Spark

### Automation and Pipelining Tool
- Airflow

### Big Data Computation Environment
- AWS EMR (Data Processing)
- EC2 (ubuntu) (Scheduling and Automation)

### Programming Language
- Python

## Project Architecture
![Project Architecture](Screenshot from 2023-10-11 21-46-52.png)

## Process DAG
![Process DAG](Screenshot from 2023-10-11 21-46-52.png)

## How to Execute the Project

### Step 1: Create an AWS EMR instance
- Refer to AWS documentation for guidance.

### Step 2: Create an EC2 Virtual Machine (Linux)
- Refer to AWS documentation for guidance.

### Step 3: Set up an AWS S3 Bucket
- Refer to AWS documentation for guidance.

### Step 4: In the Virtual Machine
- Install Apache Airflow
    - Start Airflow
      ```bash
      $ airflow standalone
      ```
- Place all DAG code inside a folder and configure it in `airflow.cfg`
  - Location:
    `/path/to/airflow/airflow.cfg`

### Step 5: Open Airflow UI
In your browser, access the following path:
```
<your_instance_aws_ip>:8080/dags/
```

Make sure to replace `/path/to` with the appropriate path.
```
