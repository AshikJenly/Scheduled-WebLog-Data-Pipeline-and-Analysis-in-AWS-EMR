---

# Scheduled WebLog Data Pipeline and Analysis in AWS EMR

Process weblog data to extract insights using the AWS EMR cluster computing service with Apache Spark and schedule weekly log analysis of data from Amazon S3.

## Tools Used

### Big Data Tools
- Spark

### Automation and Pipelining Tool
- Airflow

### Big Data Computation Environment
- AWS EMR (Data Processing)
- EC2 (Ubuntu) (Scheduling and Automation)

### Programming Language
- Python

## Project Architecture
![Screenshot from 2023-10-12 00-18-57](https://github.com/AshikJenly/Scheduled-WebLog-Data-Pipeline-and-Analysis-in-AWS-EMR/assets/116492348/23d7f563-2ad4-4373-90b0-b02ebf0bbb37)


## Process DAG
![Screenshot from 2023-10-11 21-46-52](https://github.com/AshikJenly/Scheduled-WebLog-Data-Pipeline-and-Analysis-in-AWS-EMR/assets/116492348/6561440a-b19a-4ed5-93ff-77162eb57e94)

## How to Execute the Project

### Step 1: Create an AWS EMR Instance
- Refer to AWS documentation for guidance.

### Step 2: Create an EC2 Virtual Machine (Linux)
- Refer to AWS documentation for guidance.

### Step 3: Set up an AWS S3 Bucket
- Refer to AWS documentation for guidance.

### Step 4: On the Virtual Machine
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

---
