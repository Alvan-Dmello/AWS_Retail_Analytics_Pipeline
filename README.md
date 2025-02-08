# AWS-Retail-Analytics-Pipeline

## Project Overview  
This project focuses on building a **scalable data pipeline** using **AWS and PySpark** to process and analyze a retail store dataset. The pipeline automates data ingestion, transformation, storage, and visualization while integrating **AWS SageMaker Autopilot** for machine learning and **AWS QuickSight** for interactive dashboards.

## Features  
- **Automated Data Pipeline**: Uses **cron jobs** to schedule data processing and updates.  
- **Big Data Processing**: Utilizes **PySpark** for efficient data transformation and aggregation.  
- **AWS Integration**: Stores raw and processed data in **AWS S3** for scalability.  
- **Machine Learning**: Leverages **AWS SageMaker Autopilot** for automated model training.  
- **Data Visualization**: Creates **interactive dashboards** using **AWS QuickSight**.  

## Tech Stack  
- **AWS Services**: S3, SageMaker, QuickSight  
- **Data Processing**: PySpark, Spark SQL  
- **Automation**: Cron Jobs  
- **Storage**: AWS S3  

## Project Structure  
├── Aggregated Data/ # Data after running various aggregare queries in pyspark
├── Code Files/ # PySpark and SQL scripts
├── Preprocessed Data/ # Final Data after transformation and cleaning
├── Dataset.csv # Raw dataset used for processing
├── Project_Report.pdf # Detailed report on the project
├── Quicksight_Dashboard.pdf # Dashboard visualizations and insights
├── README.md # Project documentation

