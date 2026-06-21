init commit

This project is used as a practice project with the intention to demonstrate a end to end version of the Kappa/Medallion architecture
Kappa is selected as it's a light weight streaming architecture that is easy to implement and requires less codebase as compared to lambda architecture.

I choose the following technologies for this project:
- Kafka: For streaming data  
- Spark: For data processing
- Python: For streaming pipeline and simulator 3.11-slim
- Docker: For containerization
- Docker-compose: For orchestrating the services
- Minio : to Emulate object storage
- Delta Lake: Used as the data management solution 

The project is structured as follows:
- apps/: For Python code
- docker/: For Dockerfiles
- docker-compose.yml: For orchestrating the 

Sample datasets included in git for reference: 
- [dataset-sample/Fruit Spoilage Prediction in an IoT Enabled Cold Storage System Dataset.csv](dataset-sample/Fruit%20Spoliage%20Prediction%20in%20an%20IoT%20Enabled%20Cold%20Storage%20System%20Dataset.csv)
- [dataset-sample/smart-logistics-supply-chain-dataset/delivery_status.csv](dataset-sample/smart-logistics-supply-chain-dataset/delivery_status.csv)

