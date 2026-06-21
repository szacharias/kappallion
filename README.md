init commit

This project is used as a practice project with the intention to demonstrate a end to end version of the Kappa/Medallion architecture
Kappa is selected as it's a light weight streaming architecture that is easy to implement and requires less codebase as compared to lambda architecture.

I choose the following technologies for this project:
- Kafka: For streaming data  
- Spark: For data processing
- Python: For streaming pipeline and simulator
- Docker: For containerization
- Docker-compose: For orchestrating the services
- Some sort of service to emulate a streaming data format. TBD
- Data storage is also TBD. Considering between Delta Lake and Iceberg

The project is structured as follows:
- apps/: For Python code
- docker/: For Dockerfiles
- docker-compose.yml: For orchestrating the 