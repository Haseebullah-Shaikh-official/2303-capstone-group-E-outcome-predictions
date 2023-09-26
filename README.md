## Outcome Predictions

### Problem Statement
- Our goal is to optimize the process of finding an ideal councillor for patients, ensuring:
- **Increased Success**
- **Affordability**
- **Minimal Time Investment**

### Pipline Architecture
- The data pipeline begins with the extraction of data from designated APIs.
- The extracted data is then transformed using the power of PySpark.
- The processed data, referred as "Outcome Predictions," is seamlessly loaded into a Postgres database.
- Transformation is implemented in three distinct ways.
- To facilitate easy access to the transformed data, we have exposed an API that efficiently retrieves the stored information.
- Each service in our system is Dockerized


### Requirements
- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)


### Commands to get started
- Run the postgres db service first, to make sure it should be running before transformation
```bash
docker compose up postgres
```
- To run transformation and expose api service
```bash
docker compose up
```
- Wait few minutes so all requirments can be installed and transformation results processed and exposed through the api
- Make outcome predictions, to get success success rate, avg time and cost spent for each conucillor
link:http://localhost:8000/councillor_id , example: http://localhost:8000/55

### Contributors
- [Haseebullah (Team lead)](https://github.com/Haseebullah-Shaikh-official)
- [Huzaifa Waseem](https://github.com/Huzaifawaseem)
- [Hammad Irshad](https://github.com/hammadirshad19)
- [Hamza Asim](https://github.com/hamzaasim3639)
- [Humza Moeen](https://github.com/MHumza1731)



## References
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
https://fastapi.tiangolo.com/lo/ https://fastapi.tiangolo.com/advanced/using-request-directly/
https://www.dataquest.io/blog/python-api-tutorial/ https://realpython.com/python-requests/
https://apscheduler.readthedocs.io/en/3.x/
https://apscheduler.readthedocs.io/en/latest/modules/events.html#module-apscheduler.events
https://realpython.com/python-requests/
https://realpython.com/python-requests/
https://medium.com/@arturocuicas/fastapi-with-postgresql-part-1-70a3960fb6ee
https://docs.python.org/3/library/unittest.mock.html
https://pypi.org/project/testing.postgresql/
Grzegorz Jurdzinski testing examples.
