# Create db
airflow db init

# Create uesr
airflow users create --username admin --firstname Hung --lastname Pham --role Admin --email phamhung3589@gmail.com

# start the web server, default port is 8080
airflow webserver -p 8081 -D

# start the scheduler
airflow scheduler -D

# kill all airflow job
kill $(ps aux | grep 'airflow' | awk '{print $2}')
