# FIX: Change this link, before it was /home/ubuntu/Documents/DE300/lab_doc/lab7/ml
docker run -v C:/Users/great/Documents/GitHub/DE300/lab7/ml:/tmp/ml -it \
           -p 8888:8888 \
           --name spark-sql-container \
	   pyspark-image