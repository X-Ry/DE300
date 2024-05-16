# If needed, update the directory that word-count is located at, using the absolute path.

docker run -v C:/Users/great/Documents/GitHub/DE300/lab6/word-count:/tmp/wc-demo -it \
           -p 8888:8888 \
           --name wc-container \
           pyspark-image