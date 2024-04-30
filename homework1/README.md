
# HW 1 notes DE 300

## Setup Guide:

1. Pull my HW1 folder containing my python notebook from GitHub to the ES2 instance you are using.
2. Navigate to the folder with the Dockerfile.
3. Run the following command to build the docker image:
``docker build -t hw1_image .``   
Note: Please prefix with `sudo` if you run into permission issues.

4. Now, let's pull a MySql image:
	``docker pull mysql:8.3.0``

5. Create a docker network:
``docker network create net``

6. This command is creating and starting up a new Docker container for the MySQL database:
``sudo  docker  run  -d  --name  database  \
-e  MYSQL_ROOT_PASSWORD=root  \
-e  MYSQL_DATABASE=mydb  \
--network  net  \
mysql``
8. Run the container:
``
sudo docker run -p 8888:8888 --network net --name my_container hw1_image
``
 This will eventually output a link in the console that can be navigated to start the Jupyter notebook.
9. Finally, enter my HW1 folder and you should be able to run `HW1.ipynb` and work with the MySQL database. To do this, navigate to the Jupyter notebook link in your web browser.