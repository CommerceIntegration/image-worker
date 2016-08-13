# our base image
FROM alpine:latest
#COPY repositories.txt /etc/apk/repositories

# Install python and pip
RUN apk add --update py-pip

# upgrade pip
RUN pip install --upgrade pip

# install Python modules needed by the Python app
COPY requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r /usr/src/app/requirements.txt

# copy files required for the app to run
COPY main.py /usr/src/app/
#COPY templates/index.html /usr/src/app/templates/

# tell the port number the container should expose
#EXPOSE 5000

# run the application
CMD ["python", "/usr/src/app/main.py"]