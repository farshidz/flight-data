FROM openjdk:8

# Note context must be parent directory (docker)

RUN apt-get update &&\
    apt-get install -y apt-transport-https gnupg2 &&\
    echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list &&\
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 &&\
    apt-get update &&\
    apt-get install -y sbt=1.3.3

COPY project /app/source/project
COPY build.sbt /app/source
COPY src /app/source/src

RUN cd /app/source &&\
	sbt pack &&\
	cp -r target/pack /app/pack

FROM gettyimages/spark:2.4.1-hadoop-3.0

COPY --from=0 /app/pack/lib /app

ENTRYPOINT ["spark-submit", "--master", "local", "--conf", "spark.driver.extraClassPath=/app/*", \
			"/app/flight-data_2.11-0.1.jar"]
