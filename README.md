# Flight Data App
To test and build:
```
docker build -t flight-data .
```

To run, copy CSV files to `data` directory and run:
```
docker run --volume `realpath data`:/data -it --rm flight-data /data/flightData.csv /data/passengers.csv
```

The program will pause before each stage. Press ENTER so the results are calculated.
