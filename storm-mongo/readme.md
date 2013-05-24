This is an attempt at an "enterprise" solution for Goldbach's conjecture.  It makes use of the [Storm](http://storm-project.net/) distributed computing framework and [MongoDB](http://www.mongodb.org/) for storing a pre-computed list of primes dowloaded from [The Prime Pages](http://primes.utm.edu/).

## Instructions:

  - Download [Storm](http://storm-project.net) and place the storm.jar and all of the lib/*.jar into the lib folder of this project.  This has been tested with 0.8.3.
  - Download the MongoDB [Java Client](https://github.com/mongodb/mongo-java-driver/downloads) and place in the lib folder of this project.
  - Make sure mongod is running locally with default port and password.  Support hasn't been added to configure a different Mongo host.
  - Run the getprimezips script in the scripts folder of this project.
  - Run the cleanprimetxts script in the scripts folder of this project.
  - Run the mongo importer in the scripts folder of this project.
  ```php mongoload.php```
  - Compile the storm topology.
  ```javac -cp "lib/*" src/*.java -d build```
  - Execute the storm topology.
  ```cd build; java -cp "../lib/*:." GoldbachTopology```
