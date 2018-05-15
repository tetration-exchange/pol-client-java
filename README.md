# tnp-enforcement-client (Tetration Network Policy Enforcement Client)
This is a reference implementation of *Tetration Network Policy Enforcement Client* that can be used
by third party vendors in order to translate Tetration network policies intended for a network
device or appliance such as load balancer, firewall etc into its proprietary configuration
rules and to deploy them appropriately. That said, it should be noted that this code does not implement
enforcement of Tetration network policies for any specific network device.

Tetration system publishes network policies defined by Tetration users/administrators to
Apache Kafka on topics per root scope. In general this allows any consumers with access to a
Tetration cluster to retrieve Tetration network policies in form of Kafka messages and
to process them as needed. Note that access to Kafka is isolated per root scope and
read-only as one needs to download a client certificate (tar.gz) file from Tetration
portal in order to connect to Kafka.

The *tnp-enforcement-client* presented here performs the following tasks:
- Connect to Tetration cluster using the given client certificate and retrieve Tetration network
policies from Kafka
- Determine network policies intended for a network device by matching its IP address
- Calculate the changes between versions of network policies and deliver only the delta to an
event callback to be provided by third party vendors

Note that a full snapshot of network policies will be sent as first event during initialization
phase of this client code.

Thus, this project aims to help third party vendors focusing on their own enforcement implementation.

## Building
First make sure Java 8 JDK and [Apache Maven](https://maven.apache.org/) are installed.

```
# change to tnp-enforcement-client's source dir
# compile
mvn compile
# test (unit test only, not end-to-end)
mvn test
# alternatively
mvn verify
# build jar file
mvn -DskipTests package
# Build output is located in sub dir "target"
```

Note the unit test run can take up to 75-90 secs!

## Bug Reports and Enhancement Requests
Please use github's issues tab to file issues as well as any improvement suggestions.

## Requirements
- Java 8 JDK
- [Apache Maven](https://maven.apache.org/) version 3.3.9

The following libraries are required at runtime:

- [Apache Log4j](http://logging.apache.org/log4j): log4j-1.2.17.jar
- [Simple Logging Facade for Java](https://www.slf4j.org/): slf4j-api-1.7.25.jar, slf4j-log4j12-1.7.25.jar
- [Apache Kafka Clients](http://kafka.apache.org/): kafka-clients-1.0.0.jar
- [Protocol Buffers Core](https://github.com/google/protobuf): protobuf-java-3.4.1.jar

And these libraries are needed to run unit test:

- [Testng](http://testng.org/): testng-6.13.jar
- [Jcommander](http://beust.com/jcommander): jcommander-1.48.jar
- [Mockito Core](https://github.com/mockito/mockito): mockito-core-2.12.0.jar
- [Byte Buddy](http://bytebuddy.net): byte-buddy-1.7.9.jar, byte-buddy-agent-1.7.9.jar
- [Objenesis](http://objenesis.org/): objenesis-2.6.jar

## Deployment Scenarios
Needless to mention that this is a task to be implemented by third party vendors,
especially the packaging and installation of required software modules to respective
network appliances.

1. *tnp-enforcement-client runs on one network appliance*

  This is the simplest approach if required Java packages can be installed and run smoothly on the
  network appliance.

1. *tnp-enforcement-client runs on another machine*

  This method is recommended if it is not possible to install and run Java on the network appliance
  for any reasons. In this scenario another machine can run tnp-enforcement-client and deploy
  network policies to a bunch of network appliances. Note this requires remote communication between
  this machine and network appliances.

## License
Please refer to the file *LICENSE.pdf* in same directory of this README file.
