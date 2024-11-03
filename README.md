# Master's Thesis Assignment "Finding more needles in the haystack"

The exercise:

- Write a data generator in Java that creates hourly readings for 10 households. The generated data should be timestamp sorted.
- Write a Flink query (in Java) that reads this data and that calculates the average over tumbling windows of 6 hours in event time. Use Flink CEP to find sequences with at least 3 consecutive growing averages (on a per-household basis)
- The parallelism degree of the query should be an input parameter.

# Project Structure
- `EMeterEvent.java`: Java POJO class to represent a stream tuple
- `HouseDataGenerator`: Stream data generator
- `HouseDataGeneratorToSocket`: Stream data generator writes to a socket
- `HouseWindowOperation`: Operation (average calculation) done in time window
- `LimitedEventTester`: Quick Tester class. All helper classes available with in the same class

## Project Creation
- To create a template Flink repository with Maven: `mvn archetype:generate -DarchetypeGroupId=org.apache.flink -DarchetypeArtifactId=flink-quickstart-java -DarchetypeVersion=1.20.0`. Refer: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/configuration/overview/

# Build
- Create JAR file `mvn clean package`
- Submit job `./bin/flink run thesis-assignment/target/flink-assignment-1.0.jar 30`

# Flink
Some useful reference.

## Flink tumbling window are taken in absolute time
Ex: Event - (00.30.00, e1), (01.00.00, e2), (01.30.00, e3), (02.00.00, e4), (02.30.00, e5) for a window size of 2 hr will have: window 1 (00.00.00 - 01.59.59): [e1,e2,e3] and window 2 (02.00.00 - 03.59.59): [e4.e5]. 
https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/windows/#tumbling-windows

## How to use Watermarking strategies
https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/event-time/generating_watermarks/#introduction-to-watermark-strategies

## Buildin Watermark Generator
https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/event-time/built_in/#monotonously-increasing-timestamps

## Data Generation with `GeneratorFunction`
https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/datagen/

## CEP patterns refernce 
https://nightlies.apache.org/flink/flink-docs-master/docs/libs/cep/#combining-patterns

## reference git links
- https://github.com/metrolinkai/Datorios/tree/main/flink-examples/flink-examples-java
- https://github.com/CarlosSanabriaM/flink-basics/tree/master
- https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/statemachine/StateMachineExample.java
- https://github.com/eleniKougiou/Flink-cep-examples/blob/main/src/main/java/flinkCEP/cases/CEPCase.java
- https://flink.apache.org/2016/04/06/introducing-complex-event-processing-cep-with-apache-flink/
- https://github.com/streaming-with-flink/examples-java/blob/master/src/main/java/io/github/streamingwithflink/
