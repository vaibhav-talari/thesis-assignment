# thesis-assignment

# Project structure
Created project with maven. Step at https://nightlies.apache.org/flink/flink-docs-release-1.2/quickstart/java_api_quickstart.html

`mvn archetype:generate -DarchetypeGroupId=org.apache.flink -DarchetypeArtifactId=flink-quickstart-java -DarchetypeVersion=1.2.1`

# Build

`mvn clean install -Pbuild-jar`

## Pull docker image
refer - https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/standalone/docker/

## Flink tumbling window are taken in absolute time
Ex: Event - (00.30.00, e1), (01.00.00, e2), (01.30.00, e3), (02.00.00, e4), (02.30.00, e5) for a window size of 2 hr will have: window 1 (00.00.00 - 01.59.59): [e1,e2,e3] and window 2 (02.00.00 - 03.59.59): [e4.e5]. 
https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/windows/#tumbling-windows
