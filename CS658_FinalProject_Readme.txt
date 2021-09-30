Compilation:
	javac Nimbus.java
	javac Workers.java 
	javac -cp .:twitter4j-core-4.0.4.jar:twitter4j-stream-4.0.4.jar Spout_Bolt_Classes.java

1) First start the nimbus node
         This will print the IP address and the port number its listening on.
	java Nimbus
2) Then start all the workers
	java Workers
3) After starting the nimbus and workers, You can enter the topology configuration files in the commands provided by Nimbus
        Enter 1 and then enter the TopologyFileName - GenerateNumbersTopo 
							        FileReaderTopo
						                   TwitterStreamTopo
