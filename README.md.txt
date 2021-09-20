--Nordea bank Data engg problem
--Date:19-Sep-2021
--Shrikant Bhadane, Warsaw


Prerequisites:
--------------
java version "1.8.0_271"
kafka_2.12-2.8.0
zookeeper 3.5
python 3.9.0

Python packages used:
---------------------
xml.etree.ElementTree
pandas
json
operator
csv
confluent_kafka
socket
argparse

Steps to run:
--------------
I have installed both zookeeper and kafka locally

1.Start zookeeper
C:\D_Drive\kafka_2.12-2.8.0\bin\windows\zookeeper-server-start.bat C:\D_Drive\kafka_2.12-2.8.0\config\zookeeper.properties

2.start kafka
C:\D_Drive\kafka_2.12-2.8.0\bin\windows\kafka-server-start.bat C:\D_Drive\kafka_2.12-2.8.0\config\server.properties

--Make sure both should started
3.run consumer script in 1 window with below command
	cd <local dir>\streaming_solution\bin
	python subStream.py my-stream

4.Run below command which will read data from file and provide to consumer
	cd <local dir>\streaming_solution\
	python ./bin/pubStream.py ./data/outfile.csv my-stream --speed 2


Output:
--------
Display on terminal also data is stored in output file data/outfile.csv
Country,City,population,Date(YYYY-MM-DD),measured_at_ts,Temp_Cel,Temp_Far

Requirements Status:
--------------------
## Requirement 1 - sourcing, storing and displaying data --> Done
## Requirement 2 - transformations	--> Done
## Requirement 3 - multiple streams	--> Done
## Requirement 4 - retention of data	--> Not Done
## Requirement 5 - Slowing changing dimension Type 2	--> Not Done

Exceptions Handled:
--------------------
1.Incorrect format inside XML
2.Incorrect value for the given unit
3.Non XML files in the folder.

Commands to stop
-----------------
--Stop zookeeper,kafka
C:\D_Drive\kafka_2.12-2.8.0\bin\windows\zookeeper-server-stop
C:\D_Drive\kafka_2.12-2.8.0\bin\windows\kafka-server-stop