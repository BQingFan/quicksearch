# 23sp-CIS5550-Team-quicksearch

The "main" folder contains the main code for the final result of the project, and the "HW_" folders contain each component implemented from our homework assignments.

How to Compile & Run the Solutions:

cd main

java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:lib/jsoup-1.16.1.jar:bin cis5550.kvs.Master 8000

(for as many workers as you want)
java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:lib/jsoup-1.16.1.jar:bin cis5550.kvs.Worker 8001 worker1 3.93.77.207:8000

javac -cp lib/webserver.jar:lib/jsoup-1.16.1.jar:lib/gson-2.8.6.jar:src -d ./bin/ src/cis5550/frontend/FrontendServer.java

java -cp lib/webserver.jar:lib/jsoup-1.16.1.jar:lib/gson-2.8.6.jar:bin cis5550.frontend.FrontendServer 8001 3.93.77.207:8000
(where 3.93.77.207 is the ip of the KVS Master)


Extra Libraries:

Jsoup - https://jsoup.org/download
Gson - https://github.com/google/gson
