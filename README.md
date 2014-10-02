Pastry
======

Using the actor model the Pastry protocol and a simple object access service to prove its usefulness. Implement in Scala.


        After compile,runing project3 numNodes  numRequests to run the program. NumNodes of node will join in the Pastry one by one. Each node will gets its own leaf tables and route tables by join. When one node built up and update all its tables, it will begin to send message. Each node will send numRequests message. Each request it use a random String in the ID range as the key. Passing each node will make the hop add 1.When the message arrives, the initial sender will receive a Message Arrive message and give the hops number it passed. When all nodes receive all the message arrive message for all the message be sent and the average number of hops will be calculated, the Pastry will stop and exit.
