Sample app for aeron-cluster
=

App models single integer counter and provides http endpoints to read/change its value. 

To run:
=

Run confgurations are located in [./run](/.run) folder.

Alternatively, you can create 3 run configurations (one for each node) in intellij idea:

Main class: `me.mkulak.cluster.MainKt`

VM options: `--add-opens java.base/sun.nio.ch=ALL-UNNAMED -Daeron.cluster.ingress.channel=aeron:udp -Daeron.dir.delete.on.start=true`

environment vars: 
* `HTTP_PORT=8080;NODE_ID=0` for first node
* `HTTP_PORT=8081;NODE_ID=1` for second node
* `HTTP_PORT=8082;NODE_ID=2` for third node

To interact:
=
Sample requests are [here](req.http)

