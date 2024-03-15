Sample app for aeron-cluster
=

To run:
=

Create 3 run configurations in intellij idea:

Main class: `me.mkulak.cluster.MainKt`

VM options: `--add-opens java.base/sun.nio.ch=ALL-UNNAMED -Daeron.cluster.ingress.channel=aeron:udp -Daeron.dir.delete.on.start=true`

environment vars: 
* `HTTP_PORT=8080;NODE_ID=0` for first node
* `HTTP_PORT=8081;NODE_ID=1` for second node
* `HTTP_PORT=8082;NODE_ID=2` for third node

Sample requests are [here](req.http)

