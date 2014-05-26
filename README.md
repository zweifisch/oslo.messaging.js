# oslo.messaging.js

nodejs client for oslo.messaging

## usage

```
Client = require 'oslo.messaging'

client = new Client
    url: 'amqp://localhost'
    exchange: 'exchange'
    topic: 'topic'
    version: '1.0'
    timeout: 5000

client.connect().then ->
    client.call 'name.space', 'method', param
    .then (data)->
        console.log data
    .catch (err)->
        console.log err
.catch (err)->
    console.log err
```
