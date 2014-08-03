# oslo.messaging.js

nodejs client for oslo.messaging

## usage

```javascript
var messaging = require('oslo.messaging');

var client = new messaging.RpcClient({
    url: 'amqp://localhost',
    exchange: 'exchange',
    topic: 'topic',
    version: '1.0',
    timeout: 5000,
    noAck: false,
    delay: 1000 // reconnection delay
});

context = {}

client.call('name.space', context, 'method', {param: 1, param2: false}).then(function(data) {
    console.log(data);
}).catch(function(err) {
    console.log(err);
});
```
