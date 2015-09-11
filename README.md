# oslo.messaging.js

[![NPM Version][npm-image]][npm-url]

nodejs client for oslo.messaging

## usage

```javascript
var messaging = require('oslo.messaging');

var client = new messaging.RpcClient({
    url: 'amqp://localhost;amqp//10.0.0.10',
    exchange: 'exchange',
    topic: 'topic',
    version: '1.0',
    timeout: 5000,
    noAck: false,
    retryDelay: 3000
});

context = {}

client.call('name.space', context, 'method', {param: 1, param2: false}).then(function(data) {
    console.log(data);
}).catch(function(err) {
    console.log(err);
});
```

[npm-image]: https://img.shields.io/npm/v/oslo.messaging.svg?style=flat
[npm-url]: https://npmjs.org/package/oslo.messaging
