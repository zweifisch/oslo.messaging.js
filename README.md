# oslo.messaging.js

nodejs client for oslo.messaging

## usage

```javascript
var Client = require('oslo.messaging');

var client = new Client({
    url: 'amqp://localhost',
    exchange: 'exchange',
    topic: 'topic',
    version: '1.0',
    timeout: 5000
});

client.call('name.space', {context:null}, 'method', param).then(function(data) {
    console.log(data);
}).catch(function(err) {
    console.log(err);
});
```
