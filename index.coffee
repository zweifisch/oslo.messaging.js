amqp = require 'amqplib'
crypto = require 'crypto'
wann = require 'when'
signal = require('too-late')()


class Client

    constructor: ({@url, @exchange, @topic, @version, @timeout, @ttl})->
        @ttl or= 60000

    connect: ->
        return @connectP if @connectP
        @connectP = wann.promise (resolve, reject)=>
            amqp.connect(@url).then (connection)=>
                connection.createChannel().then (@channel)=>
                    # @channel.assertExchange "#{@exchange}_fanout", 'topic'
                    # @channel.assertExchange @exchange, 'topic'
                    @replayQ = "reply_#{crypto.randomBytes(16).toString 'hex'}"

                    @channel.assertExchange @replayQ, 'direct', autoDelete: yes, durable: no
                    @channel.assertQueue @replayQ, autoDelete: yes, durable: no
                    @channel.bindQueue @replayQ, @replayQ, @replayQ

                    onMsg = (msg)=>
                        decoded = JSON.parse msg.content.toString()
                        content = JSON.parse decoded['oslo.message']
                        if content.ending then return
                        signal.deliver content._msg_id, content

                    @channel.consume(@replayQ, onMsg).then =>
                        resolve this
                    .then null, (err)->
                        reject err
                .then null, (err)->
                    reject err
            .then null, (err)->
                reject err

    call: (namespace, context, method, args)->
        wann.promise (resolve, reject)=>
            @connect().then =>
                msgId = crypto.randomBytes(16).toString 'hex'
                payload =
                    _msg_id: msgId
                    _reply_q: @replayQ
                    _unique_id: crypto.randomBytes(16).toString 'hex'
                    args: args
                    method: method
                    namespace: namespace
                    version: @version
                if context
                    for own key, value of context
                        payload["_context_#{key}"] = value

                payload = new Buffer JSON.stringify
                    'oslo.message': JSON.stringify payload
                    'oslo.version': '2.0'

                @channel.publish @exchange, @topic, payload,
                    contentEncoding: 'utf-8'
                    contentType: 'application/json'
                    headers:
                        ttl: @ttl
                    priority: 0
                    deliveryMode: 2

                do(msgId, reject, resolve)=>

                    signal.waitfor msgId, (data)->
                        if data.failure
                            reject JSON.parse data.failure
                        else
                            resolve data.result
                    .till @timeout, ->
                        reject
                            message: 'timeout'
                            message_id: msgId
            .then null, (err)->
                reject err


module.exports = Client
