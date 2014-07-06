amqp = require 'amqplib'
crypto = require 'crypto'
wann = require 'when'
signal = require('too-late')()


getSignature = (fn)->
    params = /\(([\s\S]*?)\)/.exec fn
    if params and params[1].trim()
        params[1].split(',').map (x)-> x.trim()
    else
        []


class Client

    constructor: ({@url, @exchange, @topic, @version, @timeout, @ttl})->
        @consumers = {}
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
                        return if content.ending

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
                    version: @version
                if namespace
                    payload.namespace = namespace
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

    cast: (namespace, context, method, args)->
        @connect().then =>
            msgId = crypto.randomBytes(16).toString 'hex'
            payload =
                _msg_id: msgId
                _unique_id: crypto.randomBytes(16).toString 'hex'
                args: args
                method: method
                version: @version
            if namespace
                payload.namespace = namespace
            if context
                for own key, value of context
                    payload["_context_#{key}"] = value

            payload = new Buffer JSON.stringify
                'oslo.message': JSON.stringify payload
                'oslo.version': '2.0'

            @channel.publish @exchange, '#', payload,
                contentEncoding: 'utf-8'
                contentType: 'application/json'
                headers:
                    ttl: @ttl
                priority: 0
                deliveryMode: 2

    on: (namespace, method, callback)->
        @connect().then =>

            unless @fanoutQ
                @fanoutQ = "fanout_#{crypto.randomBytes(16).toString 'hex'}"
                @channel.assertQueue @fanoutQ, autoDelete: yes, durable: no
                @channel.bindQueue @fanoutQ, @exchange, '#'
                @channel.consume @fanoutQ, (msg)=>
                    decoded = JSON.parse msg.content.toString()
                    content = JSON.parse decoded['oslo.message']
                    return if content.ending

                    {namespace, method, args} = content
                    return unless method and args
                    key = if namespace then "#{namespace}:#{method}" else method
                    if key of @consumers
                        for callback in @consumers[key]
                            signature = getSignature callback
                            preparedParams = signature.map (name)-> args[name]
                            callback preparedParams...
                            for key of args
                                unless key in signature
                                    throw new Error "unexpected param '#{key}' for #{namespace} #{method}"

            key = "#{namespace}:#{method}"
            if key of @consumers
                @consumers[key].push callback
            else
                @consumers[key]= [callback]
            this

module.exports = Client
