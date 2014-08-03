crypto = require 'crypto'
kwfn = require 'keyword-arguments'
signal = require('too-late')()
{promise} = require 'when'

log = require('./logger') 'rpcclient'
{getConnectionPool} = require './ConnectionPool'


class RpcClient

    constructor: ({@url, @exchange, @topic, @version, @timeout, @ttl, @noAck, @delay})->
        @consumers = {}
        @ttl or= 60000
        @delay or= 1000
        @replyQ = "reply_#{crypto.randomBytes(16).toString 'hex'}"
        log.debug "queue: #{@replyQ}"

    connect: ->
        unless @q
            @q = promise (resolve, reject)=>
                (getConnectionPool @delay).connect @url, (connection)=>
                    connection.createChannel().then (@channel)=>

                        @channel.assertExchange @replyQ, 'direct', autoDelete: yes, durable: no
                        @channel.assertQueue @replyQ, autoDelete: yes, durable: no
                        @channel.bindQueue @replyQ, @replyQ, @replyQ

                        onMsg = (msg)=>
                            decoded = JSON.parse msg.content.toString()
                            content = JSON.parse decoded['oslo.message']
                            return if content.ending
                            signal.deliver content._msg_id, content

                        @channel.consume(@replyQ, onMsg, noAck: @noAck).then =>
                            log.info "wait for result on queue #{@replyQ}"
                            resolve this
                        .then null, (error)->
                            reject error
                    .then null, (error)->
                        reject error
        @q

    call: (namespace, context, method, args)->
        log.debug namespace, context, method, args
        promise (resolve, reject)=>
            @connect().then =>
                msgId = crypto.randomBytes(16).toString 'hex'
                payload =
                    _msg_id: msgId
                    _reply_q: @replyQ
                    _unique_id: crypto.randomBytes(16).toString 'hex'
                    args: args
                    method: method
                    version: @version
                log.debug msgId, payload
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
            .then null, (error)->
                reject error

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

module.exports = RpcClient