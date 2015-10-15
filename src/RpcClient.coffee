crypto = require 'crypto'
kwfn = require 'keyword-arguments'
signal = require('too-late')()
{EventEmitter} = require 'events'

log = require('./logger') 'rpcclient'
ConnectionManager = require './ConnectionManager'


onMsg = (msg)=>
    decoded = JSON.parse msg.content.toString()
    content = JSON.parse decoded['oslo.message']
    return if content.ending
    signal.deliver content._msg_id, content


class RpcClient extends EventEmitter

    constructor: ({@url, @exchange, @topic, @version, @timeout, @ttl, @noAck, retryDelay, @messageTtl, maxRetry, connectionTimeout})->
        @ttl or= 60000
        @replyQ = "reply_#{crypto.randomBytes(16).toString 'hex'}"
        log.debug "queue: #{@replyQ}"
        @connection = ConnectionManager.getConnection
            retryDelay: retryDelay or 3000
            urls: @url
            maxRetry: maxRetry or 3
            timeout: connectionTimeout
        @connection.on 'reconnected', @setup

    setup: (conn)=>

        @q = conn.createChannel().then (@channel)=>
            log.info "channel created"
            @channel.assertExchange @replyQ, 'direct',
                autoDelete: yes, durable: no
            .then =>
                log.info "exchange #{@replyQ} asserted"
                @channel.assertQueue @replyQ,
                    autoDelete: yes, durable: no, messageTtl: @messageTtl
            .then =>
                log.info "queue #{@replyQ} asserted"
                @channel.bindQueue @replyQ, @replyQ, @replyQ
            .then =>
                @channel.consume @replyQ, onMsg, noAck: @noAck
            .then =>
                log.info "wait for result on queue #{@replyQ}"
                @channel.once 'error', (e)=>
                    @emit "error", e
                @channel.once 'close', =>
                    log.error "about to recreate channel, channel closed"
                    @reconnect()
                this

    connect: ->
        @q or @q = @connection.connect().then @setup

    reconnect: ->
        @q = @connection.connect()

    call: (namespace, context, method, args)->
        log.debug "calling", namespace, method, context, args
        @connect().then =>
            msgId = crypto.randomBytes(16).toString 'hex'
            payload =
                _msg_id: msgId
                _reply_q: @replyQ
                _unique_id: crypto.randomBytes(16).toString 'hex'
                args: args
                method: method
                version: @version
            log.debug msgid: msgId, payload: payload
            if namespace
                payload.namespace = namespace
            if context
                for own key, value of context
                    payload["_context_#{key}"] = value

            payload = new Buffer JSON.stringify
                'oslo.message': JSON.stringify payload
                'oslo.version': '2.0'

            try
                @channel.publish @exchange, @topic, payload,
                    contentEncoding: 'utf-8'
                    contentType: 'application/json'
                    headers:
                        ttl: @ttl
                    priority: 0
                    deliveryMode: 2
            catch e
                @reconnect()
                @emit "error", e
                log.error "failed to publish, about to recreat channel", e
                return Promise.reject message: "Publish Failed, Please Retry", message_id: msgId

            new Promise (resolve, reject)=>
                signal.waitfor msgId, (data)->
                    if data.failure
                        log.info data.failure
                        reject JSON.parse data.failure
                    else
                        resolve data.result
                .till @timeout, ->
                    reject
                        message: 'timeout'
                        message_id: msgId
                        method: method
                        namespace: namespace

module.exports = RpcClient
