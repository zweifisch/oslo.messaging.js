crypto = require 'crypto'

log = require('./logger') 'notifier'
ConnectionManager = require './ConnectionManager'


class Notifier

    constructor: ({@url, @topic, @exchange, @queue, @noAck, retryDelay , maxRetry, connectionTimeout, prefix})->
        @queue ?= "#{prefix or 'notifier'}_#{crypto.randomBytes(16).toString 'hex'}"
        log.debug "queue: #{@queue}"
        @connection = ConnectionManager.getConnection
            retryDelay: retryDelay or 3000
            urls: @url
            maxRetry: maxRetry or 3
            timeout: connectionTimeout or 10
        @connection.on 'reconnected', @setup

    setup: (conn)=>

        conn.createChannel().then (channel)=>

            channel.assertExchange @exchange, 'topic',
                autoDelete: no, durable: no
            .then =>
                log.info "exchange #{@exchange} asserted"
                channel.assertQueue @queue, autoDelete: yes, durable: no
            .then =>
                log.info "queue #{@queue} asserted"
                channel.bindQueue @queue, @exchange, @topic
            .then =>
                log.info "topic #{@topic} binded"
                channel.consume @queue, (msg)=>
                    decoded = JSON.parse msg.content.toString()
                    log.debug decoded
                    @consume decoded, => channel.ack msg
                , noAck: @noAck
            .then =>
                log.info "wait for notification on queue #{@queue}"
                channel.on 'error', => @connect
                this

    connect: ->
        @q or= @connection.connect().then @setup

    onMessage: (callback)->
        @consume = callback

module.exports = Notifier
