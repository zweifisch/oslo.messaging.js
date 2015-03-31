crypto = require 'crypto'
{EventEmitter} = require 'events'
{promise} = require 'when'

log = require('./logger') 'notifier'
Connection = require './Connection'


class Notifier extends EventEmitter

    constructor: ({@url, @prefix, @topic, @exchange, @retryDelay, @noAck, @queue, @maxRetry, @heartbeat})->
        @retryDelay ?= 3000
        @maxRetry ?= 3
        @heartbeat ?= 10
        @queue ?= "#{@prefix || 'notifier'}_#{crypto.randomBytes(16).toString 'hex'}"
        log.debug "queue: #{@queue}"
        @connection = Connection.getConnection
            retryDelay: @retryDelay
            urls: @url
            maxRetry: @maxRetry
            heartbeat: @heartbeat

    connect: ->
        unless @q
            @q = promise (resolve, reject)=>
                @connection.connect (connection)=>
                    connection.createChannel().then (@channel)=>

                        @channel.assertExchange @exchange, 'topic', autoDelete: no, durable: no
                        .then => log.info "exchange #{@exchange} asserted"
                        .then null, (error)=>
                            @emit 'error', error

                        @channel.assertQueue @queue, autoDelete: yes, durable: no
                        .then => log.info "queue #{@queue} asserted"
                        .then null, (error)=>
                            @emit 'error', error

                        @channel.bindQueue @queue, @exchange, @topic
                        .then => log.info "topic #{@topic} binded"
                        .then null, (error)=>
                            @emit 'error', error

                        consumer = (msg)=>
                            decoded = JSON.parse msg.content.toString()
                            log.debug decoded
                            if @noAck
                                @callback decoded
                            else
                                @callback decoded, => @channel.ack msg

                        consumeQ = @channel.consume @queue, consumer, noAck: @noAck
                        consumeQ.then =>
                            log.info "wait for notification on queue #{@queue}"
                            @emit 'ready', @queue
                            resolve this
                        consumeQ.then null, (error)=>
                            @emit 'error', error
                            reject error
        @q

    onMessage: (callback)->
        @callback = callback

module.exports = Notifier
