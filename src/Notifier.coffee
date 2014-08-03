crypto = require 'crypto'
{promise} = require 'when'

log = require('./logger') 'notifier'
{getConnectionPool} = require './ConnectionPool'


class Notifier

    constructor: ({@url, @prefix, @topic, @exchange, @delay, @noAck})->
        @delay ?= 1000
        @queue = "#{@prefix || 'notifier'}_#{crypto.randomBytes(16).toString 'hex'}"
        log.debug "queue: #{@queue}"

    connect: ->
        unless @q
            @q = promise (resolve, reject)=>
                (getConnectionPool @delay).connect @url, (connection)=>
                    connection.createChannel().then (@channel)=>

                        @channel.assertExchange @exchange, 'topic', autoDelete: no, durable: no
                        .then => log.info "exchange #{@exchange} asserted"
                        .then null, (error)=> log.error error

                        @channel.assertQueue @queue, autoDelete: yes, durable: no
                        .then => log.info "queue #{@queue} asserted"
                        .then null, (error)=> log.error error

                        @channel.bindQueue @queue, @exchange, @topic
                        .then => log.info "topic #{@topic} binded"
                        .then null, (error)=> log.error error

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
                            resolve this
                        consumeQ.then null, (error)=>
                            reject error
        @q

    onMessage: (callback)->
        @callback = callback

module.exports = Notifier
