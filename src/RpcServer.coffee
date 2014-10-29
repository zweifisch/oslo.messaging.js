crypto = require 'crypto'
kwfn = require 'keyword-arguments'

Connection = require './Connection'


class RpcServer

    constructor: ({@url, @exchange, @topic, @version, @timeout, @ttl, @noAck, @delay, @maxRetry})->
        @consumers = {}
        @ttl or= 60000
        @delay or= 1000
        @maxRetry or= 3
        @replayQ = "reply_#{crypto.randomBytes(16).toString 'hex'}"
        @connection = new Connection
            delay: @delay
            urls: @url
            maxRetry: @maxRetry

    on: (namespace, method, callback)->
        @connection.connect().then =>
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
                            callback args

            key = "#{namespace}:#{method}"
            if key of @consumers
                @consumers[key].push kwfn callback
            else
                @consumers[key]= [kwfn callback]
            this

module.exports = RpcServer
