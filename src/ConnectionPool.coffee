amqp = require 'amqplib'
log = require('./logger') 'pool'


sanitize = (url)->
    url.replace /\/\/[^@]*@/, '//<sanitized>@'

timeout = (time, callback)->
    setTimeout callback, time


class ConnectionPool

    connections = {}
    callbacks = {}
    delays = {}

    constructor: (@delay)->

    connect: (url, callback)->
        unless url of connections
            delays[url] = 0
            @_connect url
        (callbacks[url] ||= []).push callback
        connections[url].then (connection)->
            callback connection

    _connect: (url)->
        q = connections[url] = amqp.connect url
        q.then (connection)=>
            connection.on 'error', (error)=>
                log.error error
                @_reconnect url
            log.info "#{sanitize url} connected"
        q.then null, (error)=>
            log.error error
            @_reconnect url
        q

    _reconnect: (url)->
        log.info "retry #{sanitize url} in #{@delay + delays[url]}"
        timeout @delay + delays[url], =>
            delays[url] += 1000 if delays[url] < @delay * 5
            @_connect(url).then (connection)->
                delays[url] = 0
                for cb in callbacks[url]
                    cb connection

pool = null
getConnectionPool = (delay)->
    pool ||= new ConnectionPool delay

module.exports =
    getConnectionPool: getConnectionPool
