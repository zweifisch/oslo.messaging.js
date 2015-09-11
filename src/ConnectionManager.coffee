{EventEmitter} = require 'events'
amqp = require 'amqplib'
log = require('./logger') 'connection'


sanitize = (url)->
    url.replace /\/\/[^@]*@/, '//<sanitized>@'


class ConnectionManager extends EventEmitter

    constructor: ({@retryDelay, urls, @maxRetry, @timeout})->
        throw new Error "urls must be string" unless 'string' is typeof urls
        throw new Error "urls is empty" unless urls.length
        @urls = urls.split /[;,]/
        @urlIndex = 0
        @retried = 0
        log.debug "urls #{urls}"
        log.debug "maxRetry #{@maxRetry}"

    connect: ->
        if @connectionPromise
            log.debug 'connection available'
        else
            @connectionPromise = @_connect @getCurrentUrl() 
            @connectionPromise.then (conn)=>
                @emit "connected", conn
        @connectionPromise

    _connect: (url)->
        log.debug "connecting url #{url} timeout #{@timeout}"
        sanitizedUrl = sanitize url
        @emit 'connecting', url: sanitizedUrl
        q = amqp.connect url, timeout: @timeout
        q.then (connection)=>
            connection.on 'error', (error)=>
                log.error error
                @emit 'error', error
                setTimeout @reconnect, @retryDelay
            log.info "#{sanitizedUrl} connected"
        .catch (error)=>
            log.error error
            @emit 'error', error
            setTimeout @reconnect, @retryDelay
        q

    reconnect: =>
        @retried += 1
        log.debug "retry #{@retried}"
        if @retried > @maxRetry
            @retried = 0
            url = @getNextUrl()
        else
            url = @getCurrentUrl()
        @connectionPromise = @_connect url
        @connectionPromise.then (conn)=>
            @emit "reconnected", conn

    getNextUrl: ->
        @urlIndex += 1
        if @urlIndex >= @urls.length
            @urlIndex -= @urls.length
        log.debug "get next url #{@urls[@urlIndex]}"
        @urls[@urlIndex]

    getCurrentUrl: ->
        log.debug "current url #{@urls[@urlIndex]}"
        @urls[@urlIndex]

connections = {}

ConnectionManager.getConnection = ({urls, retryDelay, maxRetry, timeout})->
    if not connections[urls]
        connections[urls] = new ConnectionManager
            urls: urls
            retryDelay: retryDelay
            maxRetry: maxRetry
            timeout: timeout * 1000
    connections[urls]

module.exports = ConnectionManager
