{EventEmitter} = require 'events'
amqp = require 'amqplib'
log = require('./logger') 'connection'


sanitize = (url)->
    url.replace /\/\/[^@]*@/, '//<sanitized>@'


class Connection extends EventEmitter

    constructor: ({@retryDelay, urls, @maxRetry, @timeout})->
        throw new Error "urls must be string" unless 'string' is typeof urls
        throw new Error "urls is empty" unless urls.length
        @urls = urls.split /[;,]/
        @callbacks = []
        @urlIndex = 0
        @retried = 0
        log.debug "urls #{urls}"
        log.debug "maxRetry #{@maxRetry}"

    connect: (callback)->
        @callbacks.push callback
        if @connectionPromise
            log.debug 'connection available'
        @connectionPromise = @_connect @getCurrentUrl() unless @connectionPromise
        @connectionPromise.then callback

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
            @emit 'connected', sanitizedUrl
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
        @connectionPromise.then (connection)=>
            callback connection for callback in @callbacks

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

Connection.getConnection = ({urls, retryDelay, maxRetry, timeout})->
    if not connections[urls]
        connections[urls] = new Connection
            urls: urls
            retryDelay: retryDelay
            maxRetry: maxRetry
            timeout: timeout * 1000
    connections[urls]

module.exports = Connection
