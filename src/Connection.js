// Generated by CoffeeScript 1.8.0
(function() {
  var Connection, EventEmitter, amqp, connections, log, sanitize,
    __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
    __hasProp = {}.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

  EventEmitter = require('events').EventEmitter;

  amqp = require('amqplib');

  log = require('./logger')('connection');

  sanitize = function(url) {
    return url.replace(/\/\/[^@]*@/, '//<sanitized>@');
  };

  Connection = (function(_super) {
    __extends(Connection, _super);

    function Connection(_arg) {
      var urls;
      this.retryDelay = _arg.retryDelay, urls = _arg.urls, this.maxRetry = _arg.maxRetry;
      this.reconnect = __bind(this.reconnect, this);
      if ('string' !== typeof urls) {
        throw new Error("urls must be string");
      }
      if (!urls.length) {
        throw new Error("urls is empty");
      }
      this.urls = urls.split(';');
      this.callbacks = [];
      this.urlIndex = 0;
      this.retried = 0;
      log.debug("urls " + urls);
      log.debug("maxRetry " + this.maxRetry);
    }

    Connection.prototype.connect = function(callback) {
      this.callbacks.push(callback);
      if (this.connectionPromise) {
        log.debug('connection available');
      }
      if (!this.connectionPromise) {
        this.connectionPromise = this._connect(this.getCurrentUrl());
      }
      return this.connectionPromise.then(callback);
    };

    Connection.prototype._connect = function(url) {
      var q, sanitizedUrl;
      log.debug("connecting url " + url);
      sanitizedUrl = sanitize(url);
      this.emit('connecting', {
        url: sanitizedUrl
      });
      q = amqp.connect(url);
      q.then((function(_this) {
        return function(connection) {
          connection.on('error', function(error) {
            log.error(error);
            _this.emit('error', {
              error: error,
              url: sanitizedUrl
            });
            return setTimeout(_this.reconnect, _this.retryDelay);
          });
          log.info("" + sanitizedUrl + " connected");
          return _this.emit('connected', sanitizedUrl);
        };
      })(this));
      q.then(null, (function(_this) {
        return function(error) {
          log.error(error);
          _this.emit('error', {
            error: error,
            url: sanitizedUrl
          });
          return setTimeout(_this.reconnect, _this.retryDelay);
        };
      })(this));
      return q;
    };

    Connection.prototype.reconnect = function() {
      var url;
      this.retried += 1;
      log.debug("retry " + this.retried);
      if (this.retried > this.maxRetry) {
        this.retried = 0;
        url = this.getNextUrl();
      } else {
        url = this.getCurrentUrl();
      }
      this.connectionPromise = this._connect(url);
      return this.connectionPromise.then((function(_this) {
        return function(connection) {
          var callback, _i, _len, _ref, _results;
          _ref = _this.callbacks;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            callback = _ref[_i];
            _results.push(callback(connection));
          }
          return _results;
        };
      })(this));
    };

    Connection.prototype.getNextUrl = function() {
      this.urlIndex += 1;
      if (this.urlIndex >= this.urls.length) {
        this.urlIndex -= this.urls.length;
      }
      log.debug("get next url " + this.urls[this.urlIndex]);
      return this.urls[this.urlIndex];
    };

    Connection.prototype.getCurrentUrl = function() {
      log.debug("current url " + this.urls[this.urlIndex]);
      return this.urls[this.urlIndex];
    };

    return Connection;

  })(EventEmitter);

  connections = {};

  Connection.getConnection = function(_arg) {
    var maxRetry, retryDelay, urls;
    urls = _arg.urls, retryDelay = _arg.retryDelay, maxRetry = _arg.maxRetry;
    if (!connections[urls]) {
      connections[urls] = new Connection({
        urls: urls,
        retryDelay: retryDelay,
        maxRetry: maxRetry
      });
    }
    return connections[urls];
  };

  module.exports = Connection;

}).call(this);
