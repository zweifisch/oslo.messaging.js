// Generated by CoffeeScript 1.9.2
(function() {
  var Connection, EventEmitter, Notifier, crypto, log, promise,
    extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
    hasProp = {}.hasOwnProperty;

  crypto = require('crypto');

  EventEmitter = require('events').EventEmitter;

  promise = require('when').promise;

  log = require('./logger')('notifier');

  Connection = require('./Connection');

  Notifier = (function(superClass) {
    extend(Notifier, superClass);

    function Notifier(arg) {
      this.url = arg.url, this.prefix = arg.prefix, this.topic = arg.topic, this.exchange = arg.exchange, this.retryDelay = arg.retryDelay, this.noAck = arg.noAck, this.queue = arg.queue, this.maxRetry = arg.maxRetry, this.connectionTimeout = arg.connectionTimeout;
      if (this.retryDelay == null) {
        this.retryDelay = 3000;
      }
      if (this.maxRetry == null) {
        this.maxRetry = 3;
      }
      if (this.connectionTimeout == null) {
        this.connectionTimeout = 10;
      }
      if (this.queue == null) {
        this.queue = (this.prefix || 'notifier') + "_" + (crypto.randomBytes(16).toString('hex'));
      }
      log.debug("queue: " + this.queue);
      this.connection = Connection.getConnection({
        retryDelay: this.retryDelay,
        urls: this.url,
        maxRetry: this.maxRetry,
        timeout: this.connectionTimeout
      });
    }

    Notifier.prototype.connect = function() {
      if (!this.q) {
        this.q = promise((function(_this) {
          return function(resolve, reject) {
            return _this.connection.connect(function(connection) {
              return connection.createChannel().then(function(channel) {
                var consumeQ, consumer;
                _this.channel = channel;
                _this.channel.assertExchange(_this.exchange, 'topic', {
                  autoDelete: false,
                  durable: false
                }).then(function() {
                  return log.info("exchange " + _this.exchange + " asserted");
                }).then(null, function(error) {
                  return _this.emit('error', error);
                });
                _this.channel.assertQueue(_this.queue, {
                  autoDelete: true,
                  durable: false
                }).then(function() {
                  return log.info("queue " + _this.queue + " asserted");
                }).then(null, function(error) {
                  return _this.emit('error', error);
                });
                _this.channel.bindQueue(_this.queue, _this.exchange, _this.topic).then(function() {
                  return log.info("topic " + _this.topic + " binded");
                }).then(null, function(error) {
                  return _this.emit('error', error);
                });
                consumer = function(msg) {
                  var decoded;
                  decoded = JSON.parse(msg.content.toString());
                  log.debug(decoded);
                  if (_this.noAck) {
                    return _this.callback(decoded);
                  } else {
                    return _this.callback(decoded, function() {
                      return _this.channel.ack(msg);
                    });
                  }
                };
                consumeQ = _this.channel.consume(_this.queue, consumer, {
                  noAck: _this.noAck
                });
                consumeQ.then(function() {
                  log.info("wait for notification on queue " + _this.queue);
                  _this.emit('ready', _this.queue);
                  return resolve(_this);
                });
                return consumeQ.then(null, function(error) {
                  _this.emit('error', error);
                  return reject(error);
                });
              });
            });
          };
        })(this));
      }
      return this.q;
    };

    Notifier.prototype.onMessage = function(callback) {
      return this.callback = callback;
    };

    return Notifier;

  })(EventEmitter);

  module.exports = Notifier;

}).call(this);
