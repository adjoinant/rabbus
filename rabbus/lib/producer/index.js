var EventEmitter = require("events").EventEmitter;
var util = require("util");
var _ = require("underscore");

var logger = require("../logging")("rabbus.producer");
var MiddlewareBuilder = require("../middlewareBuilder");
var Topology = require("../topology");

// Base Producer
// -------------

function Producer(rabbit, options, defaults){
  EventEmitter.call(this);

  this.rabbit = rabbit;
  this.options = options;
  this.defaults = defaults;

  this.middlewareBuilder = new MiddlewareBuilder(["msg", "hdrs"]);
}

util.inherits(Producer, EventEmitter);

// Public API
// ----------

Producer.prototype.use = function(fn){
  this.middlewareBuilder.use(fn);
};

Producer.prototype.stop = function(){
  this.removeAllListeners();
};

Producer.prototype.publish = producer(function(message, properties){
  return this._publish(message, properties);
});

Producer.prototype.request = producer(function(message, properties){
  return this._request(message, properties);
});

Producer.prototype.scatterGather = producer(function(message, properties){
  return this._scatterGather(message, properties);
});
// Private Members
// ---------------

Producer.prototype._publish = function(msg, properties){
  var rabbit = this.rabbit;
  var exchange = this.topology.exchange;

  properties = _.extend({}, properties, {
    body: msg
  });

  return rabbit.publish(exchange.name, properties);
};

Producer.prototype._request = function(msg, properties){
  var rabbit = this.rabbit;
  var exchange = this.topology.exchange;

  properties = _.extend({}, properties, {
    body: msg
  });

  return rabbit
    .request(exchange.name, properties)
    .then((reply) => {
      reply.ack();
      return reply.body;
    });
};

Producer.prototype._scatterGather = function(msg, properties){
  var rabbit = this.rabbit;
  var exchange = this.topology.exchange;

  properties = _.extend({}, properties, {
    body: msg
  });

  return rabbit
    .scatterGather(exchange.name, properties)
    .then((replies) => {
      replies.forEach(r => r.ack());
      return replies.map(r => r.body);
    });
};

Producer.prototype._verifyTopology = function(cb){
  if (this.topology) { return cb(undefined, this.topology); }
  Topology.verify(this.rabbit, this.options, this.defaults, (err, topology) => {
    if (err) { return cb(err); }
    this.topology = topology;
    return cb(undefined, topology);
  });
};

// private helper methods
// ----------------------

function producer(publishMethod){

  return function(data, properties){
    return new Promise((resolve, reject) => {
      if (!properties) { properties = {}; }
      // start the message producer
      this._verifyTopology((err, topology) => {
        if (err) { return reject(err); }

        this.emit("ready");

        var middleware = this.middlewareBuilder.build((message, middlewareHeaders, next) => {
          var headers = _.extend({}, middlewareHeaders, properties.headers);

          var messageType = topology.messageType || topology.routingKey;
          var props = _.extend({}, properties, {
            routingKey: topology.routingKey,
            type: messageType,
            headers: headers
          });

          logger.info("Publishing Message With Routing Key '" + topology.routingKey + "'");
          logger.debug("With Properties");
          logger.debug(props);

          return resolve(publishMethod.call(this, message, props));
        });

        return middleware(data, {});
      });
    });
  };
}

// Exports
// -------

module.exports = Producer;
