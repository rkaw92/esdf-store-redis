module.exports.RedisEventSink = require('./EventStore/RedisEventSink').RedisEventSink;
module.exports.RedisEventStreamer = require('./EventStore/RedisEventStreamer').RedisEventStreamer;
module.exports.RedisEventReader = require('./EventStore/RedisEventReader').RedisEventReader;
module.exports.utils = require('./utils');

// Export the redis module used:
module.exports.redis = require('redis');
