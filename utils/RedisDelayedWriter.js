var call = require('when/node').call;

function RedisDelayedWriter(client, key, options) {
	options = options || {};
	var self = this;
	
	self._client = client;
	self._key = key;
	self._value = options.initialValue;
	self._lastWrittenValue = self._value;

	self._periodicWrite = setInterval(function() {
		if (self._value !== self._lastWrittenValue) {
			var valueToWrite = self._value;
			call(client.SET.bind(client), key, valueToWrite).then(function() {
				self._lastWrittenValue = valueToWrite;
			}).catch(function(error) {
				console.error('[esdf-store-redis] RedisDelayedWriter write error:', error);
			});
		}
	}, options.writeInterval || 1000);
}

RedisDelayedWriter.prototype.write = function write(value) {
	if (value !== this._value) {
		this._value = value;
	}
};

module.exports.RedisDelayedWriter = RedisDelayedWriter;
