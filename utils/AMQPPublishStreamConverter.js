var stream = require('stream');

function AMQPPublishStreamConverter(exchangeName, options) {	
	if (typeof(exchangeName) !== 'string') {
		throw new Error('Please pass exchangename');
	}
	
	this._exchangeName = exchangeName;
	//TODO: true might be a saner default!
	this._persistent = Boolean(options.persistent);
	this._stringify = Boolean(options.stringify);
	this._publishCallback = options.publishCallback || null;
	
	stream.Transform.call(this, { objectMode: true });
	
	
}
AMQPPublishStreamConverter.prototype = Object.create(stream.Transform.prototype);

AMQPPublishStreamConverter.prototype._transform = function _transform(input, encoding, callback) {
	var commit;
	// Detect the format - container or plain commit?
	if (input.commit && typeof(input.position) === 'number') {
		// This looks like a container (RedisEventReader's options.getContainers tunable).
		commit = input.commit;
	}
	else {
		// This must be the default format - a plain commit.
		commit = input;
	}
	
	var publishCallback = (this._publishCallback) ? this._publishCallback.bind(undefined, input) : null;
	commit.events.forEach(function(event) {
		var content;
		if (this._stringify) {
			content = JSON.stringify({ event: event, commit: commit.getBare() });
		}
		else {
			content = { event: event, commit: commit.getBare() };
		}
		this.push({
			exchange: this._exchangeName,
			routingKey: commit.aggregateType + '.' + event.eventType,
			content: content,
			options: { persistent: this._persistent },
			callback: publishCallback
		});
	}, this);
	callback();
};

module.exports.AMQPPublishStreamConverter = AMQPPublishStreamConverter;
