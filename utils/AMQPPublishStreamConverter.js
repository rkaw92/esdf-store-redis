var stream = require('stream');

function AMQPPublishStreamConverter(exchangeName, options) {	
	if (typeof(exchangeName) !== 'string') {
		throw new Error('Please pass exchangename');
	}
	
	this._exchangeName = exchangeName;
	//TODO: true might be a saner default!
	this._persistent = Boolean(options.persistent);
	this._stringify = Boolean(options.stringify);
	
	stream.Transform.call(this, { objectMode: true });
	
	
}
AMQPPublishStreamConverter.prototype = Object.create(stream.Transform.prototype);

AMQPPublishStreamConverter.prototype._transform = function _transform(input, encoding, callback) {
	var commit = input;
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
			options: { persistent: this._persistent }
		});
	}, this);
	callback();
};

module.exports.AMQPPublishStreamConverter = AMQPPublishStreamConverter;
