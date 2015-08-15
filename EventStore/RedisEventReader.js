var when = require('when');
var call = require('when/node').call;
var ReadableStream = require('stream').Readable;
var esdf = require('esdf');

function RedisEventReader(readClient, subscribeClient, options) {
	var self = this;
	options = options || {};
	
	ReadableStream.call(self, {
		objectMode: true
	});
	
	// Clients:
	self._readClient = readClient;
	self._subscribeClient = subscribeClient;
	
	// Key names:
	self._dispatchList = options.dispatchList || 'dispatch:global';
	self._sequencePrefix = options.sequencePrefix || 'sequence:';
	self._dispatchNotificationChannel = options.dispatchNotificationChannel || 'dispatch-notify';
	
	// Tunables:
	self._defaultReadSize = options.defaultReadSize || 20;
	self._pollingInterval = options.pollingInterval || 2000;
	
	// State:
	self._listening = false;
	self._startNumber = Number(options.start) || 0;
	self._pendingRead = false;
	self._readRequested = false;
	self._suggestedNextRead = 0;
	self._lastKnownNumber = -1;
	
	// Relay error events to the stream:
	
	self._readClient.on('error', function(error) {
		self.emit('error', error);
	});
	self._subscribeClient.on('error', function(error) {
		self.emit('error', error);
	});
}
RedisEventReader.prototype = Object.create(ReadableStream.prototype);

RedisEventReader.prototype._read = function _read(size) {
	var self = this;
	var client = self._readClient;
	size = Math.min(size || self._defaultReadSize, self._defaultReadSize);
	
	// Start awaiting changes. This will keep updating the dispatch queue length in the background.
	this._listen();
	
	// If we are currently reading, do not start a request. Instead, remember the fact that a read request was made, to start reading immediately.
	if (self._pendingRead) {
		self._readRequested = true;
		self._suggestedNextRead = size;
		return;
	}
	
	self._readRequested = false;
	self._pendingRead = true;
	
	function readEntries() {
		call(client.LRANGE.bind(client), self._dispatchList, self._startNumber, self._startNumber + size - 1).done(function(dispatchItems) {
			//TODO: Filtering of commit entries by aggregate type?
			// Guard clause: if there are no new items to read, wait until some become available.
			if (dispatchItems.length === 0) {
				self._waitForEntries().done(readEntries);
				return;
			}
			
			when.all(dispatchItems.map(JSON.parse.bind(JSON)).map(function(item) {
				return call(client.LRANGE.bind(client), self._sequencePrefix + item.sequenceID, item.sequenceSlot - 1, item.sequenceSlot - 1).then(function(commits) {
					var commitString = commits[0];
					if (!commitString) {
						throw new Error('Consistency error: commit not found');
					}
					
					var commitObject = esdf.core.Commit.reconstruct(JSON.parse(commitString));
					self.push(commitObject);
				});
			})).done(function() {
				self._startNumber += dispatchItems.length;
				self._pendingRead = false;
				if (self._readRequested) {
					self._read(self._suggestedNextRead);
				}
			}, function(readError) {
				self._pendingRead = false;
				self.emit('error', readError);
			});
		}, function(indexReadError) {
			self._pendingRead = false;
			self.emit('error', indexReadError);
		});
	}
	
	readEntries();
};

RedisEventReader.prototype._listen = function _listen() {
	var self = this;
	var client = self._readClient;
	var subscriber = self._subscribeClient;
	var notificationChannel = self._dispatchNotificationChannel;
	
	// Guard clause: if already in the listening state, this is a no-op.
	if (self._listening) {
		return;
	}
	self._listening = true;
	
	if (subscriber) {
 		call(subscriber.SUBSCRIBE.bind(subscriber), notificationChannel).catch(function(error) {
			console.error('[esdf-store-redis] SUBSCRIBE failed:', error);
		});
		subscriber.on('message', function(channel, message) {
			// Guard clause: only accept messages interesting for us. Someone might be re-using the connection for listening on more channels.
			if (channel !== notificationChannel) {
				return;
			}
			
			// Tell ourselves that new entries have arrived.
			// TODO: Get the entry index from the message. Update self._lastKnownNumber, so that _waitForEntries does not have to rely on polling next time it is launched.
			self.emit('newEntries');
		});
	}
	
	//TODO: Interval clean-up.
	setInterval(function() {
		call(client.LLEN.bind(client), self._dispatchList).done(function(length) {
			length = Number(length);
			var newLastKnownNumber = length - 1;
			if (newLastKnownNumber > self._lastKnownNumber) {
				self._lastKnownNumber = newLastKnownNumber;
				self.emit('newEntries');
			}
		}, function(pollingError) {
			console.error('[esdf-store-redis] polling error:', pollingError);
		});
	}, self._pollingInterval);
	
};

RedisEventReader.prototype._waitForEntries = function _waitForEntries() {
	var self = this;
	return when.promise(function(resolve) {
		// There is a time gap between starting to read via LRANGE and entering here. Make sure that nothing has appeared since.
		if (self._lastKnownNumber >= self._startNumber) {
			resolve();
		}
		
		self.once('newEntries', function() {
			resolve();
		});
	});
};

module.exports.RedisEventReader = RedisEventReader;
