//TODO: use two connections - one for subscribing to redis messaging and another one to read new elements off the dispatch list
// (in case the publisher is a redis server, too, the dispatcher process will keep 3 connections open in total)

//TODO: doc

var when = require('when');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var dispatchPrefix = 'dispatch:';
var dispatchGlobalKey = 'global';
var readAtOnce = 100;
var dispatchNotificationChannel = 'dispatch-notify';
var clientIndexKeyPrefix = 'client-index:';

function RedisEventStreamer(readerConnection, subscriberConnection, streamerID, options){
	this._readerConnection = readerConnection;
	this._subscriberConnection = subscriberConnection;
	this._streamerID = streamerID;
	
	if(!options){
		options = {};
	}
	this._persistent = (typeof(options.persistent) !== 'undefined') ? Boolean(options.persistent) : true;
	this._useMessaging = (typeof(subscriberConnection) === 'object' && subscriberConnection !== null && typeof(subscriberConnection.subscribe) === 'function');
	console.log('_useMessaging set in constructor:', this._useMessaging);
	this._pollingDelay = (typeof(options.pollingDelay) === 'number') ? options.pollingDelay : 1000;
	
	this._paused = true;
	this._pausing = false;
	this._publisherFunction = undefined;
	this._subscriberFunction = undefined;
}
util.inherits(RedisEventStreamer, EventEmitter);

RedisEventStreamer.prototype.setPublisher = function setPublisher(publisher){
	if(typeof(publisher.publishCommit) !== 'function'){
		throw new Error('The publisher needs to have a publishCommit method!');
	}
	this._publisherFunction = publisher.publishCommit.bind(publisher);
};

RedisEventStreamer.prototype.start = function start(){
	if(!this._paused){
		return;
	}
	var determineFirstIndex;
	var self = this;
	// First, we need to determine at which index we want to start reading. We choose an appropriate function to give us that number.
	if(this._persistent){
		determineFirstIndex = function(){
			return when.promise(function(resolve, reject){
				console.log('Getting the streamer\'s last index');
				self._readerConnection.get(clientIndexKeyPrefix + self._streamerID, function(err, counterValue){
					console.log('Got the last index!');
					if(err){
						reject(err);
						return;
					}
					resolve(Number(counterValue));
					return;
				});
				console.log('GET called');
			});
		};
	}
	else{
		determineFirstIndex = function(){
			return when.resolve(0);
		};
	}
	
	// Actually call the index getter function.
	determineFirstIndex().then(
	function _startIndexDetermined(startIndex){
		console.log('Start index determined:', startIndex);
		var shouldReadNow = true;
		var reading = false;
		// We have the index now. Proceed with the streaming.
		function _readCommitStream(){
			if(reading){
				return;
			}
			if(self._pausing){
				self._paused = true;
				self._pausing = false;
				return;
			}
			if(shouldReadNow){
				console.log('Will issue readNow()');
				reading = true;
				// Reset the "read next" flag to false if we are using messaging - a dispatch notification message can come in and set it back to true later.
				shouldReadNow = self._useMessaging ? false : true;
				console.log('shouldReadNow: set to', shouldReadNow, ' while _useMessaging =', self._useMessaging);
				var readNow = function(){
					console.log('Issuing an LRANGE');
					self._readerConnection.lrange(dispatchPrefix + dispatchGlobalKey, startIndex, startIndex + readAtOnce - 1, function(err, elements){
						console.log('LRANGE returned:', elements);
						if(err){
							//TODO: abort/retry/fail?
							self.emit('error', err);
							reading = false;
							return;
						}
						// If we've read the maximum number of elements, chances are there are more available, so schedule an immediate read.
						if(elements.length === readAtOnce){
							shouldReadNow = true;
						}
						//TODO: foreach? promise loop?
						// Iterate through all the elements that we managed to read from the dispatch queue.
						var fetchedMessagesIndex = 0;
						//TODO: parse each commit, extract events, publish one after another.
						var processNextElement = function(){
							// Check the boundary condition - if true, we have exhausted the list we've read.
							if(fetchedMessagesIndex >= elements.length){
								reading = false;
								startIndex = startIndex + elements.length;
								// If running in persistent mode, update the current reading offset in the DB.
								if(self._persistent){
									self._readerConnection.set(clientIndexKeyPrefix + self._streamerID, startIndex, function(err, result){
										// Nothing to do here - in the worst case, if an error has occured, the client will simply resume from an old index.
									});
								}
								return;
							}
							// If we're here, this means an element is to be processed. Each element is a commit containing events, which all need to be processed separately.
							
							
							when(self._publisherFunction());
						};
						processNextElement();
					}); //end of lrange call
				};
				if(self._pollingDelay === 0){
					setImmediate(readNow);
				}
				else{
					setTimeout(readNow, self._pollingDelay);
				}
			}
			else{
				console.log('Should NOT read now...');
				// Reached only in case messaging is enabled - otherwise, "shouldReadNow" is always true (and polling is done using delays instead).
				//  This branch intentionally does nothing.
			}
		}
		
		if(self._useMessaging){
			self._subscriberConnection.subscribe(dispatchNotificationChannel);
			self._subscriberFunction = function(channel, message){
				if(channel === dispatchNotificationChannel){
					shouldReadNow = true;
					_readCommitStream();
				}
			};
			self._subscriberConnection.on('message', self._subscriberFunction);
		}
		// Launch the first read.
		console.log('Launching _readCommitStream!');
		_readCommitStream();
	},
	function _startIndexDeterminationFailure(err){
		self.emit('error', err);
	});
	
};

RedisEventStreamer.prototype.pause = function pause(){
	if(!this._pausing){
		this._pausing = true;
		if(this._useMessaging){
			this._subscriberConnection.unsubscribe();
			this._subscriberConnection.removeListener('message', this._subscriberFunction);
		}
	}
};

module.exports.RedisEventStreamer = RedisEventStreamer;