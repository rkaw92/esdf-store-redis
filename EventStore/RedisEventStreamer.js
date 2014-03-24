//TODO: doc
//TODO: high-level architectural & reliability overview.

var when = require('when');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var esdf = require('esdf');

function ErrorWrapper(message, originalError){
	this.message = message;
	this.originalError = originalError;
}
ErrorWrapper.prototype.toString = function(){
	return String(this.originalError);
};
util.inherits(ErrorWrapper, Error);

var dispatchPrefix = 'dispatch:';
var dispatchGlobalKey = 'global';
var sequencePrefix = 'sequence:';
var readAtOnce = 100;
var markerInterval = 1000;
var dispatchNotificationChannel = 'dispatch-notify';
var clientIndexKeyPrefix = 'client-index:';

function publishCommitFromPointer(readerConnection, sequenceID, sequenceSlot, publisherFunction){
	return when.promise(function(resolve, reject){
		readerConnection.lrange(sequencePrefix + sequenceID, sequenceSlot - 1, sequenceSlot - 1, function(err, results){
			if(err){
				reject(new ErrorWrapper('Single commit retrieval via LRANGE failed (used ID/slot: ' + sequenceID + '/' + sequenceSlot + ')', err));
				return;
			}
			if(results.length === 1){
				var currentCommit = esdf.core.Commit.reconstruct(JSON.parse(results[0]));
				when(publisherFunction(currentCommit),
				function _commitPublished(ok){
					resolve();
					return;
				},
				function _commitPublishFailed(reason){
					reject(reason);
					return;
				});
			}
			else{
				// Corrupt commit pointer - this should never happen!
				reject(new Error('Corrupt commit pointer - no commit found under the indicated sequenceID/sequenceSlot (or duplicate; count should be 1, is: ' + results.length + ').'));
				return;
			}
		});
	});
}


function RedisEventStreamer(readerConnection, subscriberConnection, streamerID, options){
	this._readerConnection = readerConnection;
	this._subscriberConnection = subscriberConnection;
	this._streamerID = streamerID;
	
	if(!options){
		options = {};
	}
	this._persistent = (typeof(options.persistent) !== 'undefined') ? Boolean(options.persistent) : true;
	this._useMessaging = (typeof(subscriberConnection) === 'object' && subscriberConnection !== null && typeof(subscriberConnection.subscribe) === 'function');
	this._pollingDelay = (typeof(options.pollingDelay) === 'number') ? options.pollingDelay : 1000;
	
	this._paused = true;
	this._pausing = false;
	this._publisherFunction = undefined;
	this._subscriberFunction = undefined;
	this._markerIndex = 0;
	this._savedMarkerIndex = 0;
	this._markerTimer = null;
}
util.inherits(RedisEventStreamer, EventEmitter);

RedisEventStreamer.prototype.setPublisher = function setPublisher(publisher){
	if(typeof(publisher.publishCommit) !== 'function'){
		throw new Error('The publisher needs to have a publishCommit method!');
	}
	this._publisherFunction = publisher.publishCommit.bind(publisher);
};

RedisEventStreamer.prototype._markAsPublishedEventually = function _markAsPublishedEventually(dispatchIndex){
	if(dispatchIndex + 1 > this._markerIndex){
		this._markerIndex = dispatchIndex + 1;
	}
};

RedisEventStreamer.prototype._markNextIndex = function _markNextIndex(){
	var self = this;
	// If there is nothing new to save, do not issue a database write. Save DB ops, save the Earth.
	if(self._markerIndex <= self._savedMarkerIndex){
		return;
	}
	return when.promise(function(resolve, reject){
		var currentMarkerIndex = Number(self._markerIndex);
		self._readerConnection.set(clientIndexKeyPrefix + self._streamerID, currentMarkerIndex, function(err, result){
			if(err){
				reject(new ErrorWrapper('Error marking index as next', err));
				return;
			}
			// Set the saved marker index to the value we've just saved (pretty idiomatic, isn't it?).
			self._savedMarkerIndex = currentMarkerIndex;
			resolve(currentMarkerIndex);
		});
	});
	
};

RedisEventStreamer.prototype._startMarkerTimer = function _startMarkerTimer(){
	if(this._markerTimer === null){
		// Note: self._markNextIndex() returns a promise which we completely ignore. It's a sane strategy, since re-publishing of commits must be allowed, anyway.
		this._markerTimer = setInterval(this._markNextIndex.bind(this), markerInterval);
	}
};

RedisEventStreamer.prototype.start = function start(){
	var self = this;
	if(self._running){
		return;
	}
	self._running = true;
	self._disengaged = false;
	self._halted = false;
	self.emit('engaged');
	
	self.once('halted', function(){
		self._running = false;
		self._markNextIndex();
		self.emit('ready');
	});
	
	var determineFirstIndex;
	var self = this;
	// First, we need to determine at which index we want to start reading. We choose an appropriate function to give us that number.
	if(this._persistent){
		determineFirstIndex = function(){
			return when.promise(function(resolve, reject){
				self._readerConnection.get(clientIndexKeyPrefix + self._streamerID, function(err, counterValue){
					if(err){
						reject(err);
						return;
					}
					resolve(Number(counterValue));
					return;
				});
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
	function _indexDetermined(startIndex){
		// Since we are starting from startIndex, the last published index must have been startIndex - 1.
		self._markAsPublishedEventually(startIndex - 1);
		self._startMarkerTimer();
		// Now, we can choose the streaming mode depending on the resources given to us.
		//  In the optimal case, we have both the Event Store and the subscriber connection, which lets us pick up events as they come.
		if(self._subscriberConnection && self._useMessaging){
			self._handleMessagingBasedStreaming(startIndex);
		}
		else{
			//TODO: handle the non-messaging mode, i.e. constant polling.
		}
	},
	function _indexDeterminationFailure(err){
		//TODO: replace with a better error-wrapping technique.
		self._restart(new ErrorWrapper('Failed to determine starting index due to a DB error - restarting', err));
	});
	
};

RedisEventStreamer.prototype._handleMessagingBasedStreaming = function _handleMessagingBasedStreaming(startIndex){
	var self = this;
	// Local closure variable "failed" is set to true when this particular streaming session encounters an error. It acts as a latch to prevent multiple _restart() calls.
	var failed = false;
	// Whether anyone has signalled us that we should stop whatever we are doing.
	var shouldStop = false;
	// The conditionalRestart local helper function calls self._restart once, presumably after streaming breaks.
	var conditionalRestart = function conditionalRestart(reason){
		// Check the latch variable - prevent calling _restart() multiple times in reaction to errors from multiple publishes that were in-flight concurrently.
		if(!failed){
			failed = true;
			self._restart(reason);
		}
	};
	
	// Until we've got the starting length of the dispatch list, our listener function shall do nothing but queue the messages. This is reflected by the catchingUp mode-flag.
	var catchingUp = true;
	// Store the notifications received while in catch-up mode.
	var pendingNotifications = [];
	
	// Define a helper function, used by _subscriberFunction and when transitioning from first-read mode to messaging.
	function publishCommitFromParsedPointerAsync(commitPointer){
		publishCommitFromPointer(self._readerConnection, commitPointer.sequenceID, commitPointer.sequenceSlot, self._publisherFunction).then(
			function _publishSucceeded(){
				// The message has been saved. Since this is a reactive pattern (listening to notifications), there is no explicit
				//  queueing / "next" step to do. The subscriber function will get called, anyway.
				//console.log('* async: Calling _markAsPublishedEventually for ' + commitPointer.dispatchIndex);
				self._markAsPublishedEventually(commitPointer.dispatchIndex);
			},
			function _publishFailed(reason){
				conditionalRestart(new ErrorWrapper('Publish failed - retrying from the last known published commit', reason));
			});
	}
	
	// Define the subscriber function that will be used when new messages arrive.
	self._subscriberFunction = function _commitStreamerSubscriberFunction(channel, message){
		// Guard clause: Ignore messages not directed to us (perhaps coming from side effects applied to the dependency-injected subscriber connection).
		if(channel !== dispatchNotificationChannel){
			return;
		}
		try{
			// The message in the DB should be a stringified commit header (= the commit without its events).
			var commitPointer = JSON.parse(message);
			// If still in catch-up mode, all we can do is queue the message.
			if(catchingUp){
				pendingNotifications.push(commitPointer);
			}
			else{
				//console.log('* Got a live message:', commitPointer);
				publishCommitFromParsedPointerAsync(commitPointer);
			}
		}
		catch(jsonParseError){ // Note that this is a variable, not an exception class specifier.
			// Who in their right mind would give us malformed JSON?
			conditionalRestart(jsonParseError);
		}
	};
	// Use the subscriber function, whose definition ends on the line above.
	self._subscriberConnection.on('message', self._subscriberFunction);
	self._subscriberConnection.subscribe(dispatchNotificationChannel);
	// Set up cleanup code for when a halt needs to occur.
	self.once('disengaged', function _stopProcessingAndPrepareToHalt(){
		// Tell our reader code that it should halt itself.
		shouldStop = true;
		self._subscriberConnection.unsubscribe();
		self._subscriberConnection.removeListener('message', this._subscriberFunction);
		// Neutralize the subscriber function, too.
		self._subscriberFunction = function(){};
		// If we are no longer catching up, i.e. we're in pure messaging mode, it is always safe to halt.
		if(!catchingUp){
			self._halt();
		}
	});
	
	// Get the length of the dispatch list right after subscribing. Now we know that if something were to write into the list,
	//  it would have to publish a message, too (assuming correct behaviour of the Event Sink and no crashes between write and publish).
	//TODO: handle situations where the notification is never sent because of a writer crash, etc.
	// Lock halting - _halt() will not transition to "paused" by itself if it sees that someone is reading. That gives us time to complete the operation before restart.
	self._readerConnection.llen(dispatchPrefix + dispatchGlobalKey, function(err, currentListLength){
		if(err){
			conditionalRestart(new ErrorWrapper('Initial LLEN failed when preparing to catch up before entering messaging mode', err));
			return;
		}
		// Check if anybody preempted us with a _disengage. If they did, we are obliged to stop in our tracks and finish the halting procedure.
		if(shouldStop){
			self._halt();
			return;
		}
		// We've got the dispatch list length (of some recent point in the past) and we are not supposed to stop. Activate the subscriber.
		// Now, we need to catch up with the global dispatch list.
		var initialPublishPromise;
		if(currentListLength > 0){
			initialPublishPromise = self._publishCommitsFromDispatchList(startIndex, currentListLength - 1);
		}
		else{
			initialPublishPromise = when.resolve('Nothing to publish in the initial phase - proceeding to purge the pending async list');
		}
		initialPublishPromise.then(
		function _commitsPublishedBeforeEnablingMessaging(){
			// The last shouldStop check - beyond this point, outstanding commits will be pushed out and catch-up mode will be disabled immediately.
			if(shouldStop){
				self._halt();
				return;
			}
			// We have published all the initial elements (i.e. the ones that we do not expect to see incoming as messages).
			//  All that's left is the commits which arrived when we were catching up. Throw them at Redis and call it a day.
			pendingNotifications.forEach(function(notification){
				publishCommitFromParsedPointerAsync(notification);
			});
			// Clear the temporary store and switch to messaging mode, finally.
			pendingNotifications = [];
			catchingUp = false;
			// We're in messaging mode now. Nothing more to do - the subscriber function has taken over.
		},
		function _initialPreMessagingPublishFailed(reason){
			if(shouldStop){
				self._halt();
				return;
			}
			conditionalRestart(new ErrorWrapper('Initial catch-up publish failed before entering messaging mode', reason));
		});
	});
};

RedisEventStreamer.prototype._publishCommitsFromDispatchList = function _publishCommitsFromDispatchList(start, end){
	//TODO: Chunking the list into short fragments (1000 elements?), to save memory. This can be done with recursion on this function.
	var self = this;
	var publishDeferred = when.defer();
	self._readerConnection.lrange(dispatchPrefix + dispatchGlobalKey, start, end, function(err, commitPointers){
		if(err){
			publishDeferred.resolver.reject(new ErrorWrapper('LRANGE in _publishCommitsFromDispatchList failed', err));
			return;
		}
		var pendingPublishPromises = [];
		// Issue a publish command for all the commit pointers we have at once. This is pipelining proper.
		commitPointers.forEach(function(commitJSON, elementIndex){
			try{
				var commitPointer = JSON.parse(commitJSON);
			}
			catch(jsonParseError){
				publishDeferred.resolver.reject(new ErrorWrapper('JSON parsing failed while publishing commits from dispatch list', jsonParseError));
				return;
			}
			// What we are doing below is pushing a (derived) promise, which resolves if the publish resolves, onto the array. If the publish rejects, publishDeferred is rejected.
			//  However, a successful publish does not resolve publishDeferred. Instead, the "when.all()" further down waits until all pending publishes have resolved.
			var thisPublishPromise = publishCommitFromPointer(self._readerConnection, commitPointer.sequenceID, commitPointer.sequenceSlot, self._publisherFunction).then(
				function _commitPublishedFromDispatchList(result){
					//console.log('* Marking as published (dispatch list):', start + elementIndex);
					// Update the dispatch list counter. This will be saved to the database in the near future (hopefully), so that if the streamer is aborted, we can resume from here.
					self._markAsPublishedEventually(start + elementIndex);
					// Propagate the successful resolution.
					return when.resolve(result);
				},
				undefined); // Rejection callback explicitly not provided - this will cause the derived promise to reject when the original promise is rejected.
			pendingPublishPromises.push(thisPublishPromise);
		});
		// Tie the resolution of the main promise to the successful completion of all publishes. A single failure will result in rejection (see the block above).
		when.all(pendingPublishPromises,
		function _publishingSucceeded(result){
			publishDeferred.resolver.resolve(result);
		},
		function _publishingFailed(reason){
			publishDeferred.resolver.reject(reason);
		});
	});
	return publishDeferred.promise;
};

RedisEventStreamer.prototype._disengage = function _disengage(eventType, eventPayload){
	if(!this._disengaged){
		this._disengaged = true;
		this._haltingEvent = eventType;
		this._haltingPayload = eventPayload;
		this.emit('disengaged', eventType, eventPayload);
		// Now, we have to wait for the processing to finish.
	}
};

RedisEventStreamer.prototype._halt = function _halt(){
	if(!this._halted){
		this._halted = true;
		this.emit('halted', this._haltingEvent, this._haltingPayload);
	}
};

RedisEventStreamer.prototype._restart = function _restart(reason){
	// Wait for the "ready" event - it means the "start" procedure can be called again.
	this.once('ready', (function(){
		this.start();
	}).bind(this));
	//console.log('[RESTART]', reason);
	this._disengage('restart', {reason: reason});
};

RedisEventStreamer.prototype.pause = function pause(){
	return this._disengage('pause', undefined);
};

module.exports.RedisEventStreamer = RedisEventStreamer;