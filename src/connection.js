var _ = require( 'lodash' );
var when = require( 'when' );
var nodeWhen = require( 'when/node' );
var machina = require( 'machina' );
var riakpbc = require( 'riakpbc' );
var createBucket = require( './bucket.js' );
var solr = require( './search.js' );
var uuid = require( 'node-uuid' );
var debug = require( 'debug' )( 'riaktive:connection' );

function connect( options ) {
	var nodes = [];
	var retries = 1000;
	var nodeId;
	var failed = function () {};
	if ( _.isArray( options ) ) {
		nodes = options;
	} else if ( _.isObject( options ) ) {
		if ( options.nodes ) {
			nodes = options.nodes || nodes;
		} else {
			nodes = [ {
				host: options.host,
				port: options.port,
				http: options.http,
				timeout: options.timeout
			} ];
		}
		retries = options.retries || retries;
		failed = options.failed || failed;
		nodeId = options.nodeId;
	}
	if ( !nodeId ) {
		nodeId = uuid.v4();
	}
	var client;
	var defaultNode = {
		host: 'localhost',
		port: 8087,
		http: 8098,
		timeout: '5000'
	};
	var lifted;
	var nodeIndex = 0;
	var attempts = 0;
	var limit = nodes ? nodes.length : 0;
	var Monad = machina.Fsm.extend( {
		ids: require( 'sliver' )( nodeId ),
		bucket: function ( bucketName, options ) {
			var bucket = createBucket( bucketName, options || {}, this, nodeId );
			this[ bucket.name ] = bucket;
			if ( bucket.alias ) {
				this[ bucket.alias ] = bucket;
			}
			return bucket;
		},

		index: function ( indexName, alias ) {
			var index = solr( this, indexName );
			this[ index ] = index;
			if ( alias ) {
				this[ alias ] = index;
			}
			return index;
		},

		connect: function () {
			var opts = this.getNode();
			attempts++;
			debug( 'attempting connection to node %d: %s', nodeIndex, JSON.stringify( opts ) );
			client = riakpbc.createClient( opts );
			lifted = lift( client );
			lifted.connect()
				.then( function () {
					this.handle( 'connection' );
				}.bind( this ) )
				.then( null, function ( err ) {
					this.handle( 'connection.failed', err );
				}.bind( this ) );
			client.connection.client.on( 'end', this._closed.bind( this ) );
			client.connection.client.on( 'error', this._disconnected.bind( this ) );
		},

		close: function () {
			this.transition( 'closed' );
			if ( lifted ) {
				client.disconnect();
			}
		},

		getNode: function () {
			var next = _.isEmpty( nodes ) ? {} : nodes[ nodeIndex ];
			return _.defaults( next, defaultNode );
		},

		operate: function ( call, args ) {
			var op = {
				operation: call,
				argList: args
			};
			var promise = when.promise( function ( resolve, reject, notify ) {
				op.resolve = resolve;
				op.reject = reject;
				op.notify = notify;
			} );
			this.handle( 'operate', op );
			return promise;
		},

		reset: function () {
			attempts = 0;
			this._bumpIndex();
			this.transition( 'connecting' );
		},

		_bumpIndex: function () {
			if ( nodeIndex === limit - 1 ) {
				nodeIndex = 0;
			} else {
				nodeIndex++;
			}
		},

		_closed: function () {
			this.handle( 'close' );
		},

		_disconnected: function ( err ) {
			this.handle( 'disconnect', err );
		},

		_removeSocketEvents: function () {
			client.connection.client.removeListener( 'end', this._closed );
			client.connection.client.removeListener( 'error', this._disconnected );
		},
		initialState: 'connecting',
		states: {
			'closed': {
				_onEnter: function () {
					debug( 'Connection to %s closed', JSON.stringify( this.getNode() ) );
				},
				operate: function ( /* call */) {
					this.deferUntilTransition( 'connected' );
					this.transition( 'connecting' );
				}
			},
			'connecting': {
				_onEnter: function () {
					this.connect();
				},
				connection: function () {
					debug( 'connection to %s established', JSON.stringify( this.getNode() ) );
					attempts = 0;
					this.transition( 'connected' );
				},
				'connection.failed': function ( err ) {
					debug( 'connection to %s failed with %s', JSON.stringify( this.getNode() ), err );
					if ( attempts > retries ) {
						this.transition( 'failed' );
					} else {
						this._bumpIndex();
						this._removeSocketEvents();
						this.connect();
					}
				},
				operate: function ( /* call */) {
					this.deferUntilTransition( 'connected' );
				}
			},
			'connected': {
				'disconnect': function ( err ) {
					debug( 'Lost connection to %s with %s', JSON.stringify( this.getNode() ), err );
					this.transition( 'disconnected' );
				},
				operate: function ( call ) {
					try {
						lifted[ call.operation ].apply( lifted, call.argList )
							.then( call.resolve, call.reject, call.notify );
					} catch ( err ) {
						call.reject( err );
					}
				}
			},
			'disconnected': {
				_onEnter: function () {
					this.transition( 'connecting' );
				},
				operate: function ( /* call */) {
					this.deferUntilTransition( 'connected' );
				}
			},
			'failed': {
				_onEnter: function () {
					failed();
					debug( 'Connection attempts have exceeded retry limit.' );
				},
				'reset': function () {
					this.transition( 'connecting' );
				}
			}
		}
	} );

	var operations = [ 'getBuckets', 'getBucket', 'setBucket', 'resetBucket', 'put', 'get', 'del',
		'mapred', 'getCounter', 'updateCounter', 'search', 'getClientId', 'setClientId',
		'getServerInfo', 'ping', 'startTls', 'auth', 'setBucketType', 'getBucketType',
		'updateDtype', 'fetchDtype', 'yzGetIndex', 'yzPutIndex', 'yzDeleteIndex', 'yzPutSchema',
		'yzGetSchema', 'getKeys', 'getIndex' ];
	var machine = new Monad();
	_.each( operations, function ( name ) {
		machine[ name ] = function () {
			var list = Array.prototype.slice.call( arguments, 0 );
			return machine.operate( name, list );
		}.bind( machine );
	} );
	return machine;
}

// this is here to convert Node style callbacks to promises
function lift( client ) { // jshint ignore:line
	var lifted = {
		getBuckets: nodeWhen.lift( client.getBuckets ).bind( client ),
		setBucket: nodeWhen.lift( client.setBucket ).bind( client ),
		resetBucket: nodeWhen.lift( client.resetBucket ).bind( client ),
		put: nodeWhen.lift( client.put ).bind( client ),
		get: nodeWhen.lift( client.get ).bind( client ),
		del: nodeWhen.lift( client.del ).bind( client ),
		mapred: nodeWhen.lift( client.mapred ).bind( client ),
		getCounter: nodeWhen.lift( client.getCounter ).bind( client ),
		updateCounter: nodeWhen.lift( client.updateCounter ).bind( client ),
		search: nodeWhen.lift( client.search ).bind( client ),
		getClientId: nodeWhen.lift( client.getClientId ).bind( client ),
		setClientId: nodeWhen.lift( client.setClientId ).bind( client ),
		getServerInfo: nodeWhen.lift( client.getServerInfo ).bind( client ),
		ping: nodeWhen.lift( client.ping ).bind( client ),
		startTls: nodeWhen.lift( client.startTls ).bind( client ),
		auth: nodeWhen.lift( client.auth ).bind( client ),
		setBucketType: nodeWhen.lift( client.setBucketType ).bind( client ),
		getBucketType: nodeWhen.lift( client.getBucketType ).bind( client ),
		updateDtype: nodeWhen.lift( client.updateDtype ).bind( client ),
		fetchDtype: nodeWhen.lift( client.fetchDtype ).bind( client ),
		yzGetIndex: nodeWhen.lift( client.yzGetIndex ).bind( client ),
		yzPutIndex: nodeWhen.lift( client.yzPutIndex ).bind( client ),
		yzDeleteIndex: nodeWhen.lift( client.yzDeleteIndex ).bind( client ),
		yzPutSchema: nodeWhen.lift( client.yzPutSchema ).bind( client ),
		yzGetSchema: safeLift( client.yzGetSchema.bind( client ) ),
		connect: nodeWhen.lift( client.connect ).bind( client ),
		disconnect: nodeWhen.lift( client.disconnect ).bind( client ),
		getBucket: safeLift( client.getBucket.bind( client ) ),
		getKeys: function ( params ) {
			return when.promise( function ( resolve, reject, progress ) {
				var stream = client.getKeys( params );
				stream.on( 'data', progress );
				stream.on( 'error', reject );
				stream.on( 'end', resolve );
			} );
		},
		getIndex: function ( params ) {
			return when.promise( function ( resolve, reject, progress ) {
				var stream = client.getIndex( params );
				stream.on( 'data', progress );
				stream.on( 'error', reject );
				stream.on( 'end', resolve );
			} );
		}
	};
	return lifted;
}

// for cases when a simple when lift won't do
function safeLift( fn ) { // jshint ignore:line
	return function ( params ) {
		return when.promise( function ( resolve ) {
			fn( params, function ( err, result ) {
				if ( err ) {
					resolve( {} );
				} else {
					resolve( result );
				}
			} );
		} );
	};
}

module.exports = connect;