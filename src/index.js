require( "./log" )();
var _ = require( "lodash" );
var when = require( "when" );
var nodeWhen = require( "when/node" );
var Riak = require( "basho-riak-client" );
var riakpbc = require( "@lklabs/riakpbc" );
var RiakConnection = require( "@lklabs/riakpbc/lib/connection" );
var createBucket = require( "./bucket.js" );
var api = require( "./riak.js" );
var solr = require( "./search.js" );
var uuid = require( "node-uuid" );
var pool = require( "./pool.js" );
var idStrategy = uuid.v4;

function configureLogging( logConfig ) {
	require( "./log" )( logConfig );
}

function connect( options ) {
	var nodes = [];
	var normalized = {};
	if ( _.isArray( options ) ) {
		nodes = options;
		normalized = { nodes: nodes };
	} else if ( _.isObject( options ) ) {
		if ( options.nodes ) {
			nodes = options.nodes || nodes;
			normalized = options;
		} else {
			nodes = [ options ];
			normalized = { nodes: nodes };
		}
	}

	var defaultNode = {
		host: "localhost",
		port: 8087,
		http: 8098,
		connectTimeout: 2000,
		connections: 5
	};

	var client = riakpbc.createClient( {
		nodes: [],
		connectTimeout: options.timeout || 2000
	} );

	normalized.nodes = _.map( normalized.nodes, function( n ) {
		n.connectTimeout = n.timeout;
		delete n.timeout;
		return _.merge( {}, defaultNode, n );
	} );

	client.pool = pool( normalized, function( node ) {
		return when.promise( function( resolve, reject ) {
			var conn = new RiakConnection( node );
			conn.connect( function( err ) {
				if ( err ) {
					reject( err );
				} else {
					resolve( conn );
				}
			} );
		} );
	} );

	var hosts = _.map( normalized.nodes, function( n ) {
		return n.host + ":" + n.port;
	} );

	var client2 = new Riak.Client( hosts );

	return lift( client, client2 );
}

// this is here to convert Node style callbacks to promises
function lift( client, client2 ) { // jshint ignore:line
	var liftedGetBucket = safeLift( client2.fetchBucketProps.bind( client2 ) );
	var liftedSetBucket = safeLift( client2.storeBucketProps.bind( client2 ) );
	var liftedFetchBucketTypeProps = nodeWhen.lift( client2.fetchBucketTypeProps.bind( client2 ) );
	var liftedStoreBucketTypeProps = nodeWhen.lift( client2.storeBucketTypeProps.bind( client2 ) );

	var lifted = {
		bucket: function( bucketName, options ) {
			var bucket = this[ bucketName ];
			if ( !bucket ) {
				bucket = createBucket( bucketName, options || {}, this, api.createBucket.bind( api, idStrategy ) );
				this[ bucket.name ] = bucket;
			}
			if ( bucket.alias ) {
				this[ bucket.alias ] = bucket;
			}
			return bucket;
		},
		index: function( indexName, alias ) {
			var index = this[ indexName ];
			if ( !index ) {
				index = solr( this, indexName );
				this[ indexName ] = index;
			}
			if ( alias ) {
				this[ alias ] = index;
			}
			return index;
		},
		close: function() {
			client.pool.close();
		},
		getNode: function() {
			return client.pool.getNode();
		},
		pool: client.pool,
		reset: function() {
			client.pool.restart();
		},
		getBuckets: function( _options, _notify ) {
			var notify;
			var options;
			if ( _.isFunction( _options ) ) {
				notify = _options;
				options = {};
			} else {
				options = _options;
				notify = _notify;
			}

			var opts = _.pick( options, [ "bucketType", "timeout" ] );
			if ( options.type && !opts.bucketType ) {
				opts.bucketType = options.type;
			}

			// Forcing to stream for now to strongly encourage safety
			// Safety first, kids
			opts.stream = true;

			return when.promise( function( resolve, reject ) {
				// This lib does not actually return a JS stream
				// but essentially fires a notify callback when data is retrieved
				var stream = client2.listBuckets( opts, function( err, result ) {
					if ( err ) {
						return reject( err );
					}
					notify( result.buckets );
					if ( result.done ) {
						return resolve();
					}
				} );
			} );
		},
		setBucket: function( options ) {
			var opts = {
				bucket: options.bucket,
				bucketType: options.type
			};

			_.extend( opts, options.props );

			if ( opts.search_index ) {
				opts.searchIndex = opts.search_index;
				delete opts.search_index;
			}

			return liftedSetBucket( opts );
		},
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
		setBucketType: function( _opts ) {
			var options = _opts || {};

			if ( options.type ) {
				options.bucketType = _opts.type;
				delete options.type;
			}

			return liftedStoreBucketTypeProps( options );
		},
		getBucketType: function( _opts ) {
			var options;
			if ( _.isString( _opts ) ) {
				options = {
					bucketType: _opts
				};
			} else {
				options = _.pick( _opts, "bucketType" );
				options.bucketType = options.bucketType || _opts.type;
			}

			return liftedFetchBucketTypeProps( options );
		},
		updateDtype: nodeWhen.lift( client.updateDtype ).bind( client ),
		fetchDtype: nodeWhen.lift( client.fetchDtype ).bind( client ),
		yzGetIndex: nodeWhen.lift( client.getSearchIndex ).bind( client ),
		yzPutIndex: nodeWhen.lift( client.putSearchIndex ).bind( client ),
		yzDeleteIndex: nodeWhen.lift( client.delSearchIndex ).bind( client ),
		yzPutSchema: nodeWhen.lift( client.putSearchSchema ).bind( client ),
		yzGetSchema: safeLift( client.getSearchSchema.bind( client ) ),
		connect: nodeWhen.lift( client.connect ).bind( client ),
		disconnect: nodeWhen.lift( client.disconnect ).bind( client ),
		getBucket: function( options ) {
			var opts = _.pick( options, "bucket", "bucketType" );
			if ( options.type ) {
				opts.bucketType = options.type;
			}
			return liftedGetBucket( opts );
		},
		getKeys: function( params, progress ) {
			var notify = progress || _.noop;
			return when.promise( function( resolve, reject ) {
				var stream = client.getKeys( params );
				stream.on( "data", notify );
				stream.on( "error", reject );
				stream.on( "end", resolve );
			} );
		},
		getIndex: function( params, progress ) {
			var notify = progress || _.noop;
			return when.promise( function( resolve, reject ) {
				var stream = client.getIndex( params );
				stream.on( "data", notify );
				stream.on( "error", reject );
				stream.on( "end", resolve );
			} );
		}
	};
	return lifted;
}

// for cases when a simple when lift won"t do
function safeLift( fn ) { // jshint ignore:line
	return function( params ) {
		return when.promise( function( resolve ) {
			fn( params, function( err, result ) {
				if ( err ) {
					resolve( {} );
				} else {
					resolve( result );
				}
			} );
		} );
	};
}

function setIdStrategy( idFn ) {
	idStrategy = idFn;
}

module.exports = {
	connect: connect,
	configureLogging: configureLogging,
	setIdStrategy: setIdStrategy
};
