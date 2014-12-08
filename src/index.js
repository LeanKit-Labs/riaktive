var _ = require( 'lodash' );
var when = require( 'when' );
var nodeWhen = require( 'when/node' );
var riakpbc = require( 'riakpbc' );
var RiakConnection = require( 'riakpbc/lib/connection');
var createBucket = require( './bucket.js' );
var solr = require( './search.js' );
var uuid = require( 'node-uuid' );
var pool = require( './pool.js' );

function connect( options ) {
	var nodes = [];
	var nodeId;
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
		nodeId = options.nodeId;
	}

	if ( !nodeId ) {
		nodeId = uuid.v4();
	}

	var defaultNode = {
		host: 'localhost',
		port: 8087,
		http: 8098,
		connectTimeout: 2000
	};
	
	var client = riakpbc.createClient( {
		nodes: [],
		connectTimeout: options.timeout
	} );

	normalized.nodes = _.map( normalized.nodes, function( n ) {
		n.connectTimeout = n.timeout;
		delete n.timeout;
		return n;
	} );

	client.pool = pool( normalized, function( node ) {
		var opts = _.pick( _.defaults( node, defaultNode ), [ 'host', 'port' ] );
		return when.promise( function( resolve, reject ) {
			var conn = new RiakConnection( node );
			conn.connect( function( err ) {
				if( err ) {
					reject( err );
				} else {
					resolve( conn );
				}
			} );
		} );
	} );

	return lift( client, nodeId );
}

// this is here to convert Node style callbacks to promises
function lift( client, nodeId ) { // jshint ignore:line
	var lifted = {
		bucket: function( bucketName, options ) {
			var bucket = createBucket( bucketName, options || {}, this, nodeId );
			this[ bucket.name ] = bucket;
			if( bucket.alias ) {
				this[ bucket.alias ] = bucket;
			}
			return bucket;
		},
		index: function( indexName, alias ) {
			var index = solr( this, indexName );
			this[ indexName ] = index;
			if( alias ) {
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
		reset: function() {
			client.pool.restart();
		},
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
		yzGetIndex: nodeWhen.lift( client.getSearchIndex ).bind( client ),
		yzPutIndex: nodeWhen.lift( client.putSearchIndex ).bind( client ),
		yzDeleteIndex: nodeWhen.lift( client.delSearchIndex ).bind( client ),
		yzPutSchema: nodeWhen.lift( client.putSearchSchema ).bind( client ),
		yzGetSchema: safeLift( client.getSearchSchema.bind( client ) ),
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

module.exports = { connect: connect };