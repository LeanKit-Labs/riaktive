var	_ = require( 'lodash' );
var	when = require( 'when' );
var	debug = require( 'debug' )( 'riaktive:api' );

function buildIndexQuery( bucketName, index, start, finish, limit, continuation ) {
	if( _.isObject( index ) ) {
		start = start || index.start;
		finish = finish || index.finish;
		limit = limit || index.limit || index.max_results;
		continuation = continuation || index.continuation;
		index = index.index;
	}
	if( index === '$key' || index === '$bucket' ) {
		// do nothing
	}
	else if( !/[_](bin|int)$/.test( index ) ) {
		if( _.isNumber( start ) ) {
			index = index + '_int';
		} else {
			index = index + '_bin';
		}
	}

	var query = {
		bucket: bucketName,
		index: index
	};

	if( finish ) {
		query.qtype = 1;
		query.range_min = start;
		query.range_max = finish;
	} else {
		query.qtype = 0;
		query.key = start;
	}

	if( limit ) {
		query.max_results = limit;
	}

	if( continuation ) {
		query.continuation = continuation;
	}
	return query;
}

function buildPut( bucketName, key, obj, indexes, original ) {
	var indices = indexes || processIndexes( original._indexes );
	var request = {
		bucket: bucketName,
		key: key,
		return_body: true,
		content: content( obj, indices ),
		vclock: original.vclock || obj.vclock
	};
	debug( 'Putting %s to %s in %s', JSON.stringify( request ), key, bucketName );
	return request;
}

function content( obj, indexes ) { // jshint ignore:line
	delete obj._indexes;
	var tmp = { 'content_type': 'application/json', value: JSON.stringify( obj ) };
	if( indexes ) {
		tmp.indexes = indexes;
	}
	return tmp;
}

function del( riak, bucketName, key ) {
	if( _.isArray( key ) ) {
		throw new Error( 'Multi-delete isn\'t supported' );
	} else if ( _.isObject( key ) && key.id ) {
		key = key.id;
	}
	return riak.del( {
		bucket: bucketName,
		key: key
	} );
}

function formatContent( reply, content ) {
	var doc = JSON.parse( content.value.toString() );
	doc.vclock = reply.vclock;
	parseIndexes( doc, content );
	return doc;
}

function get( riak, bucketName, key, includeDeleted ) {
	return riak.get( { bucket: bucketName, key: key } )
		.then( null, function( err ) {
			return err;
		} )
		.then( function( reply ) {
			debug( 'Get %s in %s returned %s (raw)', key, bucketName, JSON.stringify( reply ) );
			var docs = scrubDocs( reply, includeDeleted );
			if( _.isEmpty( docs ) ) {
				return undefined;
			} else if ( docs.length === 1 ) {
				return docs[ 0 ];
			} else {
				return docs;
			}
		} );
}

function getByBucketKeys( riak, bucketName, keys, includeDeleted ) {
	return when.promise( function( resolve, reject, notify ) {
		var all = _.map( keys, function( key ) {
			return get( riak, bucketName, key, includeDeleted )
				.then( notify );
		} );
		when.all( all ).then( resolve, reject );
	} );
}

function getByKeys( riak, keys ) {
	return when.promise( function( resolve, reject, notify ) {
		var all = _.map( keys, function( id ) {
			return get( riak, id.bucket, id.key, includeDeleted )
				.then( notify );
		} );
		when.all( all ).then( resolve, reject );
	} );
}

function getByIndex( riak, bucketName, index, start, finish, limit, continuation ) {
	return when.promise( function( resolve, reject, notify ) {
		var promises = [];
		getKeysByIndex( riak, bucketName, index, start, finish, limit, continuation )
			.progress( function( keys ) {
				_.each( keys, function( key ) {
					promises.push(
						get( riak, bucketName, key )
							.then( notify )
					);
				} );
			} )
			.then( function( next ) {
				debug( 'Resolving %s keys', promises.length );
				when.all( promises ).then( function() {
					resolve( next );
				} );
			} );		
	} );
}

function getKeys( riak, bucketName ) {
	return riak.getKeys( { bucket: bucketName } );
}

function getKeysByIndex( riak, bucketName, index, start, finish, limit, continuation ) { // jshint ignore:line
	var query = buildIndexQuery( bucketName, index, start, finish, limit, continuation );
	var newContinuation;
	debug( 'Requesting keys for %s', JSON.stringify( query ) );
	return riak.getIndex( query )
			.then( function() {
				if( newContinuation ) {
					query.continuation = newContinuation;
				}
				return query;
			} )
			.progress( function( data ) {
				if( data ) {
					if( data.continuation ) {
						newContinuation = data.continuation;
					}
				}
				return data ? data.keys : [];
			} );
}

function includeDeleted( flag ) { // jshint ignore:line
	return function( doc ) { return flag ? true : !doc.deleted; };
}

function mutate( riak, bucketName, key, mutateFn ) {
	return get( riak, bucketName, key )
		.then( null, function() {
			throw new Error( 'Cannot mutate - no document with key "' +  key + '" in bucket "' + bucketName + '"' );
		} )
		.then( function( mutandis ) {
			if( _.isArray( mutandis) ) {
				throw new Error( 'Cannot mutate - siblings exist for key "' + key + '" in bucket "' + bucketName + '"' );
			} else {
				var mutatis = mutateFn( mutandis );
				mutatis.vclock = mutandis.vclock;
				debug( 'mutated to %s', JSON.stringify( mutatis ) );
				return put( riak, bucketName, key, mutatis );
			}
		} )
		.then( null, function( err ) {
			return new Error( 'Mutate for key "' + key + '" in bucket "' + bucketName + '" failed with: ' + err.stack );
		} )
		.catch( function( err ) {
			return new Error( 'Mutate for key "' + key + '" in bucket "' + bucketName + '" failed with: ' + err.stack );
		} );
}

function parseIndexes( doc, obj ) { // jshint ignore:line
	var collection = {};
	if( obj.indexes && !doc._indexes ) {
		_.each( obj.indexes, function( index ) {
			var key = index.key.replace( '_int', '' ).replace( '_bin', '' );
			if( /_int$/.test( index.key ) ) {
				
				if( collection[ key ] ) {
					collection[ key ] = _.flatten( [ collection[ key ], parseInt( index.value ) ] );
				} else {
					collection[ key ] = parseInt( index.value );	
				}
			} else {
				if( collection[ key ] ) {
					collection[ key ] = _.flatten( [ collection[ key ], index.value ] );
				} else {
					collection[ key ] = index.value;
				}
			}
		} );
		doc._indexes = collection;
	}
}

function processIndexes( list ) { // jshint ignore:line
	return _.flatten( _.map( list, function( val, key ) {
		var isArray = _.isArray( val );
		var sample = isArray ? val[ 0 ] : val;
		var vals = isArray ? val : [ val ];
		if( /[_](bin|int)$/.test( key ) ) {
			return _.map( vals, function( x ) { 
				return { key: key, value: x }; 
			} );
		}
		else if( _.isNumber( sample ) ) {
			return _.map( vals, function( x ) {
				return { key: key + '_int', value: x };
			} );
		} else {
			return _.map( vals, function( x ) {
				return { key: key + '_bin', value: x };
			} );
		}
	} ) );
}

function put( riak, bucketName, key, obj, indexes, getBeforePut ) { // jshint ignore:line
	if( !obj || _.isObject( key ) ) {
		if( indexes ) {
			getBeforePut = indexes;
		}
		if( obj ) {
			indexes = processIndexes( obj );
		}
		obj = key;
		key = obj.id || riak.ids.getId();
	} else if( indexes ) {
		indexes = processIndexes( indexes );
	}
	if( _.isEmpty( indexes ) && obj._indexes ) {
		indexes = processIndexes( obj._indexes );
	}
	obj.id = obj.id || key;
	
	var getOriginal = function() { 
		var vclock = obj.vclock;
		delete obj.vclock;
		return when( {
			vclock: vclock,
			_indexes: obj._indexes
		} );
	};

	if( !obj.vclock && getBeforePut ) {
		getOriginal = function( ) {
			return get( riak, bucketName, key  )
				.then( function( result ) {
					return _.isArray( result ) ? result[ 0 ] : result;
				} );
			};
	}

	var request = when.try( buildPut, bucketName, key, obj, indexes, getOriginal() );
	return when.try( riak.put, request )
		.then( function( reply ) {
			obj.id = key;
			obj.vclock = reply.vclock;
			return key;
		} );
}

function readBucket( riak, bucketName ) {
	return riak.getBucket( { bucket: bucketName } )
		.then( null, function( err ) {
			debug( 'Failed to read bucket properties for %s with %s', bucketName, err.stack );
			return {};
		} )
		.then( function ( bucket ) {
			debug( 'Read bucket properties for %s: %s', bucketName, bucket.props || {} );
			return bucket.props || {};
		} );
}

function scrubDocs( reply, inclusive ) { // jshint ignore:line
	var filtered = _.filter( reply.content, includeDeleted( inclusive ) );
	return _.map( filtered, formatContent.bind( undefined, reply ) );
}

function createBucket( riak, bucketName ) {
	return {
		del: del.bind( undefined, riak, bucketName ),
		get: get.bind( undefined, riak, bucketName ),
		getByKeys: getByBucketKeys.bind( undefined, riak, bucketName ),
		getByIndex: getByIndex.bind( undefined, riak, bucketName ),
		getKeys: getKeys.bind( undefined, riak, bucketName ),
		getKeysByIndex: getKeysByIndex.bind( undefined, riak, bucketName ),
		mutate: mutate.bind( undefined, riak, bucketName ),
		put: put.bind( undefined, riak, bucketName ),
		readBucket: readBucket.bind( undefined, riak, bucketName )
	};
}

function createIndex( riak ) {
	return {
		getByKeys: getByKeys.bind( undefined, riak )
	};
}

exports.createBucket = createBucket;
exports.createIndex = createIndex;