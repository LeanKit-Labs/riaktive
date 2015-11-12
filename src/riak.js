var _ = require( "lodash" );
var when = require( "when" );
var parallel = require( "when/parallel" );
var log = require( "./log" )( "riaktive.api" );
var Errors = require( "./errors" );

function buildIndexQuery( bucketName, bucketType, index, start, finish, limit, continuation ) {
	var progressIndex = getProgressIndex( arguments );
	if ( _.isObject( index ) ) {
		start = index.start;
		finish = index.finish;
		limit = index.limit || index.max_results;
		continuation = index.continuation;
		index = index.index || index.name;
	} else {
		if ( progressIndex > 4 || progressIndex < 0 ) {
			finish = finish || index.finish;
			if ( progressIndex > 5 || progressIndex < 0 ) {
				limit = limit || index.limit || index.max_results;
				if ( progressIndex > 6 || progressIndex < 0 ) {
					continuation = continuation || index.continuation;
				} else {
					continuation = undefined;
				}
			} else {
				limit = continuation = undefined;
			}
		} else {
			finish = limit = continuation = undefined;
		}
	}
	if ( index === "$key" || index === "$bucket" ) {
		void 0; // esformatter trashes the entire file w/o this b/c empty blocks
	} else if ( !/[_](bin|int)$/.test( index ) ) {
		if ( _.isNumber( start ) ) {
			index = index + "_int";
		} else {
			index = index + "_bin";
		}
	}

	var query = {
		bucket: bucketName,
		type: bucketType,
		index: index
	};

	if ( finish ) {
		query.qtype = 1;
		query.range_min = start;
		query.range_max = finish;
	} else {
		query.qtype = 0;
		query.key = start;
	}

	if ( limit ) {
		query.max_results = limit;
	}

	if ( continuation ) {
		query.continuation = continuation;
	}

	return query;
}

function buildPut( bucketName, bucketType, key, obj, indexes, original ) {
	var indices = indexes || processIndexes( original._indexes );
	var request = {
		bucket: bucketName,
		key: key,
		returnBody: true,
		content: content( obj, indices ),
		vclock: original.vclock || obj.vclock,
		bucketType: bucketType
	};
	if ( request.content.value.length > 64 ) {
		log.debug( "Putting %d bytes to %s in \"%s\"",
			request.content.value.length,
			key,
			bucketName );
	} else {
		log.debug( "Putting %s to \"%s\" in \"%s\"",
			request.content.value,
			key,
			bucketName );
	}

	return request;
}

function content( obj, indexes ) {
	var tmp = { contentType: "application/json", value: JSON.stringify( _.omit( obj, "_indexes" ) ) };
	if ( indexes ) {
		tmp.indexes = indexes;
	}
	return tmp;
}

function createBucket( idStrategy, riak, bucketName, bucketType ) {
	return {
		del: del.bind( undefined, riak, bucketName, bucketType ),
		get: get.bind( undefined, riak, bucketName, bucketType ),
		getByKeys: getByBucketKeys.bind( undefined, riak, bucketName, bucketType ),
		getByIndex: getByIndex.bind( undefined, riak, bucketName, bucketType ),
		getKeys: getKeys.bind( undefined, riak, bucketName, bucketType ),
		getKeysByIndex: getKeysByIndex.bind( undefined, riak, bucketName, bucketType ),
		mutate: mutate.bind( undefined, riak, bucketName, bucketType ),
		put: put.bind( undefined, riak, idStrategy, bucketName, bucketType ),
		readBucket: readBucket.bind( undefined, riak, bucketName, bucketType )
	};
}

function createIndex( riak ) {
	return {
		getByKeys: getByKeys.bind( undefined, riak )
	};
}

function del( riak, bucketName, bucketType, key ) {
	if ( _.isArray( key ) ) {
		throw new Error( "Multi-delete isn\"t supported" );
	} else if ( _.isObject( key ) && key.id ) {
		key = key.id;
	}
	return riak.del( {
		bucket: bucketName,
		key: key,
		bucketType: bucketType
	} );
}

function formatContent( reply, content ) {
	var doc = JSON.parse( content.value.toString() );
	doc.vclock = reply.vclock;
	parseIndexes( doc, content );
	return doc;
}

function get( riak, bucketName, bucketType, key, includeDeleted ) {
	return riak.get( { bucket: bucketName, key: key, bucketType: bucketType } )
		.then( function( reply ) {
			if ( reply.isNotFound ) {
				log.error( "Get \"%s\" from \"%s\" return an empty document!", key, bucketName );
				throw new Errors.EmptyResult( bucketName, key );
			}

			log.debug( "Get \"%s\" from \"%s\" returned %d documents with %d bytes",
				key,
				bucketName,
				reply.values.length,
				_.foldl( reply.values, function( x, y ) {
					return x + y.value.length;
				}, 0 )
			);
			var docs = scrubDocs( reply, includeDeleted );
			if ( _.isEmpty( docs ) ) {
				return undefined;
			} else if ( docs.length === 1 ) {
				return docs[ 0 ];
			} else {
				return docs;
			}
		}, function( err ) {
			return err;
		} );
}

function getByBucketKeys( riak, bucketName, bucketType, keys, includeDeleted ) {
	var notify = getProgressCallback( arguments );
	var all = _.map( keys, function( key ) {
		return function() {
			return get( riak, bucketName, bucketType, key, includeDeleted )
				.tap( notify );
		};
	} );
	return parallel( all );
}

function getByKeys( riak, keys ) {
	var notify = getProgressCallback( arguments );
	var all = _.map( keys, function( id ) {
		return function() {
			return get( riak, id.bucket, id.type, id.key, includeDeleted )
				.tap( notify );
		};
	} );
	return parallel( all );
}

function getByIndex( riak, bucketName, bucketType, index, start, finish, limit, continuation ) {
	var notify = getProgressCallback( arguments );
	var promises = [];

	function onDoc( _keys ) {
		var keys = _.isArray( _keys ) ? _keys : [ _keys ];

		_.each( keys, function( key ) {
			promises.push(
				get( riak, bucketName, bucketType, key )
					.tap( notify )
			);
		} );
	}
	return getKeysByIndex( riak, bucketName, bucketType, index, start, finish, limit, continuation, onDoc )
		.then( function( results ) {
			log.debug( "Resolving %d keys", promises.length );
			return when.all( promises )
				.then( function( docs ) {
					results.docs = _.sortBy( docs, "id" );
					return results;
				} );
		} );
}

function getKeys( riak, bucketName, bucketType ) {
	return riak.getKeys( { bucket: bucketName, type: bucketType } );
}

function getKeysByIndex( riak, bucketName, bucketType, index, start, finish, limit, continuation ) {
	var query = buildIndexQuery( bucketName, bucketType, index, start, finish, limit, continuation );
	var newContinuation;
	var notify = getProgressCallback( arguments );
	log.debug( "Requesting keys for \"%s\"", JSON.stringify( query ) );
	var keys = [];
	function onKey( data ) {
		if ( data ) {
			if ( data.continuation ) {
				newContinuation = data.continuation;
			} else {
				keys = keys.concat( data.keys );
				_.each( data.keys, function( key ) {
					notify( key );
				} );
			}
		}
		return data ? data.keys : [];
	}
	return riak.getIndex( query, onKey )
		.then( function() {
			if ( newContinuation ) {
				query.continuation = newContinuation;
			}
			query.keys = keys;
			return query;
		} );
}

function getProgressCallback( args ) {
	return _.findLast( Array.prototype.slice.call( args ), _.isFunction ) || _.noop;
}

function getProgressIndex( args ) {
	var list = Array.prototype.slice.call( args );
	return _.findIndex( list, _.isFunction ) || list.length;
}

function includeDeleted( flag ) {
	return function( doc ) {
		return flag ? true : !doc.deleted;
	};
}

function mutate( riak, bucketName, bucketType, key, mutateFn ) {
	function noDocument() {
		throw new Errors.MissingDocument( bucketName, key );
	}
	function onDocument( original ) {
		if ( _.isArray( original ) ) {
			throw new Errors.SiblingMutation( bucketName, key );
		} else {
			var mutatis = mutateFn( _.cloneDeep( original ) );
			var changes = _.omit( mutatis, "vtag", "vlock" );
			var origin = _.omit( original, "vtag", "vlock" );
			if ( _.isEqual( changes, origin ) ) {
				log.debug( "No changes to document \"%s\" in \"%s\"", key, bucketName );
				return original;
			} else {
				mutatis.vclock = original.vclock;
				log.debug( "Mutated document \"%s\" in \"%s\"", key, bucketName );
				return put( riak, undefined, bucketName, bucketType, key, mutatis )
					.then( function() {
						return mutatis;
					} );
			}
		}
	}
	function onError( err ) {
		return new Errors.MutationFailed( bucketName, key, err );
	}
	return get( riak, bucketName, bucketType, key )
		.then( onDocument, noDocument )
		.catch( onError );
}

function parseIndexes( doc, obj ) {
	var collection = {};
	if ( obj.indexes && !doc._indexes ) {
		_.each( obj.indexes, function( rawVal, rawKey ) {
			var key = rawKey.replace( "_int", "" ).replace( "_bin", "" );
			if ( /_int$/.test( rawKey ) ) {
				if ( collection[ key ] ) {
					collection[ key ] = _.flatten( [ collection[ key ], parseInt( rawVal ) ] );
				} else {
					collection[ key ] = parseInt( rawVal );
				}
			} else {
				if ( collection[ key ] ) {
					collection[ key ] = _.flatten( [ collection[ key ], rawVal ] );
				} else {
					collection[ key ] = rawVal;
				}
			}
		} );
		doc._indexes = collection;
	}
}

function processIndexes( list ) {
	return _.flatten( _.map( list, function( val, key ) {
		var isArray = _.isArray( val );
		var sample = isArray ? val[ 0 ] : val;
		var vals = isArray ? val : [ val ];
		if ( /[_](bin|int)$/.test( key ) ) {
			return _.map( vals, function( x ) {
				return { key: key, value: x };
			} );
		} else if ( _.isNumber( sample ) ) {
			return _.map( vals, function( x ) {
				return { key: key + "_int", value: x };
			} );
		} else {
			return _.map( vals, function( x ) {
				return { key: key + "_bin", value: x };
			} );
		}
	} ) );
}

function put( riak, idStrategy, bucketName, bucketType, key, obj, indexes, getBeforePut ) {
	if ( _.isObject( key ) ) {
		getBeforePut = indexes;
		indexes = obj;
		obj = key;
		key = undefined;
	}
	key = key || obj.id || idStrategy();
	obj.id = obj.id || key;
	indexes = indexes || obj._indexes;
	if ( indexes ) {
		indexes = processIndexes( indexes );
	}

	var getOriginal = function() {
		var vclock = obj.vclock;
		delete obj.vclock;
		return when( {
			type: bucketType,
			vclock: vclock,
			_indexes: obj._indexes
		} );
	};

	if ( !obj.vclock && getBeforePut ) {
		getOriginal = function() {
			return get( riak, bucketName, bucketType, key )
				.then( function( result ) {
					return _.isArray( result ) ? result[ 0 ] : result;
				} );
		};
	}

	var request = when.try( buildPut, bucketName, bucketType, key, obj, indexes, getOriginal() );
	return when.try( riak.put, request )
		.then( function( reply ) {
			obj.id = key;
			obj.vclock = reply.vclock;
			return key;
		} );
}

function readBucket( riak, bucketName, bucketType ) {
	bucketName = _.isObject( bucketName ) ? bucketName.bucketName : bucketName;
	var params = { bucket: bucketName, type: bucketType };

	return riak.getBucket( params )
		.then( null, function( err ) {
			log.error( "Failed to read bucket properties for \"%s\" with %s", bucketName, err.stack );
			return {};
		} )
		.then( function( bucket ) {
			log.debug( "Read bucket properties for \"%s\": %j", bucketName, bucket.props || {} );
			return bucket.props || {};
		} );
}

function scrubDocs( reply, inclusive ) {
	var filtered = _.filter( reply.values, includeDeleted( inclusive ) );
	return _.map( filtered, formatContent.bind( undefined, reply ) );
}

exports.createBucket = createBucket;
exports.createIndex = createIndex;
