var _ = require( 'lodash' ),
	when = require( 'when' );

var content = function( obj, indexes ) {
	var tmp = { 'content_type': 'application/json', value: JSON.stringify( obj ) };
	if( indexes ) {
		tmp.indexes = indexes;
	}
	return tmp;
};

var Bucket = function( name, riak, options ) {
	this.name = _.isArray( name ) ? _.filter( name ).join( '_' ) : name;
	this.riak = riak;
	this.options = options;

	_.bindAll( this );
};

Bucket.prototype.get = function( key, single, multiple, error ) {
	return when.promise( function( resolve, reject ) {
		this.riak.client.get( {
			bucket: this.name,
			key: key
		}, function( err, reply ) {
			if( err ) {
				if( error ) {
					error( err );
				}
				reject( err );
			} else {
				if( !reply.content ) {
					if( single ) {
						single( undefined );
					}
					resolve( undefined );
				}
				else if( reply.content.length > 1 ) {
					var docs = _.map(
						_.filter( reply.content, function( v ) {
							return !v.deleted;
						}
					), function( doc ) {
						var result = doc.value;
						result.vclock = reply.vclock;
						this.riak.parseIndexes( result, doc );
						return result;
					}.bind( this ) );
					if( multiple ) {
						multiple( docs, reply.content );
					}
					resolve( docs );
				} else if ( reply.content.length > 0 ) {
					var doc = reply.content[ 0 ].value;
					doc.vclock = reply.vclock;
					this.riak.parseIndexes( doc, reply.content[ 0 ] );
					if( single ) {
						single( doc );
					}
					resolve( doc );
				}
			}
		}.bind( this ) );
	}.bind( this ) );
};

Bucket.prototype.getKeys = function( onKey, done ) {
	var stream = this.riak.client.getKeys( { bucket: this.name }, function( err, reply ) {
		done( err, reply );
	} );
	if( onKey ) {
		stream.on( 'data', onKey );
	}
};

Bucket.prototype.getByKeys = function( keys ) {
	var ids = _.map( keys, function( key ) { return { bucket: this.name, key: key } }.bind( this ) );
	return this.riak.getByKeys( ids );
};

Bucket.prototype.getKeysByIndex = function( index, start, finish, limit, continuation ) {
	if( _.isObject( index ) ) {
		start = start || index.start;
		finish = finish || index.finish;
		limit = limit || index.limit;
		continuation = continuation || index.continuation;
		index = index.index;
	}
	var originalIndex = index;
	if( index == '$key' || index == '$bucket' ) {
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
		bucket: this.name,
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

	var newContinuation;
	return when.promise( function( resolve, reject, notify ) {
		this.riak.client.getIndex( query )
			.on( 'data', function( data ) {
				if( data ) {
					if( data.continuation ) {
						newContinuation = data.continuation;
					} else {
						notify( data );
					}
				}
			} )
			.on( 'end', function() {
				if( newContinuation ) {
					resolve( 
						{
							index: originalIndex,
							limit: limit,
							start: start,
							finish: finish,
							continuation: newContinuation
						}
					);
				} else {
					resolve();
				}
			} )
			.on( 'error', reject );
	}.bind( this ) );
};

Bucket.prototype.put = function( key, obj, indexes, getBeforePut ) {
	var vclock,
		setVclock = function( done ) { 
			vclock = obj.vclock;
			delete obj[ 'vclock' ];
			done();
		};
	if( !obj || _.isObject( key ) ) {
		if( indexes ) {
			getBeforePut = indexes;
		}
		if( obj ) {
			indexes = this.processIndexes( obj );
		}
		obj = key;
		key = obj.id || this.riak.ids.getId();
	} else if( indexes ) {
		indexes = this.processIndexes( indexes );
	}
	obj.id = obj.id || key;
	var indices;
	if( !obj.vclock && getBeforePut ) {
		setVclock = function( done ) {
			this.get( { bucket: this.name, key: key }  )
				.then( function( result ) {
					if( _.isArray( result ) ) {
						vclock = result[ 0 ].vclock;
						if( result[ 0 ]._indices ) {
							indices = indices || result[ 0 ]._indices;	
						}
					} else {
						vclock = result.vclock;
						if( result._indices ) {
							indices = indices || result._indices;
						}
					}
					done();
				} )
				.then( null, function( err ) {
					done();
				} );
			}.bind( this );
	}
	return when.promise( function( resolve, reject, notify ) {
		setVclock( function() {
			indices = indexes || this.processIndexes( obj._indices );
			var request = {
				bucket: this.name,
				key: key,
				return_body: true,
				content: content( obj, indices )
			};
			if( vclock ) {
				request.vclock = vclock;
			}
			this.riak.client.put( request, function( err, reply ) {
				if( err ) {
					reject( err );
				} else {
					obj.id = key;
					obj.vclock = reply.vclock;
					resolve( key );
				}
			} );
		}.bind( this ) );
	}.bind( this ) );
};

Bucket.prototype.mutate = function( key, mutate ) {
	var mutator = function( mutandis, resolve, reject ) {
			try {
				var vclock = mutandis.vclock,
					doc = _.isArray( mutandis ) ? mutandis[ 0 ] : mutandis,
					mutatis = mutate( doc );
				this.put( key, mutatis )
					.then( null, function( err ) {
						reject( {
							reason: 'Could not apply changes to document with key "' + 
									key + '" in bucket "' + this.name + '"',
							error: err 
						} );
					}.bind( this ) )
					.then( function() {
						resolve( mutatis );
					}.bind( this ) );
			} catch ( err ) {
				reject( {
					reason: 'Could not apply changes to document with key "' + 
							key + '" in bucket "' + this.name + '"',
					error: err 
				} );
			}
		}.bind( this );

	return when.promise( function( resolve, reject ) {
		this.get( key )
			.then( function( result ) {
				if( result ) {
					mutator( result, resolve, reject );
				} else {
					reject( { 
						reason: 'Could not load document for apply with key "' + 
								key + '" in bucket "' + this.name + '"'
						} 
					);
				}
			}.bind( this ) )
			.then( null, function( err ) {
				reject( { 
					reason: 'Could not load document for apply with key "' + 
							key + '" in bucket "' + this.name + '"', 
					error: err } 
				);
			}.bind( this ) );
		}.bind( this ) );
};

Bucket.prototype.processIndexes = function( list ) {
	return _.map( list, function( val, key ) {
		if( /[_](bin|int)$/.test( key ) ) {
			return { key: key, value: val };
		}
		else if( _.isNumber( val ) ) {
			return { key: key + '_int', value: val };
		} else {
			return { key: key + '_bin', value: val };
		}
	} );
};

Bucket.prototype.del = function( key ) {
	if( _.isArray( key ) ) {
		throw "Multi-delete isn't supported"
	} else if ( _.isObject( key ) && key.id ) {
		key = key.id;
	}
	return when.promise( function( resolve, reject, notify ) {
		this.riak.client.del( {
				bucket: this.name,
				key: key 
			},
			function( err, reply ) {
			if( err ) {
				reject( err );
			} else {
				resolve();
			}
		}.bind( this ) );
	}.bind( this ) );
};

module.exports = Bucket;