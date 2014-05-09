var riakpbc = require( 'riakpbc' ),
	_ = require( 'lodash' ),
	when = require( 'when' ),
	fs = require( 'fs' ),
	path = require( 'path' ),
	Bucket = require( './bucket.js' ),
	Search = require( './search.js' );

module.exports = function( config, nodeId ) {

	var SchemaName,
		SchemaPath;

	var Riak = function() {
		this.connected = false;

		SchemaName = config.get( 'schemaName', 'riaktive_schema');
		var defaultPath = path.resolve( path.join( __dirname, './default_solr.xml' ) );
		SchemaPath = config.get( 'schemaPath', defaultPath );

		this.client = riakpbc.createClient( {
			host: config.get( 'RIAK_SERVER', 'ubuntu' ),
			port: config.get( 'RIAK_PBC', 8087 ),
			timeout: 5000
		} );

		this.idSetup = when.promise( function( resolve, reject, notify ) {
			var ready = function() {
				resolve();
			}.bind( this );
			if( nodeId ) {
				this.ids = require( 'sliver' )( nodeId, ready );
			} else {
				this.ids = require( 'sliver' )( ready );
			}
		}.bind( this ) );
		this.Index = Search( config, this );
		_.bindAll( this );
	};

	Riak.prototype.assertIndex = function( index, schemaName ) {
		return when.promise( function( resolve, reject, notify ) {
			this.client.ykGetIndex( { name: index }, function( err, reply ) {
				if( err ) {
					this.client.ykPutIndex( {
						index: {
							name: index,
							schema: schemaName
						}
					}, function( err, result ) {
						if( err ) {
							reject( err );
						} else {
							setTimeout( function() {
								resolve();
							}, 10000);
						}
					} );
				} else {
					resolve();
				}
			}.bind( this ) );
		}.bind( this ) );
	};

	Riak.prototype.assertSchema = function( name, schemaPath ) {
		var schema = fs.readFileSync( path.resolve( schemaPath ) );
		return when.promise( function( resolve, reject, notify ) {
			this.client.ykGetSchema( { name: name }, function( err, reply ) {
				if( err || reply.schema.content != schema ) {
					this.client.ykPutSchema( { schema: { name: name, content: schema } }, 
						function( err ) {
							if( err ) {
								reject( err );
							} else {
								resolve();
							}
						} );
				} else {
					resolve();
				}
			}.bind( this ) );
		}.bind( this ) );
	};

	Riak.prototype.connect = function() {
		return when.promise( function( resolve, reject, notify ) {
			this.client.connect( function( err ) {
				if( err ) {
					reject( err );
				} else {
					this.connected = true;
					if( this.ready ) {
						resolve();
					} else {
						when.all( [ 
							this.idSetup, 
							this.assertSchema( SchemaName, SchemaPath ) 
						] ).done( function() {
							this.ready = true;
							resolve();
						} );
					}
				}
			}.bind( this ) );
		}.bind( this ) );
	};

	Riak.prototype.createBucket = function( bucketName, options ) {
		var bucket = new Bucket( bucketName, this ),
			alias = options.alias;
		bucketName = bucket.name;
		var	schemaCheck,
			defaults = { search_index: bucketName + '_index', schema: SchemaName };
		options = _.merge( defaults, ( options || {} ) );
		if( options.alias ) {
			delete options.alias;
		}
		var create = function( resolve, reject ) {
			this.client.setBucket(
				{ bucket: bucketName, props: options }, 
				function( err, reply ) {
					if( err ) {
						reject( err );
					} else {
						this[ bucketName ] = bucket;
						this[ alias ] = bucket;
						resolve( bucket );
					}
				}.bind( this )
			);
		}.bind( this );

		if( options.schema && options.schemaPath ) {
			schemaCheck = this.assertSchema( options.schema, options.schemaPath );
		} else {
			schemaCheck = when.promise( function( resolve ) {
				resolve(); 
			} );
		}

		var prerequisites = when.promise( function( resolve, reject, notify ) {
			if( options.search_index ) {
				if( schemaCheck ) {
					schemaCheck.done( function () {
						this.assertIndex( options.search_index, options.schema || SchemaName )
							.done( function() {
								resolve();
							} );
					}.bind( this ) );
				} else {
					this.assertIndex( options.search_index, options.schema || SchemaName )
						.done( function() {
							resolve();
						} );
				}
			} else {
				resolve();
			}
		}.bind( this ) );

		return when.promise( function( resolve, reject, notify ) {
			prerequisites
				.done( function() {
					create( resolve, reject );		
				}.bind( this ) );
		}.bind( this ) );
	};

	Riak.prototype.getSearchIndex = function( index ) {
		return new this.Index( index );
	};

	Riak.prototype.getByKeys = function( ids ) {
		var promises = _.map( ids, function( id ) {
			return when.promise( function( resolve, reject, notify ) {
				this.client.get( { bucket: id.bucket, key: id.key },
					function( err, reply ) {
						if( err ) {
							reject( err );
						} else {
							if( reply.content && reply.content.length > 1 ) {
								var docs = _.map( 
									_.filter( reply.content, function( v ) {
										return !v.deleted;
									}
								), function( d ) {
									var doc = d.value;
									doc.vclock = reply.vclock;
									this.parseIndexes( doc, d );
									return doc;
								} );
								notify( docs );
								resolve( docs );
							} else if ( reply.content && reply.content.length > 0 ) {
								var doc = reply.content[ 0 ].value;
								doc.vclock = reply.vclock;
								this.parseIndexes( doc, reply.content[ 0 ] );
								notify( doc );
								resolve( doc );
							}
						}
					}.bind( this ) );
			}.bind( this ) );
		}.bind( this ) );
		return when.all( promises );
	};

	Riak.prototype.parseIndexes = function( doc, obj ) {
		var collection = {};
		if( obj.indexes && !doc._indices ) {
			_.each( obj.indexes, function( index ) {
				if( /_int$/.test( index.key ) ) {
					collection[ index.key ] = parseInt( index.value );
				} else {
					collection[ index.key ] = index.value;
				}
			} );
			doc._indices = collection;
		}
	};

	var riak = new Riak();
	return riak;
};