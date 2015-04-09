var _ = require( 'lodash' );
var when = require( 'when' );
var machina = require( 'machina' );
var IndexManager = require( './indexes.js' );
var SchemaManager = require( './schema.js' );
var log = require( './log' )( 'bucket' );
var schemas, index;

function diff( one, two ) {
	var result = {};
	_.each( two, function( value, key ) {
		var orig = one[ key ];
		if ( orig !== value ) {
			result[ key ] = value;
		}
	} );
	return result;
}

function Bucket( bucket, options, riak, createBucket ) {
	schemas = schemas || new SchemaManager( riak );
	index = index || new IndexManager( riak );
	var bucketName = _.isArray( bucket ) ? _.filter( bucket ).join( '_' ) : bucket;
	var defaults = {
		schema: undefined,
		schemaPath: undefined,
		'allow_mult': true
	};
	if ( options.schema ) {
		defaults[ 'search_index' ] = bucketName + '_index'; // jshint ignore:line
	}
	var api = createBucket( riak, bucketName );
	var alias = options.alias;
	options = _.omit( options, 'alias' );
	options = _.defaults( options, defaults );
	var Monad = machina.Fsm.extend( {
		alias: alias || bucketName,
		name: bucketName,
		operate: function( call, args ) {
			var op = { operation: call, argList: args },
				promise = when.promise( function( resolve, reject, notify ) {
					op.resolve = resolve;
					op.reject = reject;
					op.notify = notify;
				} );
			this.handle( 'operate', op );
			return promise;
		},

		_assertIndex: function( name, schema ) {
			index.create( name, schema )
				.then( function( pause ) {
					setTimeout( function() {
						this.handle( 'index.asserted' );
					}.bind( this ), pause ? 10000 : 0 );
				}.bind( this ) )
				.then( null, function( err ) {
					this.handle( 'index.failed', err );
				}.bind( this ) );
		},
		_assertSchema: function( name, schemaPath ) {
			schemas.create( name, schemaPath )
				.then( function() {
					this.handle( 'schema.asserted' );
				}.bind( this ) )
				.then( null, function( err ) {
					this.handle( 'schema.failed', err );
				}.bind( this ) );
		},
		_create: function() {
			var self = this;
			log.debug( 'Getting bucket props for %s', bucketName );
			api.readBucket( riak, bucketName )
				.then( function( props ) {
					log.debug( 'Read props %s from bucket %s', JSON.stringify( props ), bucketName );
					var difference = diff( props, _.omit( options, 'schema', 'schemaPath' ) );
					if ( _.keys( difference ).length > 0 ) {
						riak.setBucket( { bucket: bucketName, props: difference } )
							.then( function() {
								self.handle( 'bucket.asserted' );
							} )
							.then( null, function( err ) {
								self.handle( 'bucket.failed', err );
							} );
					} else {
						self.handle( 'bucket.asserted' );
					}
				} )
				.then( null, function( err ) {
					log.error( 'failed to read props for bucket %s with %s', bucketName, err.stack );
				} );
		},
		initialState: 'checkingSchema',
		states: {
			creating: {
				_onEnter: function() {
					this._create();
				},
				'bucket.asserted': function() {
					log.debug( 'Bucket "%s" created with %s', bucketName, JSON.stringify( options ) );
					this.transition( 'ready' );
				},
				'bucket.failed': function( err ) {
					log.error( 'Bucket create for %s failed with %s', bucketName, err );
				},
				operate: function( /* call */ ) {
					this.deferUntilTransition( 'ready' );
				}
			},
			checkingSchema: {
				_onEnter: function() {
					if ( options.schema ) {
						log.debug( 'Checking for schema', options.schema );
						if ( options.schema && options.schemaPath ) {
							this._assertSchema( options.schema, options.schemaPath );
						} else {
							this.transition( 'checkingIndex' );
						}
					} else {
						log.debug( 'No schema specified for bucket %s, skipping to create bucket', bucketName );
						this.transition( 'creating' );
					}
				},
				'schema.asserted': function() {
					log.debug( 'Schema "%s" asserted', options.schema );
					this.transition( 'checkingIndex' );
				},
				'schema.failed': function( err ) {
					log.error( 'Schema assert for %s failed with %s', options.schema, err.stack );
				},
				operate: function( /* call */ ) {
					this.deferUntilTransition( 'ready' );
				}
			},
			checkingIndex: {
				_onEnter: function() {
					if ( options.search_index && options.schema ) {
						this._assertIndex( options.search_index, options.schema );
					} else {
						log.debug( 'Index or schema unspecified for bucket %s, skipping to create bucket', bucketName );
						this.transition( 'creating' );
					}
				},
				'index.asserted': function() {
					log.debug( 'Index "%s" created', options.search_index );
					this.transition( 'creating' );
				},
				'index.failed': function( err ) {
					log.error( 'Index assert for %s failed with %s', options.search_index, err );
				},
				operate: function( /* call */ ) {
					this.deferUntilTransition( 'ready' );
				}
			},
			ready: {
				operate: function( call ) {
					log.debug( 'Operation: %s', JSON.stringify( call ) );
					try {
						api[ call.operation ].apply( undefined, call.argList )
							.then( call.resolve, call.reject, call.notify );
					} catch ( err ) {
						log.error( 'Operation: %s failed with %s', JSON.stringify( call ), err );
						call.reject( err );
					}
				}
			}
		}
	} );

	var operations = [ 'del', 'get', 'getKeys', 'getByKeys', 'getKeysByIndex', 'getByIndex', 'mutate', 'put' ];
	var machine = new Monad();
	_.each( operations, function( name ) {
		machine[ name ] = function() {
			var list = Array.prototype.slice.call( arguments, 0 );
			return machine.operate( name, list );
		}.bind( machine );
	} );
	return machine;
}

module.exports = Bucket;