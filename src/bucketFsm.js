var _ = require( 'lodash' );
var when = require( 'when' );
var machina = require( 'machina' );
var IndexManager = require( './indexManager.js' );
var SchemaManager = require( './schemaManager.js' );
var debug = require( 'debug' )( 'riaktive:bucket' );
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

function fsm( name, options, riak, createBucket ) {
	schemas = schemas || new SchemaManager( riak );
	index = index || new IndexManager( riak );
	var bucketName = _.isArray( name ) ? _.filter( name ).join( '_' ) : name;
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
				.then( function( waitMS ) {
					waitMS = waitMS || 0;
					setTimeout( function() {
						this.handle( 'index.asserted' );
					}.bind( this ), waitMS );
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
			debug( 'Getting bucket props for %s', bucketName );
			api.readBucket( riak, bucketName )
				.then( function( props ) {
					var difference = diff( props, _.omit( options, 'schema', 'schemaPath' ) );
					debug( 'Read props %s from bucket %s; diff %s', JSON.stringify( props ), bucketName, JSON.stringify( difference ) );
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
					debug( 'failed to read props for bucket %s with %s', bucketName, err.stack );
				} );
		},
		initialState: 'checkingSchema',
		states: {
			creating: {
				_onEnter: function() {
					this._create();
				},
				'bucket.asserted': function() {
					debug( 'Bucket "%s" created with %s', bucketName, JSON.stringify( options ) );
					this.transition( 'ready' );
				},
				'bucket.failed': function( err ) {
					debug( 'Bucket create for %s failed with %s', bucketName, err );
				},
				operate: function( /* call */ ) {
					this.deferUntilTransition( 'ready' );
				}
			},
			checkingSchema: {
				_onEnter: function() {
					if ( options.schema ) {
						debug( 'Checking for schema', options.schema );
						if ( options.schemaPath ) {
							this._assertSchema( options.schema, options.schemaPath );
						} else {
							debug( 'No schema path specified for bucket %s, skipping to check index', bucketName );
							this.transition( 'checkingIndex' );
						}
					} else {
						debug( 'No schema specified for bucket %s, skipping to create bucket', bucketName );
						this.transition( 'creating' );
					}
				},
				'schema.asserted': function() {
					debug( 'Schema "%s" asserted', options.schema );
					this.transition( 'checkingIndex' );
				},
				'schema.failed': function( err ) {
					debug( 'Schema assert for %s failed with %s', options.schema, err.stack );
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
						debug( 'Index or schema unspecified for bucket %s, skipping to create bucket', bucketName );
						this.transition( 'creating' );
					}
				},
				'index.asserted': function() {
					debug( 'Index "%s" created', options.search_index );
					this.transition( 'creating' );
				},
				'index.failed': function( err ) {
					debug( 'Index assert for %s failed with %s', options.search_index, err );
				},
				operate: function( /* call */ ) {
					this.deferUntilTransition( 'ready' );
				}
			},
			ready: {
				operate: function( call ) {
					debug( 'Operation: %s', JSON.stringify( call ) );
					try {
						api[ call.operation ].apply( undefined, call.argList )
							.then( call.resolve, call.reject, call.notify );
					} catch (err) {
						debug( 'Operation: %s failed with %s', JSON.stringify( call ), err );
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

module.exports = fsm;
