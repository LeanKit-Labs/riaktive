require( '../setup' );
var realDebug = require( 'debug' )( 'riaktive:bucket' );

function noOp() {
}



describe( 'bucketFsm', function() {

	var spy, riak, api, indexMgr, schemaMgr;

	function buildStubs( specifiedBehavior ) {

		specifiedBehavior = specifiedBehavior || {};

		var defaultBehavior = {
			readBucket: function() {
				return when.resolve( {} );
			},
			put: function() {
				return when.resolve();
			},
			setBucket: function() {
				return when.resolve();
			},
			indexCreate: function() {
				return when.resolve( 20 );
			},
			schemaCreate: function() {
				return when.resolve();
			},
			debug: function() {
				var args = Array.prototype.slice.call( arguments, 0 );
				realDebug( args );
			}
		};

		function behavior( forStub ) {
			return specifiedBehavior[ forStub ] || defaultBehavior[ forStub ];
		}

		api = { readBucket: noOp, put: noOp };
		riak = { setBucket: noOp };
		indexMgr = { create: noOp };
		schemaMgr = { create: noOp };

		spy = {
			readBucket: sinon.stub( api, 'readBucket', behavior( 'readBucket' ) ),
			put: sinon.stub( api, 'put', behavior( 'put' ) ),
			setBucket: sinon.stub( riak, 'setBucket', behavior( 'setBucket' ) ),
			indexCreate: sinon.stub( indexMgr, 'create', behavior( 'indexCreate' ) ),
			schemaCreate: sinon.stub( schemaMgr, 'create', behavior( 'schemaCreate' ) ),
			debug: behavior( 'debug' )
		};
	}

	function IndexMgr() {
		return indexMgr;
	}

	function SchemaMgr() {
		return schemaMgr;
	}

	function Debug() {
		return spy.debug;
	}

	function proxyBucketFsm( index, schema ) {
		var stubs = {
			'./indexes.js': index || noOp,
			'./schema.js': schema || noOp,
			'debug': Debug
		};

		return proxyquire( '../src/bucketFsm.js', stubs );
	}


	function createBucket() {
		return api;
	}

	describe( 'when created with name string', function() {
		var bucketFsm, isReady;

		before( function() {

			buildStubs();
			bucketFsm = proxyBucketFsm();

			bucketFsm( 'test', {}, riak, createBucket )
				.on( 'transition', function( data ) {
					isReady = isReady || data.toState === 'ready';
				} );
		} );

		it( 'gets bucket props via api', function() {
			spy.readBucket.withArgs( riak, 'test' ).calledOnce.should.be.true;
		} );

		it( 'creates bucket using setBucket', function() {
			var expectedArgs = { bucket: 'test', props: { allow_mult: true } };
			spy.setBucket.withArgs( expectedArgs ).calledOnce.should.be.true;
		} );

		it( 'ready for operations', function() {
			isReady.should.be.true;
		} );
	} );

	describe( 'when created with name array', function() {
		var bucketFsm, isReady;

		before( function() {

			buildStubs();
			bucketFsm = proxyBucketFsm();

			bucketFsm( [ 'hello', 'test' ], {}, riak, createBucket )
				.on( 'transition', function( data ) {
					isReady = isReady || data.toState === 'ready';
				} );
		} );

		it( 'concatenates name parts with \'_\'', function() {
			spy.readBucket.withArgs( riak, 'hello_test' ).calledOnce.should.be.true;
		} );

	} );


	describe( 'with valid schema', function() {
		var bucketFsm, isReady, options, isIndexAsserted, isSchemaAsserted;

		before( function() {

			buildStubs();
			bucketFsm = proxyBucketFsm( IndexMgr, SchemaMgr );

			options = { schema: 'foo', schemaPath: 'Ima Path!' };

			bucketFsm( 'test', options, riak, createBucket )
				.on( 'transition', function( data ) {
					isIndexAsserted = isIndexAsserted || ( data.action === 'checkingIndex.index.asserted' );
					isSchemaAsserted = isSchemaAsserted || ( data.action === 'checkingSchema.schema.asserted' );
					isReady = isReady || data.toState === 'ready';
				} );

			return soon( 30 );
		} );

		it( 'creates the schema', function() {
			spy.schemaCreate.calledWith( options.schema, options.schemaPath )
				.should.be.true;
		} );

		it( 'asserts the schema was created', function() {
			isSchemaAsserted.should.be.true;
		} );

		it( 'creates the index', function() {
			spy.indexCreate.calledWith( 'test_index', options.schema )
				.should.be.true;
		} );

		it( 'asserts the index was created', function() {
			isIndexAsserted.should.be.true;
		} );

		it( 'gets bucket props via api', function() {
			spy.readBucket.withArgs( riak, 'test' )
				.calledOnce.should.be.true;
		} );

		it( 'creates bucket using setBucket', function() {
			var expectedArgs = { bucket: 'test', props: { allow_mult: true, search_index: 'test_index' } };
			spy.setBucket.withArgs( expectedArgs ).calledOnce.should.be.true;
		} );

		it( 'ready for operations', function() {
			isReady.should.be.true;
		} );
	} );

	describe( 'with invalid schema', function() {
		var bucketFsm, options, isIndexAsserted, isSchemaAsserted;
		var isReady = false;
		before( function() {
			buildStubs( {
				schemaCreate: function() {
					return when.reject( new Error( 'your momma don\'t like this schema' ) );
				}
			} );
			bucketFsm = proxyBucketFsm( IndexMgr, SchemaMgr );

			options = { schema: 'foo', schemaPath: 'Ima Path!' };

			bucketFsm( 'test', options, riak, createBucket )
				.on( 'transition', function( data ) {
					// console.log( data );
					isIndexAsserted = isIndexAsserted || ( data.action === 'checkingIndex.index.asserted' );
					isSchemaAsserted = isSchemaAsserted || ( data.action === 'checkingSchema.schema.asserted' );
					isReady = isReady || data.toState === 'ready';
				} );

			return soon( 40 );
		} );

		it( 'tries to create schema', function() {
			spy.schemaCreate.calledWith( options.schema, options.schemaPath )
				.should.be.true;
		} );
		it( 'is not ready for operations', function() {
			isReady.should.be.false;
		} );
	} );

	describe( 'with missing schema path', function() {
		var bucketFsm, options;

		before( function() {
			buildStubs();
			bucketFsm = proxyBucketFsm( IndexMgr, SchemaMgr );

			options = { schema: 'foo' };

			bucketFsm( 'test', options, riak, createBucket );

		} );

		it( 'does not attempt to create schema', function() {
			sinon.assert.notCalled( spy.schemaCreate );
		} );

		it( 'attempts to create index', function() {
			spy.indexCreate.calledOnce.should.be.true;
		} );
	} );

	describe( 'with valid schema and specified search index', function() {
		var bucketFsm, options, isIndexAsserted, isSchemaAsserted;
		var isReady = false;
		before( function() {

			buildStubs();
			bucketFsm = proxyBucketFsm( IndexMgr, SchemaMgr );

			options = { schema: 'foo', schemaPath: 'Ima Path!', search_index: 'myIndex' };

			bucketFsm( 'test', options, riak, createBucket )
				.on( 'transition', function( data ) {
					isIndexAsserted = isIndexAsserted || ( data.action === 'checkingIndex.index.asserted' );
					isSchemaAsserted = isSchemaAsserted || ( data.action === 'checkingSchema.schema.asserted' );
					isReady = isReady || data.toState === 'ready';
				} );

			return soon( 40 );
		} );
		it( 'creates the index', function() {
			spy.indexCreate.args[ 0 ];
			spy.indexCreate.calledWith( 'myIndex', 'foo' )
				.should.be.true;
		} );

		it( 'asserts the index was created', function() {
			isIndexAsserted.should.be.true;
		} );

	} );

	describe( 'with invalid index', function() {
		var bucketFsm, options, isIndexAsserted;
		var isReady = false;

		before( function() {

			buildStubs( {
				indexCreate: function() {
					return when.reject( new Error( 'Ima not create that index!' ) );
				}
			} );

			bucketFsm = proxyBucketFsm( IndexMgr, SchemaMgr );

			options = { schema: 'foo', schemaPath: 'Ima Path!', search_index: 'myIndex' };

			bucketFsm( 'test', options, riak, createBucket )
				.on( 'transition', function( data ) {
					isIndexAsserted = isIndexAsserted || ( data.action === 'checkingIndex.index.asserted' );
					isReady = isReady || data.toState === 'ready';
				} );
		} );

		it( 'tries to create the index', function() {
			spy.indexCreate.calledWith( 'myIndex', 'foo' )
				.should.be.true;
		} );

		it( 'does not assert the index was created', function() {
			isIndexAsserted.should.be.false;
		} );
		it( 'is not ready for operations', function() {
			isReady.should.be.false;
		} );
	} );

	describe( 'when setBucket fails', function() {
		var bucketFsm, options;
		var isReady = false;

		before( function() {
			buildStubs( {
				setBucket: function() {
					return when.reject( new Error( 'won\'t set the bucket' ) );
				}
			} );

			bucketFsm = proxyBucketFsm( IndexMgr, SchemaMgr );

			options = { };

			bucketFsm( 'test', options, riak, createBucket )
				.on( 'transition', function( data ) {
					isReady = isReady || data.toState === 'ready';
				} );
		} );

		it( 'is not ready for operations', function() {
			isReady.should.be.false;
		} );
	} );

	describe( 'when the bucket and propertes already exists', function() {
		var bucketFsm, isReady;

		before( function() {

			buildStubs( {
				readBucket: function() {
					return when.resolve( { allow_mult: true, test_index: 'index' } );
				}
			} );
			bucketFsm = proxyBucketFsm();

			bucketFsm( 'test', { }, riak, createBucket )
				.on( 'transition', function( data ) {
					isReady = isReady || data.toState === 'ready';
				} );
		} );

		it( 'reads bucket props', function() {
			spy.readBucket.withArgs( riak, 'test' ).calledOnce.should.be.true;
		} );

		it( 'does not set bucket properties', function() {
			sinon.assert.notCalled( spy.setBucket );
		} );
		it( 'asserts the bucket was created', function() {} );

		it( 'is ready for operations', function() {
			isReady.should.be.true;
		} );
	} );

	describe( 'when readBucket returns an error', function() {
		var bucketFsm;
		var isReady = false;

		before( function() {

			buildStubs( {
				readBucket: function() {
					return when.reject( new Error( 'yo, get your own bucket' ) );
				},
				debug: sinon.spy()
			} );
			bucketFsm = proxyBucketFsm();

			bucketFsm( 'test', { }, riak, createBucket )
				.on( 'transition', function( data ) {
					isReady = isReady || data.toState === 'ready';
				} );
		} );
		it( 'does not set bucket properties', function() {
			sinon.assert.notCalled( spy.setBucket );
		} );
		it( 'is not ready for operations', function() {
			isReady.should.be.false;
		} );
		it( 'logs expected debug message', function() {
			spy.debug.withArgs( 'No schema specified for bucket %s, skipping to create bucket', 'test' )
				.calledOnce.should.be.true;
		} );
	} );

	describe( 'when the bucket is ready', function() {
		var bucketFsm, bucket;
		var isReady = false;

		before( function() {

			buildStubs();
			bucketFsm = proxyBucketFsm();

			bucket = bucketFsm( 'test', { }, riak, createBucket );
			bucket
				.on( 'transition', function( data ) {
					isReady = isReady || data.toState === 'ready';
					if ( isReady ) {
						bucket.put( { id: 1, name: 'bob' } );
					}
				} );
		} );

		it( 'operations are performed immediately ', function() {
			spy.put.withArgs( { id: 1, name: 'bob' } ).calledOnce.should.be.true;
		} );
	} );

	describe( 'when ops are requested', function() {
		describe( 'while checking schema', function() {

			var bucketFsm, bucket;

			before( function() {

				buildStubs( {
					schemaCreate: function() {
						return soon( 50, function() {
							return when.resolve();
						} );
					}
				} );
				bucketFsm = proxyBucketFsm( IndexMgr, SchemaMgr );

				bucket = bucketFsm( 'test', { schema: 'foo', schemaPath: 'path' }, riak, createBucket );
				bucket.put( { id: 1, name: 'bob' } );
				return soon( 100 );
			} );

			it( 'performs the operation when ready', function() {
				spy.put.args[ 0 ];
				spy.put.withArgs( { id: 1, name: 'bob' } ).calledOnce.should.be.true;
			} );
		} );

		describe( 'while checking index', function() {

			var bucketFsm, bucket;

			before( function() {

				buildStubs( {
					indexCreate: function() {
						return soon( 50, function() {
							return when.resolve();
						} );
					}
				} );
				bucketFsm = proxyBucketFsm( IndexMgr, SchemaMgr );

				bucket = bucketFsm( 'test', { schema: 'foo', schemaPath: 'path' }, riak, createBucket );
				bucket.on( 'transition', function( data ) {
					if ( data.toState === 'checkingIndex' ) {
						bucket.put( { id: 1, name: 'bob' } );
					}
				} );
				return soon( 100 );
			} );

			it( 'performs the operation when ready', function() {
				spy.put.withArgs( { id: 1, name: 'bob' } ).calledOnce.should.be.true;
			} );
		} );

		describe( 'while creating bucket', function() {

			var bucketFsm, bucket;

			before( function() {

				buildStubs( {
					readBucket: function() {
						return soon( 50, function() {
							return when.resolve( {} );
						} );
					}
				} );
				bucketFsm = proxyBucketFsm( IndexMgr, SchemaMgr );

				bucket = bucketFsm( 'test', { schema: 'foo', schemaPath: 'path' }, riak, createBucket );
				bucket.on( 'transition', function( data ) {
					if ( data.toState === 'creating' ) {
						bucket.put( { id: 1, name: 'bob' } );
					}
				} );
				return soon( 100 );
			} );

			it( 'performs the operation when ready', function() {
				spy.put.withArgs( { id: 1, name: 'bob' } ).calledOnce.should.be.true;
			} );
		} );
	} );
} );
