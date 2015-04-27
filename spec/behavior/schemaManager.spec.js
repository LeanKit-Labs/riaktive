require( '../setup' );
function noOp() {
}

var stubs;

function proxySchemaManager( riak ) {
	var fsStub = {
		readFileSync: noOp
	};

	stubs = {
		'fs': fsStub
	};

	sinon.stub( stubs.fs, 'readFileSync', function() {
		return 'boo';
	} );
	var SchemaMgr = proxyquire( '../src/schemaManager.js', stubs );
	return new SchemaMgr( riak );
}

describe( 'schemaManager', function() {
	var schemaMgr;

	describe( 'when creating a new schema manager', function() {
		before( function() {
			schemaMgr = proxySchemaManager( noOp );
		} );
		it( 'has an empty set of schemas', function() {
			schemaMgr.schemas.should.eql( {} );
		} );

		it( 'it has a create function', function() {
			_.isFunction( schemaMgr.create ).should.be.true;
		} );
	} );

	describe( 'when creating a schema', function() {
		var schemaMgr;

		describe( 'and it is new', function() {
			var riak;
			before( function() {
				riak = {
					yzGetSchema: noOp,
					yzPutSchema: noOp
				};
				sinon.stub( riak, 'yzGetSchema', function() {
					return when( {} );
				} );
				sinon.stub( riak, 'yzPutSchema', function() {
					return when();
				} );

				schemaMgr = proxySchemaManager( riak );

				return schemaMgr.create( 'schema1', 'Path' );
			} );

			it( 'reads schema file', function() {
				stubs.fs.readFileSync.withArgs( 'Path', 'utf8' )
					.calledOnce.should.be.true;
			} );
			it( 'gets existing schema content', function() {
				riak.yzGetSchema.withArgs( { name: 'schema1' } )
					.calledOnce.should.be.true;
			} );

			it( 'creates a new schema', function() {
				riak.yzPutSchema.withArgs( { schema: { name: 'schema1', content: 'boo' } } )
					.calledOnce.should.be.true;
			} );
		} );

		describe( 'and the schema is already cached', function() {
			var riak;
			before( function() {
				riak = {
					yzGetSchema: noOp,
					yzPutSchema: noOp
				};
				sinon.stub( riak, 'yzGetSchema', function() {
					return when( {} );
				} );
				sinon.stub( riak, 'yzPutSchema', function() {
					return when();
				} );

				schemaMgr = proxySchemaManager( riak );

				return schemaMgr.create( 'schema1', 'Path' )
					.then( function() {
						return schemaMgr.create( 'schema1', 'Path' );
					} );
			} );
			it( 'reads schema file', function() {
				stubs.fs.readFileSync.withArgs( 'Path', 'utf8' )
					.calledTwice.should.be.true;
			} );
			it( 'does not get existing schema content', function() {
				riak.yzGetSchema.withArgs( { name: 'schema1' } )
					.calledOnce.should.be.true;
			} );
			it( 'does not create a new schema', function() {
				riak.yzPutSchema.withArgs( { schema: { name: 'schema1', content: 'boo' } } )
					.calledOnce.should.be.true;
			} );
		} );
		describe( 'and it exists but is not cached', function() {
			var riak;
			before( function() {
				riak = {
					yzGetSchema: noOp,
					yzPutSchema: noOp
				};
				sinon.stub( riak, 'yzGetSchema', function() {
					return when( { schema: { content: 'boo' } } );
				} );
				sinon.stub( riak, 'yzPutSchema', function() {
					return when();
				} );

				schemaMgr = proxySchemaManager( riak );

				return schemaMgr.create( 'schema1', 'Path' );
			} );
			it( 'reads schema file', function() {
				stubs.fs.readFileSync.withArgs( 'Path', 'utf8' )
					.calledOnce.should.be.true;
			} );
			it( 'gets existing schema content', function() {
				riak.yzGetSchema.withArgs( { name: 'schema1' } )
					.calledOnce.should.be.true;
			} );
			it( 'does not create a new schema', function() {
				riak.yzPutSchema.withArgs( { schema: { name: 'schema1', content: 'boo' } } )
					.calledOnce.should.be.false;
			} );
		} );
	} );
} );
