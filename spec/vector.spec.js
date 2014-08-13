var should = require( 'should' ); // jshint ignore:line
var Vector = require( '../src/vector.js' )( 'nodeid' );

describe( 'when version vector has a single node', function() {
	
	describe( 'when comparing compatible but equal vectors', function() {
		var v1 = new Vector( 'a:1' ),
			v2 = new Vector( 'a:1' );

		it( 'should be equal', function() {
			v1.compare( v2 ).should.equal( 'equal' );
		} );
	} );

	describe( 'when comparing compatible but unequal vectors', function() {
		var v1 = new Vector( 'a:1' ),
			v2 = new Vector( 'a:2' );

		it( 'should be lesser', function() {
			v1.compare( v2 ).should.equal( 'lesser' );
		} );
	} );
} );

describe( 'when incrementing vector', function() {
	
	describe( 'when comparing compatible but equal vectors', function() {
		var v1 = new Vector( 'a:1;b:2' ),
			v2 = new Vector( 'nodeid:1;b:1' );

		v1.increment();
		v2.increment();

		it( 'should increment missing node', function() {
			v1.toString().should.equal( 'a:1;b:2;nodeid:2' );
			v1.versions.nodeid.should.equal( 2 );
		} );

		it( 'should increment existing node', function() {
			v2.toString().should.equal( 'b:1;nodeid:2' );
			v2.versions.nodeid.should.equal( 2 );
		} );
	} );

	describe( 'when comparing compatible but unequal vectors', function() {
		var v1 = new Vector( 'a:1' ),
			v2 = new Vector( 'a:2' );

		it( 'should be lesser', function() {
			v1.compare( v2 ).should.equal( 'lesser' );
		} );
	} );
} );

describe( 'when version vector has multiple nodes', function() {
	
	describe( 'when comparing equal vectors', function() {
		var v1 = new Vector( 'a:1;b:2' ),
			v2 = new Vector( 'b:2;a:1' );
		
		it( 'should be equal', function() {
			v1.compare( v2 ).should.equal( 'equal' );
		} );
	} );

	describe( 'when comparing incompatible vectors', function() {
		var v1 = new Vector( 'a:1' ),
			v2 = new Vector( 'b:2' );
		
		it( 'should be diverged', function() {
			v1.compare( v2 ).should.equal( 'diverged' );
		} );
	} );

	describe( 'when comparing equal vectors', function() {
		var v1 = new Vector( 'a:1;b:2' ),
			v2 = new Vector( 'b:2;a:1' );
		
		it( 'should be equal', function() {
			v1.compare( v2 ).should.equal( 'equal' );
		} );
	} );

	describe( 'when comparing a greater vector', function() {
		var v1 = new Vector( 'a:2;b:2' ),
			v2 = new Vector( 'b:2;a:1' );
		
		it( 'should be greater', function() {
			v1.compare( v2 ).should.equal( 'greater' );
		} );
	} );

	describe( 'when comparing a greater vector because of additional nodes', function() {
		var v1 = new Vector( 'a:1;b:2;c:1' ),
			v2 = new Vector( 'b:2;a:1' );
		
		it( 'should be greater', function() {
			v1.compare( v2 ).should.equal( 'greater' );
		} );
	} );
} );

describe( 'when merging vectors', function() {
	
	var v1 = new Vector( 'a:2;b:3' ),
		v2 = new Vector( 'b:3;c:2' );

	v1.merge(v2);
	it( 'should include all nodes and increment local node', function() {
		v1.toString().should.equal( 'a:2;b:3;c:2;nodeid:2' );
	} );
} );