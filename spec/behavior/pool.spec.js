require( "../setup" );
var when = require( "when" );
var connectionManager = require( "../../src/connectionManager" );
var createPool = require( "../../src/pool" );

describe( "Connectivity", function() {
	var connectionMock = function() {
		return {
			client: {
				handlers: {},
				on: function( ev, handle ) {
					this.handlers[ ev ] = handle;
				},
				removeListener: function( ev ) {
					delete this.handlers[ ev ];
				}
			},
			raise: function( ev ) {
				this.client.handlers[ ev ].apply( undefined, Array.prototype.slice.call( arguments, 1 ) );
			}
		};
	};

	describe( "Connection Manager", function() {
		describe( "with no connectivity", function() {
			var manager;

			before( function( done ) {
				manager = connectionManager( 1, {}, function() {
					return when.reject( new Error( "boo" ) );
				}, 5, 10 );
				manager.on( "shutdown", function() {
					done();
				} );
				manager.connect();
			} );

			it( "should have shutdown after limit", function() {
				manager.consecutiveFailures.should.equal( 6 );
			} );

			after( function() {
				manager.close();
			} );
		} );

		describe( "with connectivity", function() {
			var manager;
			var failures = 0;
			var tries = 0;
			before( function( done ) {
				var factory = function() {
					if ( ++tries > 2 ) {
						return when.resolve( connectionMock() );
					} else {
						return when.reject( new Error( "boo" ) );
					}
				};
				manager = connectionManager( 1, {}, factory, 5, 10 );
				manager.on( "connected", function() {
					done();
				} );
				manager.on( "disconnected", function() {
					failures++;
				} );
				manager.connect();
			} );

			it( "should resolve to a connected state", function() {
				manager.state.should.equal( "connected" );
			} );

			it( "should retry until a connection succeeds", function() {
				failures.should.equal( 2 );
			} );

			it( "should have reset consecutiveFailures", function() {
				manager.consecutiveFailures.should.equal( 0 );
			} );

			after( function() {
				manager.close();
			} );
		} );

		describe( "with lost connection", function() {
			var manager;
			var failures = 0;
			var connections = 0;
			var connection = connectionMock();
			before( function( done ) {
				var factory = function() {
					return when.resolve( connection );
				};
				manager = connectionManager( 1, {}, factory, 5, 10 );
				manager.on( "connected", function() {
					if ( ++connections >= 2 ) {
						done();
					} else {
						connection.raise( "end" );
					}
				} );
				manager.on( "disconnected", function() {
					failures++;
				} );
				manager.connect();
			} );

			it( "should have reconnected", function() {
				connections.should.equal( 2 );
			} );

			it( "should resolve to a connected state", function() {
				manager.state.should.equal( "connected" );
			} );

			it( "should have disconnected", function() {
				failures.should.equal( 1 );
			} );

			it( "should have reset consecutiveFailures", function() {
				manager.consecutiveFailures.should.equal( 0 );
			} );

			after( function() {
				manager.close();
			} );
		} );
	} );

	describe( "Connection Pool", function() {
		describe( "with no connectivity", function() {
			var pool;
			var error;
			before( function( done ) {
				pool = createPool( { nodes: [ { host: "herp" }, { host: "dederp" } ], wait: 20, limit: 2 }, function() {
					return when.reject( new Error( "fail" ) );
				} );
				pool.acquire( function( err ) {
					error = err;
					pool.close();
					done();
				} );
			} );

			it( "should reject acquisition with error", function() {
				error.toString().should.equal( "Error: All nodes were unreachable." );
			} );
		} );

		describe( "with one available connection", function() {
			var pool;
			var connection;
			var lease1;
			var lease2;
			var order = [];
			before( function( done ) {
				connection = connectionMock();
				pool = createPool( { nodes: [ {} ], wait: 20, limit: 2 }, function() {
					return when.resolve( connection );
				} );
				pool.acquire( function( err, connection ) {
					lease1 = connection;
					order.push( 1 );
					pool.release( connection );
				} );
				pool.acquire( function( err, connection ) {
					order.push( 2 );
					lease2 = connection;
					done();
				} );
			} );

			it( "should fulfill both acquisition requests", function() {
				lease1.connection.should.equal( connection );
				lease2.connection.should.equal( connection );
			} );

			it( "should resolve acquisition requests in order", function() {
				order.should.eql( [ 1, 2 ] );
			} );

			after( function() {
				pool.close();
			} );
		} );
	} );
} );
