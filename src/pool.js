var _ = require( 'lodash' );
var connectionManager = require( './connectionManager' );
var log = require( './log' )( 'pool' );

var defaultNode = {
	host: 'localhost',
	port: 8087,
	http: 8098,
	connectTimeout: 2000,
	connections: 5
};

function createPool( config, factory ) {

	var nodes = config.nodes;
	var managers = [];
	var nodeConnections = new Array( config.nodes.length );
	var waiting = [];
	var shutdown = 0;
	var closed = false;

	log.debug( 'Setting up connection pool for %j', config );

	function acquire( cb ) {
		var connection = _.find( nodeConnections, function( list ) {
			return _.first( list );
		} );
		if ( connection && connection.length ) {
			cb( null, connection.shift() );
		} else {
			log.debug( 'Enqueueing connection acquisition. %d in the queue.', ( waiting.length + 1 ) );
			waiting.push( cb );
		}
	}

	function initialize() {
		if ( !closed ) {
			for (var i = 0; i < nodeConnections.length; i++) {
				var count = nodes[ i ].connections || 1;
				for (var j = 0; j < count; j++) {
					newManager( i );
					nodeConnections[ i ] = [];
				}
			}
		} else {
			log.error( 'Initialization of connection pool was cancelled because the application has closed it. Call restart to re-establish the pool.' );
		}
	}

	function onConnection( connection ) {
		if ( connection.state === 'connected' ) {
			if ( waiting.length ) {
				log.debug( 'Granting acquisition request from queue. %d remaining in the queue.', ( waiting.length - 1 ) );
				waiting.shift()( null, connection );
			} else {
				log.debug( 'Saving connection to %s:%s for later', connection.config.host, connection.config.port );
				nodeConnections[ connection.id ].push( connection );
				// _.foldl( nodeConnections, function( x, y ) { return x + y.length; }, 0 );
			}
		}
	}

	function onDisconnection( connection ) {
		nodeConnections[ connection.id ] = _.without( nodeConnections[ connection.id ], connection );
	}

	function onShutdown( connection ) {
		log.warn( 'Shutting down connection to node %s:%s due to too many consecutive failed connection attempts.', connection.config.host, connection.config.port );
		connection.off( 'connected' );
		connection.off( 'disconnected' );
		connection.off( 'shutdown' );
		connection.off( 'closed' );
		if ( config.failed ) {
			config.failed();
		}
		managers = _.without( managers, connection );
		if ( ++shutdown === nodes.length && !closed ) {
			log.error( 'All defined nodes have shutdown. Connection pool will require a reset to continue attempting connections.' );
			while ( waiting.length ) {
				waiting.pop()( new Error( 'All nodes were unreachable.' ) );
			}
		}
	}

	function onClose( connection ) {
		log.debug( 'Closed connection to node %s:%s.', connection.config.host, connection.config.port );
		connection.off( 'connected' );
		connection.off( 'disconnected' );
		connection.off( 'shutdown' );
		connection.off( 'closed' );
		managers = _.without( managers, connection );
		if ( managers.length === 0 ) {
			log.warn( 'All connections in the pool have closed' );
		}
	}

	function newManager( i ) { // jshint ignore: line
		var manager = connectionManager( i, nodes[ i ], factory, config.limit || config.retries, config.wait );
		managers.push( manager );
		manager.on( 'connected', onConnection );
		manager.on( 'disconnected', onDisconnection );
		manager.on( 'shutdown', onShutdown );
		manager.on( 'closed', onClose );
		manager.connect();
	}

	function reset() {
		closed = false;
		initialize();
	}

	initialize();
	return {
		acquire: acquire,
		close: function() {
			closed = true;
			_.each( managers, function( manager ) {
				manager.close();
			} );
		},
		addNode: function( node ) {
			nodes.push( _.merge( {}, defaultNode, node ) );
			nodeConnections = new Array( nodes.length );
		},
		getNode: function() {
			var matches = _.find( managers, function( x ) {
				return x.state === 'connected' || x.state === 'connecting';
			} );
			return matches.length ? matches[ 0 ].config : ( matches ? matches.config : undefined );
		},
		release: function( connection ) {
			log.debug( 'Acquisition for connection to %s:%s has been released.', connection.config.host, connection.config.port );
			onConnection( connection );
		},
		restart: reset
	};
}

module.exports = createPool;
