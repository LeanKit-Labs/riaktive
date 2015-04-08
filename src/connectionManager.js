var machina = require( 'machina' );
var monologue = require( 'monologue.js' );
var log = require( './log' )( 'connection' );

function connectionManager( index, config, factory, limit, wait ) {
	var Machine = machina.Fsm.extend( {
		initialize: function() {
			this.id = index;
			this.config = config;
			this.consecutiveFailures = 0;
		},
		_attachHandlers: function() {
			if ( this.connection && this.connection.client ) {
				this.connection.client.on( 'end', this._onEnd.bind( this ) );
				this.connection.client.on( 'error', this._onError.bind( this ) );
			}
		},
		_detachHandlers: function() {
			if ( this.connection && this.connection.client ) {
				this.connection.client.removeListener( 'end', this._onEnd );
				this.connection.client.removeListener( 'error', this._onError );
			}
		},
		_onConnection: function( connection ) {
			log.info( 'Connection established to %s:%s', config.host, config.port );
			this.connection = connection;
			this._attachHandlers();
			this.transition( 'connected' );
		},
		_onError: function( err ) {
			log.error( 'Connection error on %s:%s - %s', config.host, config.port, ( err.stack ? err.stack : err ) );
			this._detachHandlers();
			this.handle( 'connection.error', err );
		},
		_onEnd: function() {
			log.warn( 'Connection closed on %s:%s', config.host, config.port );
			this._detachHandlers();
			this.handle( 'connection.end' );
		},
		close: function() {
			this._detachHandlers();
			this.transition( 'closed' );
		},
		connect: function() {
			this.consecutiveFailures = 0;
			this.transition( 'connecting' );
		},
		makeRequest: function( options, callback ) {
			this.connection.makeRequest( options, callback );
		},
		states: {
			connecting: {
				_onEnter: function() {
					factory( config )
						.then( this._onConnection.bind( this ) )
						.then( null, this._onError.bind( this ) );
				},
				'connection.end': function() {
					this.transition( 'disconnected' );
				},
				'connection.error': function() {
					this.transition( 'disconnected' );
				}
			},
			connected: {
				_onEnter: function() {
					this.emit( 'connected', this );
					this.consecutiveFailures = 0;
				},
				'connection.end': function() {
					this.transition( 'disconnected' );
				},
				'connection.error': function() {
					this.transition( 'disconnected' );
				}
			},
			disconnected: {
				_onEnter: function() {
					this.emit( 'disconnected', this );
					this.consecutiveFailures++;
					if ( this.consecutiveFailures <= ( limit || 5 ) ) {
						log.info( 'Will attempt to reconnect to %s:%s after %d ms', config.host, config.port, ( wait || 5000 ) );
						this.timeout = setTimeout( function() {
							if ( this.state !== 'shutdown' && this.state !== 'closed' ) {
								this.transition( 'connecting' );
							}
						}.bind( this ), wait || 5000 );
					} else {
						this.transition( 'shutdown' );
					}
				}
			},
			shutdown: {
				_onEnter: function() {
					if ( this.timeout ) {
						clearTimeout( this.timeout );
					}
					log.debug( 'entering shut down' );
					this.emit( 'shutdown', this );
				}
			},
			closed: {
				_onEnter: function() {
					log.debug( 'Closing connection to %s:%s.', config.host, config.port );
					if ( this.timeout ) {
						clearTimeout( this.timeout );
					}
					this.emit( 'closed', this );
				}
			}
		}
	} );
	monologue.mixInto( Machine );
	return new Machine();
}

module.exports = connectionManager;
