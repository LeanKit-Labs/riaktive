var _ = require( 'lodash' );
var postal = require( 'postal' );
var logFn = require( 'whistlepunk' );
var log = configure( {} );
var logs = {};

function configure( config ) {
	var envDebug = !!process.env.DEBUG;
	if ( envDebug ) {
		return logFn( postal, { adapters: { debug: { level: 5 } } } );
	} else {
		return logFn( postal, config );
	}
}

function proxyLog( logName ) {
	var lg = logs[ logName ];
	return {
		debug: lg.debug.bind( lg ),
		info: lg.info.bind( lg ),
		warn: lg.warn.bind( lg ),
		error: lg.error.bind( lg )
	};
}

module.exports = function( config ) {
	if ( !_.isString( config ) ) {
		log = configure( config || {} );
		logs.bucket = log( 'riaktive:bucket' );
		logs.connection = log( 'riaktive:connectionManager' );
		logs.indexes = log( 'riaktive:index' );
		logs.pool = log( 'riaktive:pool' );
		logs.api = log( 'riaktive:api' );
		logs.schema = log( 'riaktive:schema' );
		logs.search = log( 'riaktive:search' );
	}
	return _.isString( config ) ? proxyLog( config ) : proxyLog;
};
