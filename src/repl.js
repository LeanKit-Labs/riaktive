var replFn = require( 'repl' );
var riaktive = require( './index' );
var _ = require( 'lodash' );
var util = require( 'util' );
var stdout = process.stdout;
var line = '-------------------------------------------------------------------------------';

var connectHelp = 'Type "connect [host] [port]" to get started!';
var bucketHelp = 'Type "bucket [name]" to create a bucket. Buckets are added to the shell by name.';

function logBox() {
	var args = Array.prototype.slice.call( arguments );
	var pattern = args[ 0 ];
	var opts = args.slice( 1 );
	if ( _.isArray( pattern ) ) {
		pattern = pattern.join( '\n' );
	}
	log( [ line, pattern, line ], opts );
}

function log() {
	var args = Array.prototype.slice.call( arguments );
	var pattern = args[ 0 ];
	var opts = args.slice( 1 );
	if ( _.isArray( pattern ) ) {
		pattern = pattern.join( '\n' );
	}
	pattern = util.format.apply( undefined, [ pattern ].concat( opts || [] ) );
	stdout.write( pattern + '\n' );
}

repl = replFn.start( {
	prompt: '> ',
	ignoreUndefined: true,
	eval: customEval
} );

var commands = {
	bucket: createBucket,
	connect: connect,
	help: help,
	exit: exit
};


function connect( host, port ) {
	var node = { failed: handleFailed };
	if ( host ) {
		node.host = host;
	}
	if ( port ) {
		node.port = port;
	}
	log( 'Trying to connect to %s:%d ...', host || 'localhost', port || 8097 );
	this.riak = riaktive.connect( node );
	this.riak.ping().then( handleConnection );
}

function createBucket( name, opts ) {
	var bucket = this.riak.bucket( name, opts );
	this[ name ] = bucket;
}

function customEval( cmd, context, filename, callback ) {
	var clean = cmd.replace( '\n', '' ).slice( 0 );
	var part = clean.split( ' ' );
	var call = commands[ part[ 0 ] ];
	try {
		var result;
		if ( _.isFunction( call ) ) {
			result = call.apply( undefined, part.slice( 1 ) );
		} else {
			result = eval( clean, context ); // jshint ignore:line
		}
		if ( _.isObject( result ) ) {
			callback( JSON.stringify( result ) );
		} else {
			callback( result );
		}
	} catch ( e ) {
		callback( 'error ' + e.stack + ' : ' + cmd );
	}
}

function exit() {
	process.exit();
}

function handleConnection() {
	log( 'Connected!' );
}

function handleFailed( e ) {
	log( 'Cannot connect :(', e.message );
}

function help() {
	logBox( !this.riak ? connectHelp : bucketHelp );
}

connect();
