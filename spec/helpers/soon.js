function soon( ms, promiseFn ) {
	// return a promise, then do "promiseFn" (if it's there) after waiting ms
	return when.promise( function( resolve, reject ) {
		setTimeout( function() {
			if ( promiseFn ) {
				promiseFn().then( resolve, reject );
			} else {
				resolve();
			}
		}, ms );
	} );
}

module.exports = soon;
