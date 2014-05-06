var _ = require( 'lodash' );

module.exports = function( presetNodeId ) {

	var VersionVector = function( vector ) {
		this.node = presetNodeId;
		this.versions = {};
		if( presetNodeId ) {
			this.versions[ presetNodeId ] = 1;
		}
		this.parseString( vector );
	};

	VersionVector.prototype.increment = function() {
		if( this.versions[ this.node ] ) {
			this.versions[ this.node ] ++;
		} else {
			this.versions[ this.node ] = 1;
		}
	};

	VersionVector.prototype.compare = function( vector ) {
		var keys = _.keys( this.versions ),
			vectorKeys = _.keys( vector.versions ),
			allKeys = _.union( keys, vectorKeys ),
			local = 0,
			other = 0;
		_.each( allKeys, function( k ) {
			var diff = ( this.versions[ k ] || 0 ) - ( vector.versions[ k ] || 0 );
			if( diff > 0 ) {
				local ++;
			} else if ( diff < 0 ) {
				other ++;
			}
		}.bind( this ) );
		if( local == 0 && other == 0 ) {
			return 'equal';
		} else if ( local > 0 && other > 0 ) {
			return 'diverged';
		} else if ( local > 0 ) {
			return 'greater';
		} else {
			return 'lesser';
		}
	};

	VersionVector.prototype.merge = function( vector ) {
		var keys = _.keys( this.versions ),
			vectorKeys = _.keys( vector.versions ),
			allKeys = _.union( keys, vectorKeys );
		_.each( allKeys, function( key ) {
			var x = this.versions[ key ] || 0,
				y = vector.versions[ key ] || 0;
			this.versions[ key ] = x >= y ? x : y;
		}.bind( this ) );
		this.versions[ this.node ] ++;
	};

	VersionVector.prototype.parseString = function( str ) {
		_.each( str.split( ';' ), function( pair ) {
			var parts = pair.split( ':' );
			this.versions[ parts[ 0 ] ] = parts[ 1 ];
		}.bind( this ) );
	};

	VersionVector.prototype.toString = function() {
		var vals = _.map( this.versions, function( val, key ) {
			return key + ':' + val;
		} );
		vals.sort();
		return vals.join( ';' );
	};
	
	return VersionVector;
};