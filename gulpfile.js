var gulp = require( 'gulp' );
var bg = require( 'biggulp' )( gulp );

gulp.task( 'coverage', bg.withCoverage() );

gulp.task( 'coverage-watch', function() {
	bg.watch( [ 'coverage' ] );
} );

gulp.task( 'show-coverage', bg.showCoverage() );

gulp.task( 'run-tests', function() {
	bg.test();
} );

gulp.task( 'test', [ 'run-tests' ], function() {
	bg.watch( [ 'run-tests' ] );
} );

gulp.task( 'default', [ 'coverage', 'coverage-watch' ], function() {} );
