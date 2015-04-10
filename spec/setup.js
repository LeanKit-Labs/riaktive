var chai = require( 'chai' );
chai.use( require( 'chai-as-promised' ) );
global.should = chai.should();
global.expect = chai.expect;
global.sinon = require( 'sinon' );
global.proxyquire = require( 'proxyquire' );
global._ = require( 'lodash' );
global.when = require( 'when' );
global.soon = require( './helpers/soon.js' );
