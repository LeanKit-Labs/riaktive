var util = require( "util" );

function EmptyResultError( bucket, key ) {
	this.name = "EmptyResultError";
	this.message = util.format( "Get \"%s\" from \"%s\" return an empty document!", key, bucket );
}

EmptyResultError.prototype = Object.create( Error.prototype );
EmptyResultError.constructor = EmptyResultError;

function MissingDocumentError( bucket, key ) {
	this.name = "MissingDocumentError";
	this.message = util.format( "Cannot mutate - no document at \"%s\" in \"%s\"", key, bucket );
}

MissingDocumentError.prototype = Object.create( Error.prototype );
MissingDocumentError.constructor = MissingDocumentError;

function MutationFailedError( bucket, key, err ) {
	this.name = "MutationFailedError";
	this.message = util.format( "Mutate for \"%s\" in \"%s\" failed with: %s", key, bucket, err.stack );
}

MutationFailedError.prototype = Object.create( Error.prototype );
MutationFailedError.constructor = MutationFailedError;

function SiblingMutationError( bucket, key ) {
	this.name = "SiblingMutationError";
	this.message = util.format( "Cannot mutate - siblings exist for \"%s\" in \"%s\"", key, bucket );
}

SiblingMutationError.prototype = Object.create( Error.prototype );
SiblingMutationError.constructor = SiblingMutationError;

module.exports = {
	EmptyResult: EmptyResultError,
	MissingDocument: MissingDocumentError,
	MutationFailed: MutationFailedError,
	SiblingMutation: SiblingMutationError
};
