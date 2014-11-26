var Q = require('q'),
    _ = require('lodash');

// Constants
var VALID_DB_TYPES = ['rethinkdb'];

function ThrowawayDB(options) {
    var self = this;
    self.options = options || {db: 'rethinkdb'};

    // Ensure db has been specified
    if (!_.has(self.options, 'db') ||
        _.isUndefined(self.options.db) ||
        !_.contains(VALID_DB_TYPES, self.options.db)) {
        throw new Error("Invalid/missing database type");
    }

}

/**
 * Load data for the database from a file
 *
 * @param {string} type - Type of data file (ex. json, binary)
 * @param {string} filePath - Path to the file that contains the data to be loaded
 * @returns ThrowawayDB instance
 */
ThrowawayDB.prototype.loadDataFromFile = function(type, filePath) {

    return this;
};

/**
 * Start the database process for the database
 * @returns ThrowawayDB instance
 */
ThrowawayDB.prototype.start = function() {
    return this;
};

/**
 * Create database
 *
 * @param {string} dbName - Database to create
 * @returns ThrowawayDB instance
 */
ThrowawayDB.prototype.createDB = function(dbName) {
    return this;
};

/**
 * Delete database
 *
 * @param {string} dbName - Database to delete
 * @returns ThrowawayDB instance
 */
ThrowawayDB.prototype.deleteDB = function(dbName) {
    return this;
};


exports.ThrowawayDB = ThrowawayDB;
