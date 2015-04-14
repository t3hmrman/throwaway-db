var os = require('os'),
    temp = require('temp'),
    fs = require('fs'),
    portfinder = require('portfinder'),
    child_process = require('child_process'),
    Q = require('q'),
    _ = require('lodash'),
    rdb = require('rethinkdb');

// Track temporary files & cleanup at exit by default, disable if needed
temp.track();

// Constants
var VALID_DB_TYPES = ['rethinkdb'];

// Promise-ify some interfaces
var pGetTempDirectory = Q.denodeify(temp.mkdir);
var pGetPort = Q.denodeify(portfinder.getPort);
var pExec = Q.denodeify(child_process.exec);

function ThrowawayDB(options) {
    var self = this;
    self.options = options || {db: 'rethinkdb'};
    self.state = {};

    // Ensure db has been specified
    if (!_.has(self.options, 'db') ||
        _.isUndefined(self.options.db) ||
        !_.contains(VALID_DB_TYPES, self.options.db)) {
        throw new Error("Invalid/missing database type");
    }

}

/**
 * Start the database process for the database
 * @returns ThrowawayDB instance
 */
ThrowawayDB.prototype.start = function() {
    this.currentOp = pGetTempDirectory('throwaway_db_' + process.env.USER + '_' + process.pid)
    .then(liftResultToObject('path'))
    .then(this.setupTempDirectory)
    .then(this.getPorts)
    .then(this.prepareDatabase)
    .then(startRethinkProcess)
    .then(function(dbInfo) {
        console.log("INFO: Successfully started DB! dbInfo:", dbInfo);
    });
    return this;
};

/**
 * Start the database process
 */
ThrowawayDB.prototype.prepareDatabase = function(dbInfo) {
    var deferred = Q.defer();
    var dataPath = dbInfo.path + "/rethinkdb_data";

    // Start the database process
    var createProc = child_process.spawn("rethinkdb", ["create", "-d", dataPath], { cwd: dbInfo.path });

    // Attach error handler and detect successful rethinkdb create
    createProc.on('error', function(err) { console.log("ERROR preparing RethinkDB temporary instance:", err); });
    createProc.stderr.setEncoding('utf8');
    createProc.stderr.on('data', function(data) {
        // Listen for created directory log message, then spin off rethinkdb serve
        if (_.contains(data, "Created directory '" + dataPath + "'")) {
            console.log("INFO: Initialized RethinkDB database");
            console.log("INFO: RethinkDB data path: [" + dataPath + "]");
            dbInfo.dataPath = dataPath;

            createProc.kill();
            deferred.resolve(dbInfo);
        }
    });

    return deferred.promise;
};

/**
 * Helper function to start the rethinkdb server process in the background
 *
 * @param {object} dbInfo - Object with information about database
 */
function startRethinkProcess(dbInfo) {
    var deferred = Q.defer();
    var rdb_args = ['serve',
                    '--driver-port', dbInfo.driverPort,
                    '--cluster-port', dbInfo.clusterPort,
                    '--directory', dbInfo.dataPath,
                    '--no-http-admin'];

    console.log("INFO: About to start rethinkdb child process");
    console.log("CMD: rethinkdb " + rdb_args.join(" "));

    // Start the database process process
    var rdb_process = child_process.spawn('rethinkdb', rdb_args, { cwd: dbInfo.path });

    // Ensure db process is killed upon exit of this process
    process.on('exit', function() {  rdb_process.kill(); });
    process.on('uncaughtException', function() { rdb_process.kill(); });
    process.on('SIGINT', function() { rdb_process.kill(); });
    process.on('SIGTERM', function() { rdb_process.kill(); });

    // Alert the user if DB process quits/crashes
    rdb_process.on('close', function (code) {
        console.log('WARNING: Test RethinkDB child process exited with code ' + code);
    });

    // Watch rethinkdb process output until it says that rethinkdb is ready for connections
    rdb_process.stderr.setEncoding('utf8');
    rdb_process.stderr.on('data', function (data) {
        // Have to detect "Loading data" message rather than "Server ready", due to rethinkdb giving up outputing
        if (_.contains(data, "Loading data from directory")) {
            // Connect to the instance and pass a connection
            var rdb_config = {host: 'localhost', port: dbInfo.driverPort};
            console.log("rdb_config:", rdb_config);

            // Connect to the database & pass along instance and process information
            rdb.connect(rdb_config, function(err,conn) {
                console.log("error?", err);
                if (err) { throw err; }
                dbInfo.conn = conn;
                dbInfo.process = {
                    dir: dbInfo.path,
                    cluster_port: dbInfo.clusterPort,
                    driver_port: dbInfo.driverPort,
                    process: rdb_process
                };
                console.log("about to resolve deferred!:", dbInfo);
                deferred.resolve(dbInfo);
            }); // /rdb.connect
        } // /if
    });// /stderr handler

    return deferred.promise;
};

/**
 * Helper function to wrap information into an object for the db info in a promise
 *
 * @param v - the value to assign to the key in the resulting dbInfo object
 * @param k - the key to use to insert into the resulting dbInfo object
 * @returns a promise
 */
function liftResultToObject(objectKey) {
    return _.partialRight(function(result) {
        return Q.fcall(function() {
            var obj = {};
            obj[objectKey] = result;
            return obj;
        });
    }, objectKey);
};

/**
 * Get the ports required to run the database
 *
 * @param {object} dbInfo - object containing various database information
 */
ThrowawayDB.prototype.getPorts = function(dbInfo) {
    // Get call get port twice, update the received dbInfo object, and return it
    return pGetPort()
    // Save the first (cluster) port
        .then(function(port) {
            return Q.fcall(function() { dbInfo.clusterPort = port; });
        })
    // Increment the base port of portfinder
        .then(function() {
            portfinder.basePort += 1;
            return pGetPort();
        })
    // Save the second (driver) port
        .then(function(port) {
            return Q.fcall(function() { dbInfo.driverPort = port;  });
        })
        .then(function() { return Q.fcall(function() { return dbInfo; }); });
};

/**
 * Get a temporary directory
 *
 * @param {object} dbInfo - object containing information about the database
 * @returns A promise that resolves immediately with the path after doing setup
 */
ThrowawayDB.prototype.setupTempDirectory = function(dbInfo) {
    return Q.fcall(function() {
        console.log("Setting up temp directory @ [" + dbInfo.path + "]");
        return dbInfo;
    });
};

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
