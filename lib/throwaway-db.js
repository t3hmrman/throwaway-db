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
    .then(this.startDBProcess)
    .then(function(dbInfo) {
        console.log("Got to the part after starting the DB!, dbInfo:", dbInfo);
    });
    return this;
};

/**
 * Start the database process
 */
ThrowawayDB.prototype.startDBProcess = function(dbInfo) {
    var deferred = Q.defer();
    var dataPath = dbInfo.path + "/rethinkdb_data";

    console.log("About to run rethinkdb create...");
    setTimeout(function() { deferred.resolve(dbInfo); }, 5000);

    // Start the database process
    if (false) {
    child_process.spawn("rethinkdb create -d " + dataPath, { cwd: dbInfo.path }, function(err,stdout,stderr) {
        if (err) { throw err; }
        console.log("INFO: Initialized rethinkdb database");
        // Start rethinkdb
        console.log("INFO: About to start rethinkdb child process");
        var rdb_args = ['serve',
                        '--driver-port', dbInfo.driverPort,
                        '--cluster-port', dbInfo.clusterPort,
                        '--directory', dataPath,
                        '--no-http-admin'];
        console.log("CMD: rethinkdb " + rdb_args.join(" "));
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
        rdb_process.stderr.on('data', function (data) {
            if (data.toString().match(/Server ready/)) {
                // Connect to the instance and pass a connection
                var rdb_config = {host: 'localhost',
                                  port: dbInfo.driverPort};

                // Connect to the database & pass along instance and process information
                rdb.connect(rdb_config,function(err,conn) {
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
    }); // /rethinkdb create command
}

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
        .then(function(port) {
            return Q.fcall(function() { dbInfo.clusterPort = port; });
        })
        .then(function() { return pGetPort(); })
        .then(function(port) {
            return Q.fcall(function() { dbInfo.clusterPort = port;  });
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
