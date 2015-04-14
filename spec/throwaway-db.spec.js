var mocha = require('mocha'),
    should = require('should'),
    t = require('../lib/throwaway-db');

describe('throwaway-db rethinkdb plugin', function() {

    it('should create an object with no options', function() {
        var tdb =  new t.ThrowawayDB();
        tdb.should.be.ok;
    });
    
    it('should error on an invalid database type', function() {
        (function() {
            var tdb = new t.ThrowawayDB({db: 'nopedb'});
        }).should.throw(/Invalid\/missing database type/);
    });

});
