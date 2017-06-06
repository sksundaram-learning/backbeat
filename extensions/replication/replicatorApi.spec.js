const assert = require('assert');
const async = require('async');

const AWS = require('aws-sdk');
const S3 = AWS.S3;

const replicatorApi = require('./replicatorApi');
const Logger = require('werelogs').Logger;
const logger = new Logger('Backbeat:Replication:test', {
    level: 'debug', dump: 'info'
});
const log = logger.newRequestLogger();

const s3host = '172.17.0.2';
const testBucket = 'replicator-test-bucket';

//FIXME: use getConfig from S3 aws-node-sdk test utils

const s3config = {
    endpoint: `http://${s3host}:8000`,
    s3ForcePathStyle: true,
    credentials: new AWS.Credentials('accessKey1', 'verySecretKey1')
};
const zookeeperConfig = {
    host: 'localhost',
    port: 2181,
};
const bucketFileConfig = {
    host: s3host,
    port: 9990,
};

describe.only('replicator', () => {
    let replicatorState;
    let s3;
    let initialLastProcessedSeq;

    before(done => {
        s3 = new S3(s3config);
        async.waterfall([
            next => {
                s3.createBucket({
                    Bucket: testBucket
                }, next);
            },
            (data, next) => {
                s3.putBucketVersioning(
                    { Bucket: testBucket,
                      VersioningConfiguration: {
                          Status: 'Enabled'
                      }
                    }, next);
            },
            (data, next) => {
                s3.putBucketReplication(
                    { Bucket: testBucket,
                      ReplicationConfiguration: {
                          Role: 'arn:aws:iam:::123456789012:role/backbeat',
                          Rules: [{
                              Destination: {
                                  Bucket: 'arn:aws:s3:::dummy-dest-bucket',
                                  StorageClass: 'STANDARD'
                              },
                              Prefix: '',
                              Status: 'Enabled'
                          }]
                      }
                    }, next);
            },
            (data, next) => {
                replicatorApi.openBucketFileLog(bucketFileConfig, log, next);
            },
            (logState, next) => {
                replicatorApi.createReplicator(logState, zookeeperConfig,
                                               log, next);
            },
            (_replicatorState, next) => {
                replicatorState = _replicatorState;
                next();
            }
        ], err => {
            assert.ifError(err);
            done();
        });
    });
    after(done => {
        async.waterfall([
            next => {
                next();
            }
        ], done);
    });

    it('processAllLogEntries with nothing to do', done => {
        replicatorApi.processAllLogEntries(
            replicatorState, { maxRead: 10 }, log, (err, counters) => {
                // we need to fetch what is the current last processed
                // sequence number because the storage backend may
                // have a non-empty log already
                initialLastProcessedSeq = counters.lastProcessedSeq;
                assert.ifError(err);
                assert.deepStrictEqual(counters, {
                    read: 2, queued: 0,
                    lastProcessedSeq: initialLastProcessedSeq });
                done();
            });
    });
    it('processAllLogEntries with an object to replicate', done => {
        async.waterfall([
            next => {
                s3.putObject({ Bucket: testBucket,
                               Key: 'popo42',
                               Body: 'howdy',
                               Tagging: 'mytag=mytagvalue' }, next);
            },
            (data, next) => {
                replicatorApi.processAllLogEntries(
                    replicatorState, { maxRead: 10 }, log, next);
            },
            (counters, next) => {
                assert.deepStrictEqual(counters, {
                    read: 2, queued: 1,
                    lastProcessedSeq: initialLastProcessedSeq + 2 });
                next();
            }
        ], err => {
            assert.ifError(err);
            done();
        });
    });
});
