const async = require('async');
const zookeeper = require('node-zookeeper-client');

const arsenal = require('arsenal');
const { isMasterKey } = require('arsenal/lib/versioning/Version');
const Logger = require('werelogs').Logger;
const BackbeatProducer = require('../../lib/BackbeatProducer');
const MetadataFileClient = arsenal.storage.metadata.MetadataFileClient;
const raftAdmin = arsenal.storage.metadata.raftAdmin;

function openRaftLog(raftConfig, raftSession, log, done) {
    log.info('initializing raft log handle',
             { method: 'openRaftLog', raftConfig, raftSession });
    // TODO find the leader session here
    const { host, adminPort } = raftConfig.repds[raftSession];
    const url = `http://${host}:${adminPort}`;
    const raftLogState = {
        logProxy: raftAdmin.openRecordLog({ url, logger: log }),
        raftSession,
    };
    setImmediate(() => done(null, raftLogState));
}

function openBucketFileLog(bucketFileConfig, log, done) {
    log.info('initializing bucketfile log handle',
             { method: 'openBucketFileLog', bucketFileConfig });
    const mdClient = new MetadataFileClient({
        host: bucketFileConfig.host,
        port: bucketFileConfig.port,
        log: {
            logLevel: 'info',
            dumpLevel: 'error',
        },
    });
    const logProxy = mdClient.openRecordLog({
        logName: bucketFileConfig.logName,
    }, err => {
        if (err) {
            return done(err);
        }
        const bucketFileLogState = {
            logProxy,
            logName: bucketFileConfig.logName || 'main',
        };
        return done(null, bucketFileLogState);
    });
}

function writeLastProcessedSeq(replicatorState, log, done) {
    const zkClient = replicatorState.zkClient;
    const pathToLastProcessedSeq = replicatorState.pathToLastProcessedSeq;
    const lastProcessedSeq = replicatorState.lastProcessedSeq;
    log.debug('saving last processed sequence number',
              { method: 'writeLastProcessedSeq',
                zkPath: pathToLastProcessedSeq,
                lastProcessedSeq });
    zkClient.setData(
        pathToLastProcessedSeq, Buffer(lastProcessedSeq.toString()), -1,
        err => {
            if (err) {
                log.error('error saving last processed sequence number',
                          { method: 'writeLastProcessedSeq',
                            zkPath: pathToLastProcessedSeq,
                            lastProcessedSeq });
            }
            done(err);
        });
}

function readLastProcessedSeq(replicatorState, log, done) {
    const zkClient = replicatorState.zkClient;
    const pathToLastProcessedSeq = replicatorState.pathToLastProcessedSeq;
    replicatorState.zkClient.getData(
        pathToLastProcessedSeq, (err, data, stat) => {
            if (err) {
                if (err.name !== 'NO_NODE') {
                    log.error('Could not fetch latest processed ' +
                              'sequence number',
                              { method: 'readLastProcessedSeq',
                                error: err,
                                errorStack: err.stack });
                    return done(err);
                }
                return zkClient.mkdirp(pathToLastProcessedSeq, err => {
                    if (err) {
                        log.error('Could not pre-create path in zookeeper',
                                  { method: 'readLastProcessedSeq',
                                    zkPath: pathToLastProcessedSeq,
                                    error: err,
                                    errorStack: err.stack });
                        return done(err);
                    }
                    return done(null, 0);
                });
            }
            if (data) {
                const lastProcessedSeq = Number.parseInt(data, 10);
                if (isNaN(lastProcessedSeq)) {
                    log.error('invalid latest processed sequence number',
                              { method: 'readLastProcessedSeq',
                                zkPath: pathToLastProcessedSeq,
                                lastProcessedSeq: data.toString() });
                    return done(null, 0);
                }
                log.debug('fetched latest processed sequence number',
                          { method: 'readLastProcessedSeq',
                            zkPath: pathToLastProcessedSeq,
                            lastProcessedSeq });
                return done(null, lastProcessedSeq);
            }
            return done(null, 0);
        });
}

function createReplicator(logState, zookeeperConfig, log, cb) {
    const replicatorState = { logState };
    const zkConf = { host: 'localhost', port: 2181,
                     namespace: '/backbeat/replicator' };
    if (zookeeperConfig) {
        Object.assign(zkConf, zookeeperConfig);
    }
    async.parallel([
        done => {
            const producer = new BackbeatProducer({
                zookeeper: zkConf,
                log: { logLevel: 'info', dumpLevel: 'error' },
                topic: 'replication',
            });
            producer.once('error', done);
            producer.once('ready', () => {
                producer.removeAllListeners('error');
                replicatorState.producer = producer;
                done();
            });
        },
        done => {
            const zookeeperUrl =
                      `${zkConf.host}:${zkConf.port}${zkConf.namespace}`;
            log.info('opening zookeeper connection for state management',
                     { zookeeperUrl });
            const zkClient = zookeeper.createClient(zookeeperUrl);
            zkClient.connect();
            replicatorState.zkClient = zkClient;
            if (logState.raftSession !== undefined) {
                replicatorState.pathToLastProcessedSeq =
                    `/logState/raft_${logState.raftSession}/lastProcessedSeq`;
            } else {
                replicatorState.pathToLastProcessedSeq =
                    `/logState/bucketFile_${logState.logName}/lastProcessedSeq`;
            }
            readLastProcessedSeq(replicatorState, log, (err, seq) => {
                if (err) {
                    return done(err);
                }
                replicatorState.lastProcessedSeq = seq;
                return done();
            });
        }
    ], err => {
        if (err) {
            log.error('Error starting up replicator',
                      { method: 'createReplicator', logState,
                        error: err, errorStack: err.stack });
            return cb(err);
        }
        return cb(null, replicatorState);
    });
}


function logEntryToQueueEntry(record, entry, log) {
    if (entry.type === 'put') {
        const value = JSON.parse(entry.value);
        if (! isMasterKey(entry.key) &&
            value.replicationInfo &&
            value.replicationInfo.status === 'PENDING') {
            log.trace('pushing entry', { entry });
            const queueEntry = {
                type: entry.type,
                bucket: record.db,
                key: entry.key,
                value: entry.value,
            };
            return {
                key: entry.key,
                message: JSON.stringify(queueEntry),
            };
        }
    }
    return null;
}

function processLogEntries(replicatorState, params, log, cb) {
    const producer = replicatorState.producer;
    const logProxy = replicatorState.logState.logProxy;
    const lastProcessedSeq = replicatorState.lastProcessedSeq;
    const readOptions = {};
    if (lastProcessedSeq !== undefined) {
        readOptions.startSeq = lastProcessedSeq + 1;
    }
    if (params && params.maxRead !== undefined) {
        readOptions.limit = params.maxRead;
    }
    const entriesToPublish = [];
    let nbLogEntriesRead = 0;
    async.waterfall([
        done => {
            log.debug('reading records', { readOptions });
            logProxy.readRecords(readOptions, (err, recordStream) => {
                if (err) {
                    log.error('error while reading log records',
                              { method: 'log.readRecords',
                                error: err, errorStack: err.stack });
                    return done(err);
                }
                return done(err, recordStream);
            });
        },
        (recordStream, done) => {
            recordStream.on('data', record => {
                record.entries.forEach(entry => {
                    nbLogEntriesRead += 1;
                    const queueEntry = logEntryToQueueEntry(record, entry,
                                                            log);
                    if (queueEntry) {
                        entriesToPublish.push(queueEntry);
                    }
                });
            });
            recordStream.on('end', () => {
                log.debug('ending record stream');
                done();
            });
        },
        done => {
            if (entriesToPublish.length > 0) {
                return producer.send(entriesToPublish, err => {
                    if (err) {
                        log.error('error publishing entries from log',
                                  { method: 'log.readRecords',
                                    error: err, errorStack: err.stack });
                        return done(err);
                    }
                    replicatorState.lastProcessedSeq =
                        lastProcessedSeq + nbLogEntriesRead;
                    log.debug('entries published successfully',
                              { entryCount: entriesToPublish.length,
                                lastProcessedSeq });
                    return done();
                });
            }
            replicatorState.lastProcessedSeq =
                lastProcessedSeq + nbLogEntriesRead;
            return done();
        },
        done => {
            return writeLastProcessedSeq(replicatorState, log, done);
        }], err => {
            if (err) {
                return cb(err);
            }
            return cb(null, {
                read: nbLogEntriesRead,
                queued: entriesToPublish.length,
                lastProcessedSeq:
                replicatorState.lastProcessedSeq,
                processedAll: (!params || !params.maxRead
                               || nbLogEntriesRead < params.maxRead),
            });
        });
    return undefined;
}



function processAllLogEntries(replicatorState, params, log, done) {
    const countersTotal = {
        read: 0,
        queued: 0,
        lastProcessedSeq: replicatorState.lastProcessedSeq,
    };
    function cbProcess(err, counters) {
        if (err) {
            return done(err);
        }
        countersTotal.read += counters.read;
        countersTotal.queued += counters.queued;
        countersTotal.lastProcessedSeq = counters.lastProcessedSeq;
        log.debug('process batch finished', { counters, countersTotal });
        if (counters.processedAll) {
            return done(null, countersTotal);
        }
        return processLogEntries(replicatorState, params, log, cbProcess);
    }
    processLogEntries(replicatorState, params, log, cbProcess);
}

module.exports = {
    openRaftLog,
    openBucketFileLog,
    createReplicator,
    processLogEntries,
    processAllLogEntries,
};
