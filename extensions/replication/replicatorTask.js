const async = require('async');
const schedule = require('node-schedule');

const errors = require('arsenal').errors;
const replicatorApi = require('./replicatorApi');
const Logger = require('werelogs').Logger;
const logger = new Logger('Backbeat:Replication');
const log = logger.newRequestLogger();

//FIXME: should be from config object
const raftConfig = {
    repds: [
        { host: '172.17.0.2', adminPort: 4205 },
        { host: '172.17.0.2', adminPort: 4206 },
        { host: '172.17.0.2', adminPort: 4207 },
        { host: '172.17.0.2', adminPort: 4208 },
        { host: '172.17.0.2', adminPort: 4209 },
    ],
}

const zookeeperConfig = { host: 'localhost', port: 2181 };
const bucketFileConfig = { host: '172.17.0.2', port: 9990 };

const replicationConfig = {
    //scality: {
    //    replicateRaftSessions: [0],
    //},
    file: bucketFileConfig,
    cronRule: '*/5 * * * * *',
    batchMaxRead: 10000,
};


function queueBatch(replicatorState, taskState) {
    if (replicatorState.batchInProgress) {
        log.warn('skipping replication batch: ' +
                 'previous one still in progress');
        return undefined;
    }
    log.info('start queueing replication batch');
    replicatorState.batchInProgress = true;
    replicatorApi.processAllLogEntries(
        replicatorState, { maxRead: replicationConfig.batchMaxRead },
        log, (err, counters) => {
            if (err) {
                log.error('an error occurred during replication',
                          { error: err, errorStack: err.stack });
            } else {
                log.info('replication batch finished', { counters });
            }
            replicatorState.batchInProgress = false;
        });
    return undefined;
}

async.waterfall([
    done => {
        if (replicationConfig.scality !== undefined) {
            //FIXME do this for all replicated raft sessions
            replicatorApi.openRaftLog(raftConfig, 0, log, done);
        } else if (replicationConfig.file !== undefined) {
            replicatorApi.openBucketFileLog(bucketFileConfig, log, done);
        } else {
            log.error('expect "scality" or "file" section in ' +
                      'replication configuration');
            done(errors.InternalError);
        }
    }, (logState, done) => {
        replicatorApi.createReplicator(logState, zookeeperConfig, log, done);
    }, (replicatorState, done) => {
        const taskState = {
            batchInProgress: false,
        };
        const queueJob = schedule.scheduleJob(replicationConfig.cronRule, () => {
            queueBatch(replicatorState, taskState);
        });
        done();
    }
], err => {
    if (err) {
        log.error('error during replicator initialization', { error: err });
        process.exit(1);
    }
});
