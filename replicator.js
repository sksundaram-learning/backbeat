const schedule = require('node-schedule');
const arsenal = require('arsenal');
const Logger = require('werelogs').Logger;
const Publisher = require('./lib/Publisher');
const MetadataFileClient = arsenal.storage.metadata.MetadataFileClient;
const logger = new Logger('Backbeat:Replication');
const log = logger.newRequestLogger();

const sourceMD = new MetadataFileClient({
    host: 'localhost',
    port: '9990',
    recordLogEnabled: true,
    log: {
        logLevel: 'info',
        dumpLevel: 'error',
    },
});

const pub = new Publisher({
    zookeeper: { host: 'localhost', port: 2181 },
    log: { logLevel: 'info', dumpLevel: 'error' },
    topic: 'replication',
    partition: 0,
});

const LOGNAME = 'main';

let lastProcessedSeq = 0;


function queueEntries() {
    sourceMD.openRecordLog(LOGNAME, (err, logProxy) => {
        if (err) {
            return log.error('error fetching log stream', { error: err });
        }
        pub.setClient(err => {
            if (err) {
                return log.error('error in publisher.setClient',
                                 { error: err.stack });
            }
	    // pub.refreshMetadata();
            const readOptions = { minSeq: (lastProcessedSeq + 1) };
            logProxy.readRecords(readOptions, (err, recordStream) => {
                recordStream.on('data', record => {
                    const value = JSON.parse(record.value);
                    const repStatus = value['x-amz-replication-status'];
                    if (record.type === 'put' && record.key.includes('\u0000')
                        && repStatus === 'PENDING') {
                        const queueEntry = {
                            type: record.type,
                            seq: record.seq,
                            bucket: record.prefix[0],
                            key: record.key,
                            value: record.value,
                            timestamp: record.timestamp,
                        };
                        pub.publish(JSON.stringify(queueEntry), err => {
                            if (err) {
                                return log.error('error publishing entry',
                                                 { error: err });
                            }
                            lastProcessedSeq = record.seq;
                            return log.info('entry published successfully', { queueEntry });
                        });
                    }
                });
                recordStream.on('end', () => {
                    //pub.close();
                    log.info('ending record stream');
                });
            });
            return undefined;
        });
        return undefined;
    });
}

// schedule every 3 seconds
queueEntries();
//schedule.scheduleJob('*/3 * * * * *', queueEntries);
