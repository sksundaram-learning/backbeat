const arsenal = require('arsenal');
const MetadataFileClient = arsenal.storage.metadata.MetadataFileClient;

const LogReader = require('./LogReader');

class BucketFileLogReader extends LogReader {
    constructor(params) {
        const { zkClient, kafkaProducer, dmdConfig, logConfig, log }
                  = params;
        log.info('initializing bucketfile log reader',
                 { method: 'BucketFileLogReader.constructor',
                   dmdConfig });

        const mdClient = new MetadataFileClient({
            host: dmdConfig.host,
            port: dmdConfig.port,
            log: logConfig,
        });
        const logConsumer = mdClient.openRecordLog({
            logName: dmdConfig.logName,
        }, err => {
            if (err) {
                this.log.error('error opening record log',
                               { method: 'BucketFileLogReader.constructor',
                                 dmdConfig });
            }
            if (err) {
                this.openRecordLogStatus = err;
            } else {
                this.openRecordLogStatus = null;
            }
            if (this.setupCb) {
                this.setupCb(err);
            }
        });
        super({ zkClient, kafkaProducer, logConsumer,
                logId: `bucketFile_${dmdConfig.logName}`, log });
        this.logName = dmdConfig.logName;
        this.openRecordLogStatus = undefined;
        this.setupCb = null;
    }

    setup(done) {
        super.setup(err => {
            if (err) {
                return done(err);
            }
            if (this.openRecordLogStatus !== undefined) {
                return done(this.openRecordLogStatus);
            }
            // wait until mdClient.openRecordLog() completes
            this.setupCb = done;
            return undefined;
        });
    }

    getLogInfo() {
        return { logName: this.logName };
    }
}

module.exports = BucketFileLogReader;
