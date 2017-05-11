const http = require('http');
const async = require('async');
const schedule = require('node-schedule');

const mapSeries = async.mapSeries;
const waterfall = async.waterfall;

const Subscriber = require('./lib/Subscriber');
const Logger = require('werelogs').Logger;
const logger = new Logger('Backbeat:Replication');
const log = logger.newRequestLogger();

const sourceHostname = 'localhost';
const sourcePort = '8000';
const targetHostname = '217.182.11.7';
const targetPort = '8000';

const sub = new Subscriber({
    zookeeper: { host: 'localhost', port: 2181 },
    log: { logLevel: 'info', dumpLevel: 'error' },
    topic: 'replication',
    partition: 0,
    groupId: 'crr',
});
const subClient = sub.setClient();
const httpAgent = new http.Agent({ keepAlive: true });

function _createRequestHeader(where, method, path, headers) {
    const reqHeaders = headers || {};
    const hostname = where === 'source' ? sourceHostname : targetHostname;
    const port = where === 'source' ? sourcePort : targetPort;
    reqHeaders['content-type'] = 'application/octet-stream';
    return {
        hostname,
        port,
        method,
        path,
        headers: reqHeaders,
        agent: httpAgent,
    };
}

function _createRequest(reqHeaders) {
    const request = http.request(reqHeaders);
    return request;
}

function _getData(bucket, object, locations, log, cb) {
    log.info('getting data', { bucket, object });
    const payload = JSON.stringify(locations);
    const path = `/_/backbeat/${bucket}/${object}/data`;
    const reqHeaders = _createRequestHeader('source', 'POST', path, {
        'content-length': Buffer.byteLength(payload),
    });
    const req = _createRequest(reqHeaders);
    req.on('error', cb);
    req.on('response', incomingMsg => cb(null, incomingMsg));
    req.end(payload);
}

function _putData(bucket, object, sourceStream, contentLength, contentMd5, log,
    cb) {
    log.info('putting data', { bucket, object });
    const path = `/_/backbeat/${bucket}/${object}/data`;
    const reqHeaders = _createRequestHeader('target', 'PUT', path, {
        'content-length': contentLength,
        'content-md5': contentMd5,
    });
    const req = _createRequest(reqHeaders);
    sourceStream.pipe(req);
    req.on('response', incomingMsg => {
        const body = [];
        let bodyLen = 0;
        incomingMsg.on('data', chunk => {
            body.push(chunk);
            bodyLen += chunk.length;
        });
        incomingMsg.on('end', () => cb(null,
            Buffer.concat(body, bodyLen).toString()));
    });
    req.on('error', cb);
}

function _putMetaData(where, bucket, object, payload, log, cb) {
    log.info('putting metadata', { where, bucket, object });
    const path = `/_/backbeat/${bucket}/${object}/metadata`;
    const reqHeaders = _createRequestHeader(where, 'PUT', path, {
        'content-length': Buffer.byteLength(payload),
    });
    const req = _createRequest(reqHeaders);
    req.write(payload);
    req.on('response', response => {
         const body = [];
         response.on('data', chunk => body.push(chunk));
         response.on('end', () => cb());
     });
    req.on('error', cb);
    req.end();
}

function _processEntry(entry, cb) {
    const record = JSON.parse(entry.value);
    const mdEntry = JSON.parse(record.value);
    const object = record.key.split('\u0000')[0];
    const bucket = record.bucket;
    log.info('processing entry', { object });
    waterfall([
        // get data stream from source bucket
        next => _getData(bucket, object, mdEntry.location, log, next),
        // put data in target bucket
        (stream, next) => _putData(bucket, object, stream,
            mdEntry['content-length'], mdEntry['content-md5'], log, next),
        // update location, replication status and put metadata in target bucket
        (locationRes, next) => {
            log.info('updating dest MD');
            const destEntry = Object.assign({}, mdEntry);
            destEntry.location = JSON.parse(locationRes);
            destEntry['x-amz-replication-status'] = 'REPLICA';
            _putMetaData('target', bucket, object,
                         JSON.stringify(destEntry), log, next);
        },
        // update rep. status to COMPLETED and put metadata in source bucket
        next => {
            log.info('updating source MD to COMPLETED');
            mdEntry['x-amz-replication-status'] = 'COMPLETED';
            _putMetaData('source', bucket, object,
                         JSON.stringify(mdEntry), log, next);
        },
    ], cb);
}


function replicateEntries() {
    log.info('starting replication....');
    subClient.read((err, entries) => {
        if (err) {
            return log.error('error getting messages', err);
        }
        log.info('processing entries...', { entriesCount: entries.length });
        return mapSeries(entries, _processEntry, err => {
            if (err) {
                return log.error('error processing entries',
                    { error: err.stack || err });
            }
            return sub.commit(() => log.info('successfully processed entries',
                { entries: entries.length }));
        });
    });
}
// schedule every 5 seconds
replicateEntries();
//schedule.scheduleJob('*/5 * * * * *', replicateEntries);
