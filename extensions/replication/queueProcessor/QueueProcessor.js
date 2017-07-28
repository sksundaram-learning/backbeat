'use strict'; // eslint-disable-line

const async = require('async');
const assert = require('assert');
const AWS = require('aws-sdk');
const http = require('http');
const BackOff = require('backo');

const Logger = require('werelogs').Logger;

const errors = require('arsenal').errors;
const jsutil = require('arsenal').jsutil;
const VaultClient = require('vaultclient').Client;

const authdata = require('../../../conf/authdata.json');

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const BackbeatClient = require('../../../lib/clients/BackbeatClient');
const QueueEntry = require('../utils/QueueEntry');
const CredentialsManager = require('../../../credentials/CredentialsManager');

class _AccountAuthManager {
    constructor(authConfig, log) {
        assert.strictEqual(authConfig.type, 'account');

        this._log = log;
        const accountInfo = authdata.accounts.find(
            account => account.name === authConfig.account);
        if (accountInfo === undefined) {
            throw Error(`No such account registered: ${authConfig.account}`);
        }
        if (accountInfo.arn === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"arn" property defined');
        }
        if (accountInfo.canonicalID === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"canonicalID" property defined');
        }
        if (accountInfo.displayName === undefined) {
            throw Error(`Configured account ${authConfig.account} has no ` +
                        '"displayName" property defined');
        }
        this._accountArn = accountInfo.arn;
        this._canonicalID = accountInfo.canonicalID;
        this._displayName = accountInfo.displayName;
        this._credentials = new AWS.Credentials(accountInfo.keys.access,
                                                accountInfo.keys.secret);
    }

    getCredentials() {
        return this._credentials;
    }

    lookupAccountAttributes(accountId, cb) {
        const localAccountId = this._accountArn.split(':')[3];
        if (localAccountId !== accountId) {
            this._log.error('Target account for replication must match ' +
                            'configured destination account ARN',
                            { targetAccountId: accountId,
                              localAccountId });
            return process.nextTick(() => cb(errors.AccountNotFound));
        }
        // return local account's attributes
        return process.nextTick(
            () => cb(null, { canonicalID: this._canonicalID,
                             displayName: this._displayName }));
    }
}

class _RoleAuthManager {
    constructor(authConfig, roleArn, log) {
        this._log = log;
        const { host, port } = authConfig.vault;
        this._vaultclient = new VaultClient(host, port);
        this._credentials = new CredentialsManager(host, port,
                                                   'replication', roleArn);
    }

    getCredentials() {
        return this._credentials;
    }

    lookupAccountAttributes(accountId, cb) {
        this._vaultclient.getCanonicalIdsByAccountIds([accountId], {},
            (err, res) => {
                if (err) {
                    return cb(err);
                }
                if (!res || !res.message || !res.message.body
                    || res.message.body.length === 0) {
                    return cb(errors.AccountNotFound);
                }
                return cb(null, {
                    canonicalID: res.message.body[0].canonicalId,
                    displayName: res.message.body[0].name,
                });
            });
    }
}


function _extractAccountIdFromRole(role) {
    return role.split(':')[4];
}

class QueueProcessor {

    /**
     * Create a queue processor object to activate Cross-Region
     * Replication from a kafka topic dedicated to store replication
     * entries to a target S3 endpoint.
     *
     * @constructor
     * @param {Object} zkConfig - zookeeper configuration object
     * @param {string} zkConfig.endpoint - zookeeper endpoint string
     *   as "host:port[/chroot]"
     * @param {Object} sourceConfig - source S3 configuration
     * @param {Object} sourceConfig.s3 - s3 endpoint configuration object
     * @param {Object} sourceConfig.auth - authentication info on source
     * @param {Object} destConfig - target S3 configuration
     * @param {Object} destConfig.s3 - s3 endpoint configuration object
     * @param {Object} destConfig.auth - authentication info on target
     * @param {Object} repConfig - replication configuration object
     * @param {String} repConfig.topic - replication topic name
     * @param {String} repConfig.queueProcessor - config object
     *   specific to queue processor
     * @param {String} repConfig.queueProcessor.groupId - kafka
     *   consumer group ID
     * @param {Logger} logConfig - logging configuration object
     * @param {String} logConfig.logLevel - logging level
     * @param {Logger} logConfig.dumpLevel - dump level
     */
    constructor(zkConfig, sourceConfig, destConfig, repConfig, logConfig) {
        this.zkConfig = zkConfig;
        this.sourceConfig = sourceConfig;
        this.destConfig = destConfig;
        this.repConfig = repConfig;
        this.logConfig = logConfig;

        this.logger = new Logger('Backbeat:Replication:QueueProcessor',
                                 { level: logConfig.logLevel,
                                   dump: logConfig.dumpLevel });

        this.s3sourceAuthManager = null;
        this.s3destAuthManager = null;
        this.S3source = null;
        this.backbeatSource = null;
        this.backbeatDest = null;

        // TODO: for SSL support, create HTTPS agents instead
        this.sourceHTTPAgent = new http.Agent({ keepAlive: true });
        this.destHTTPAgent = new http.Agent({ keepAlive: true });
    }

    _createAuthManager(authConfig, roleArn, log) {
        if (authConfig.type === 'account') {
            return new _AccountAuthManager(authConfig, log);
        }
        return new _RoleAuthManager(authConfig, roleArn, log);
    }

    _setupRoles(entry, log, cb) {
        log.debug('getting bucket replication',
                  { entry: entry.getLogInfo() });
        const entryRolesString = entry.getReplicationRoles();
        let entryRoles;
        if (entryRolesString !== undefined) {
            entryRoles = entryRolesString.split(',');
        }
        if (entryRoles === undefined || entryRoles.length !== 2) {
            log.error('expecting two roles separated by a ' +
                      'comma in entry replication configuration',
                      { entry: entry.getLogInfo(),
                        origin: this.sourceConfig.s3,
                        roles: entryRolesString });
            return cb(errors.BadRole);
        }
        this._setupClients(entryRoles[0], entryRoles[1], log);
        return this.S3source.getBucketReplication(
            { Bucket: entry.getBucket() }, (err, data) => {
                if (err) {
                    log.error('error getting replication ' +
                              'configuration from S3',
                              { entry: entry.getLogInfo(),
                                origin: this.sourceConfig.s3,
                                error: err.message,
                                errorStack: err.stack,
                                httpStatus: err.statusCode });
                    return cb(err);
                }
                const roles = data.ReplicationConfiguration.Role.split(',');
                if (roles.length !== 2) {
                    log.error('expecting two roles separated by a ' +
                              'comma in bucket replication configuration',
                              { entry: entry.getLogInfo(),
                                origin: this.sourceConfig.s3,
                                roles });
                    return cb(errors.BadRole);
                }
                if (roles[0] !== entryRoles[0]) {
                    log.error('role in replication entry for source does ' +
                              'not match role in bucket replication ' +
                              'configuration ',
                              { entry: entry.getLogInfo(),
                                origin: this.sourceConfig.s3,
                                entryRole: entryRoles[0],
                                bucketRole: roles[0] });
                    return cb(errors.BadRole);
                }
                if (roles[1] !== entryRoles[1]) {
                    log.error('role in replication entry for target does ' +
                              'not match role in bucket replication ' +
                              'configuration ',
                              { entry: entry.getLogInfo(),
                                origin: this.sourceConfig.s3,
                                entryRole: entryRoles[1],
                                bucketRole: roles[1] });
                    return cb(errors.BadRole);
                }
                return cb(null, roles[0], roles[1]);
            });
    }

    _setTargetAccountMd(destEntry, targetRole, log, cb) {
        log.debug('changing target account owner',
                  { entry: destEntry.getLogInfo() });
        const targetAccountId = _extractAccountIdFromRole(targetRole);
        this.s3destAuthManager.lookupAccountAttributes(
            targetAccountId, (err, accountAttr) => {
                if (err) {
                    return cb(err);
                }
                log.debug('setting owner info in target metadata',
                          { entry: destEntry.getLogInfo(),
                            accountAttr });
                destEntry.setOwner(accountAttr.canonicalID,
                                   accountAttr.displayName);
                return cb();
            });
    }

    _getData(entry, log, cb) {
        log.debug('getting data', { entry: entry.getLogInfo() });
        const req = this.S3source.getObject({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            VersionId: entry.getEncodedVersionId() });
        const incomingMsg = req.createReadStream();
        req.on('error', err => {
            log.error('an error occurred when getting data from S3',
                      { entry: entry.getLogInfo(),
                        origin: this.sourceConfig.s3,
                        error: err.message,
                        httpStatus: err.statusCode });
            incomingMsg.emit('error', err);
        });
        return cb(null, incomingMsg);
    }

    _putData(entry, sourceStream, log, cb) {
        log.debug('putting data', { entry: entry.getLogInfo() });
        const cbOnce = jsutil.once(cb);
        sourceStream.on('error', err => {
            log.error('error from source S3 server',
                      { entry: entry.getLogInfo(),
                        error: err.message });
            return cbOnce(err);
        });
        this.backbeatDest.putData({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            CanonicalID: entry.getOwnerCanonicalId(),
            ContentLength: entry.getContentLength(),
            ContentMD5: entry.getContentMD5(),
            Body: sourceStream,
        }, (err, data) => {
            if (err) {
                log.error('an error occurred when putting data to S3',
                          { entry: entry.getLogInfo(),
                            origin: this.destConfig.s3,
                            error: err.message,
                            errorStack: err.stack });
                return cbOnce(err);
            }
            return cbOnce(null, data.Location);
        });
    }

    _putMetadata(where, entry, log, cb) {
        log.debug('putting metadata',
                  { where, entry: entry.getLogInfo(),
                    replicationStatus: entry.getReplicationStatus() });
        const cbOnce = jsutil.once(cb);
        const target = where === 'source' ?
                  this.backbeatSource : this.backbeatDest;
        const mdBlob = entry.getMetadataBlob();
        target.putMetadata({
            Bucket: entry.getBucket(),
            Key: entry.getObjectKey(),
            ContentLength: Buffer.byteLength(mdBlob),
            Body: mdBlob,
        }, (err, data) => {
            if (err) {
                log.error('an error occurred when putting metadata to S3',
                          { entry: entry.getLogInfo(),
                            origin: this.destConfig.s3,
                            error: err.message,
                            errorStack: err.stack });
                return cbOnce(err);
            }
            return cbOnce(null, data);
        });
    }

    _setupClients(sourceRole, targetRole, log) {
        this.s3sourceAuthManager =
            this._createAuthManager(this.sourceConfig.auth, sourceRole, log);
        this.s3destAuthManager =
            this._createAuthManager(this.destConfig.auth, targetRole, log);
        this.S3source = new AWS.S3({
            endpoint: `${this.sourceConfig.s3.transport}://` +
                `${this.sourceConfig.s3.host}:${this.sourceConfig.s3.port}`,
            credentials:
            this.s3sourceAuthManager.getCredentials(),
            sslEnabled: true,
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
            httpOptions: { agent: this.sourceHTTPAgent },
        });
        this.backbeatSource = new BackbeatClient({
            endpoint: `${this.sourceConfig.s3.transport}://` +
                `${this.sourceConfig.s3.host}:${this.sourceConfig.s3.port}`,
            credentials:
            this.s3sourceAuthManager.getCredentials(),
            sslEnabled: true,
            httpOptions: { agent: this.sourceHTTPAgent },
        });

        this.backbeatDest = new BackbeatClient({
            endpoint: `${this.destConfig.s3.transport}://` +
                `${this.destConfig.s3.host}:${this.destConfig.s3.port}`,
            credentials:
            this.s3destAuthManager.getCredentials(),
            sslEnabled: true,
            httpOptions: { agent: this.destHTTPAgent },
        });
    }

    /**
     * Proceed to the replication of an object given a kafka
     * replication queue entry
     *
     * @param {object} kafkaEntry - entry generated by the queue populator
     * @param {string} kafkaEntry.key - kafka entry key
     * @param {string} kafkaEntry.value - kafka entry value
     * @param {function} done - callback function
     * @return {undefined}
     */
    processKafkaEntry(kafkaEntry, done) {
        const log = this.logger.newRequestLogger();

        const sourceEntry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        if (sourceEntry.error) {
            log.error('error processing source entry',
                      { error: sourceEntry.error });
            return process.nextTick(() => done(errors.InternalError));
        }
        const backoffCtx = new BackOff({ min: 1000, max: 300000,
                                         jitter: 0.1, factor: 1.5 });
        return this._tryProcessQueueEntry(sourceEntry, backoffCtx, log,
                                          done);
    }

    _tryProcessQueueEntry(sourceEntry, backoffCtx, log, done) {
        const destEntry = sourceEntry.toReplicaEntry();

        log.debug('processing entry',
                  { entry: sourceEntry.getLogInfo() });
        let step = null;

        const _handleReplicationOutcome = err => {
            if (!err) {
                log.debug('replication succeeded for object, updating ' +
                          'source replication status to COMPLETED',
                          { entry: sourceEntry.getLogInfo() });
                return this._tryUpdateReplicationStatus(
                    sourceEntry.toCompletedEntry(), backoffCtx, log, done);
            }
            // Rely on AWS SDK notion of retryable error to decide if
            // we should set the entry replication status to FAILED
            // (non retryable) or retry later.
            if (err.retryable) {
                log.warn('temporary failure to replicate object',
                         { entry: sourceEntry.getLogInfo(),
                           error: err });
                return this._retryProcessQueueEntry(sourceEntry, backoffCtx,
                                                    log, done);
            }
            if ((step === 'setupRoles' ||
                 step === 'setTargetAccountMd' ||
                 step === 'getData') &&
                (err.BadRole ||
                 err.NoSuchEntity ||
                 err.AccessDenied)) {
                log.error('replication failed permanently for object, ' +
                          'processing skipped',
                          { entry: sourceEntry.getLogInfo(),
                            error: err.description });
                return done();
            }
            log.debug('replication failed permanently for object, ' +
                      'updating replication status to FAILED',
                      { entry: sourceEntry.getLogInfo() });
            return this._tryUpdateReplicationStatus(
                sourceEntry.toFailedEntry(), backoffCtx, log, done);
        };

        if (sourceEntry.isDeleteMarker()) {
            return async.waterfall([
                next => {
                    step = 'setupRoles';
                    this._setupRoles(sourceEntry, log, next);
                },
                (sourceRole, targetRole, next) => {
                    step = 'setTargetAccountMd';
                    this._setTargetAccountMd(destEntry, targetRole, log,
                                             next);
                },
                // put metadata in target bucket
                next => {
                    step = 'putMetadata';
                    // TODO check that bucket role matches role in metadata
                    this._putMetadata('target', destEntry, log, next);
                },
            ], _handleReplicationOutcome);
        }
        return async.waterfall([
            // get data stream from source bucket
            next => {
                step = 'setupRoles';
                this._setupRoles(sourceEntry, log, next);
            },
            (sourceRole, targetRole, next) => {
                step = 'setTargetAccountMd';
                this._setTargetAccountMd(destEntry, targetRole, log, next);
            },
            next => {
                step = 'getData';
                // TODO check that bucket role matches role in metadata
                this._getData(sourceEntry, log, next);
            },
            // put data in target bucket
            (stream, next) => {
                step = 'putData';
                this._putData(destEntry, stream, log, next);
            },
            // update location, replication status and put metadata in
            // target bucket
            (location, next) => {
                step = 'putMetadata';
                destEntry.setLocation(location);
                this._putMetadata('target', destEntry, log, next);
            },
        ], _handleReplicationOutcome);
    }

    _tryUpdateReplicationStatus(updatedSourceEntry, backoffCtx, log, done) {
        const _doneUpdate = err => {
            if (!err) {
                log.info('replication status updated',
                         { entry: updatedSourceEntry.getLogInfo(),
                           replicationStatus:
                           updatedSourceEntry.getReplicationStatus() });
                return done();
            }
            log.error('an error occurred when writing replication ' +
                      'status',
                      { entry: updatedSourceEntry.getLogInfo(),
                        replicationStatus:
                        updatedSourceEntry.getReplicationStatus() });
            // Rely on AWS SDK notion of retryable error to decide if
            // we should retry or give up updating the status.
            if (err.retryable) {
                return this._retryUpdateReplicationStatus(
                    updatedSourceEntry, backoffCtx, log, done);
            }
            return done();
        };

        if (this.backbeatSource !== null) {
            return this._putMetadata('source',
                                     updatedSourceEntry, log, _doneUpdate);
        }
        log.info('replication status update skipped',
                 { entry: updatedSourceEntry.getLogInfo(),
                   replicationStatus:
                   updatedSourceEntry.getReplicationStatus() });
        return done();
    }

    _retryProcessQueueEntry(sourceEntry, backoffCtx, log, done) {
        const retryDelayMs = backoffCtx.duration();
        log.info('scheduled retry of entry replication',
                 { entry: sourceEntry.getLogInfo(),
                   retryDelay: `${retryDelayMs}ms` });
        setTimeout(this._tryProcessQueueEntry.bind(
            this, sourceEntry, backoffCtx, log, done),
                   retryDelayMs);
    }

    _retryUpdateReplicationStatus(updatedSourceEntry, backoffCtx, log, done) {
        const retryDelayMs = backoffCtx.duration();
        log.info('scheduled retry of replication status update',
                 { entry: updatedSourceEntry.getLogInfo(),
                   retryDelay: `${retryDelayMs}ms` });
        setTimeout(this._tryUpdateReplicationStatus.bind(
            this, updatedSourceEntry, backoffCtx, log, done),
                   retryDelayMs);
    }

    start() {
        const consumer = new BackbeatConsumer({
            zookeeper: this.zkConfig,
            log: this.logConfig,
            topic: this.repConfig.topic,
            groupId: this.repConfig.queueProcessor.groupId,
            concurrency: 1, // replication has to process entries in
                            // order, so one at a time
            queueProcessor: this.processKafkaEntry.bind(this),
        });
        consumer.on('error', () => {});
        consumer.subscribe();

        this.logger.info('queue processor is ready to consume ' +
                         'replication entries');
    }
}

module.exports = QueueProcessor;
