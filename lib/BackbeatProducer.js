const { EventEmitter } = require('events');
const { Client, KeyedMessage, Producer } = require('kafka-node');

const errors = require('arsenal').errors;
const Logger = require('werelogs').Logger;
const COMPRESSION = 0;

class BackbeatProducer extends EventEmitter {

    constructor(config) {
        super();
        const { zookeeper, log, topic, partition } = config;
        this._zookeeperEndpoint = `${zookeeper.host}:${zookeeper.port}`;
        this._log = new Logger('BackbeatProducer', {
            level: log.logLevel,
            dump: log.dumpLevel,
        });
        this._topic = topic;
        this._partition = partition;
        // 0 - no compression, 1 - gzip, 2 - snappy
        this._compression = COMPRESSION;
        this._ready = false;
        // create a new producer instance
        this._client = new Client(this._zookeeperEndpoint);
        this._producer = new Producer(this._client, {
            // configuration for when to consider a message as acknowledged
            requireAcks: 1,
            // amount of time in ms. to wait for all acks
            ackTimeoutMs: 100,
            // uses keyed-message partitioner to ensure messages with the same
            // key end up in one partition
            partitionerType: 3,
            // controls compression of the message
            attributes: this._compression,
        });
        this._ready = false;
        this._producer.on('ready', () => {
            this._ready = true;
            this.emit('ready');
        });
        this._producer.on('error', error => {
            this._ready = false;
            this._log.error('error with producer', {
                errStack: error.stack,
                error: error.message,
                method: 'BackbeatProducer.constructor',
            });
            this.emit('error', error);
        });
        return this;
    }

    /**
    * create topic - works only when auto.create.topics.enable=true for the
    * Kafka server. It sends a metadata request to the server which will create
    * the topic
    * @param {callback} cb - cb(err, data)
    * @return {this} - current instance
    */
    createTopic(cb) {
        this._producer.createTopics([this._topic], cb);
        return this;
    }

    /**
    * synchronous check for producer's status
    * @return {bool} - check result
    */
    isReady() {
        return this._ready;
    }

    /**
    * sends entries/messages to the given topic
    * @param {Object[]} entries - array of entries objects' with properties
    * key and message ([{ key: 'foo', message: 'hello world'}, ...])
    * @param {callback} cb - cb(err)
    * @return {this} current instance
    */
    send(entries, cb) {
        this._log.debug('publishing entries',
            { method: 'BackbeatProducer.send' });
        if (!this._ready) {
            return process.nextTick(() => cb(errors.InternalError));
        }
        const messages = this._partition === undefined ?
            entries.map(item => new KeyedMessage(item.key, item.message)) :
            entries.map(item => item.message);
        const payload = [{
            topic: this._topic,
            messages,
        }];
        this._client.refreshMetadata([this._topic], () =>
            this._producer.send(payload, err => {
                if (err) {
                    this._log.error('error publishing entries', {
                        error: err,
                        method: 'BackbeatProducer.send',
                    });
                    return cb(errors.InternalError.
                        customizeDescription(err.message));
                }
                return cb();
            })
        );
        return this;
    }

    /**
    * close client connection
    * @param {callback} cb - cb(err)
    * @return {object} this - current class instance
    */
    close(cb) {
        this._producer.close(() => {
            this._ready = false;
            cb();
        });
        return this;
    }
}

module.exports = BackbeatProducer;
