const { Client, Producer } = require('kafka-node');

const errors = require('arsenal').errors;
const Logger = require('werelogs').Logger;

class Publisher {

    constructor(config) {
        const { zookeeper } = config;
        this._zookeeperEndpoint = `${zookeeper.host}:${zookeeper.port}`;
        this._log = new Logger('BackbeatPublisher', {
            level: config.log.logLevel,
            dump: config.log.dumpLevel,
        });
        this._topic = config.topic;
        this._partition = config.partition;
        // 0 - no compression, 1 - gzip, 2 - snappy
        this._compression = 0;
    }

    setClient(cb) {
        const client = new Client(this._zookeeperEndpoint);
        this._producer = new Producer(client, {
            // configuration for when to consider a message as acknowledged
            requireAcks: 1,
            // amount of time in ms. to wait for all acks
            ackTimeoutMs: 100,
            // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3,
            // custom = 4), default 0
            partitionerType: 2,
        });
        this._producer.once('ready', cb);
        this._producer.once('error', error => {
            this._log.error('error creating producer', {
                error,
                method: 'Publisher.setClient',
            });
            cb(errors.InternalError);
        });
        return this;
    }

    publish(entries, cb) {
        this._log.debug('publishing entries', { method: 'Publisher.publish' });
        const payload = [{
            topic: this._topic,
            partition: this._partition,
            messages: entries,
        }];
        return this._producer.send(payload, err => {
            if (err) {
                this._log.error('error publishing entries', { error: err });
                return cb(errors.InternalError);
            }
            return cb();
        });
    }
    close() {
        this._producer.close();
    }
}

module.exports = Publisher;
