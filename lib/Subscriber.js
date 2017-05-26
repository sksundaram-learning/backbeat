const { Client, Consumer } = require('kafka-node');

const errors = require('arsenal').errors;
const Logger = require('werelogs').Logger;

class Subscriber {

    constructor(config) {
        const { zookeeper } = config;
        this._zookeeperEndpoint = `${zookeeper.host}:${zookeeper.port}`;
        this._log = new Logger('BackbeatSubscriber', {
            level: config.log.logLevel,
            dump: config.log.dumpLevel,
        });
        this._topic = config.topic;
        this._partition = config.partition;
        this._groupId = config.groupId;
    }

    setClient() {
        const client = new Client(this._zookeeperEndpoint);
        this._consumer = new Consumer(
            client,
            [{ topic: this._topic, partition: this._partition }],
            { groupId: this._groupId, autoCommit: false }
        );
        return this;
    }

    read(cb) {
        const msgs = [];
        this._consumer.resume();

        // consume everything in the fetch loop
        this._consumer.on('message', message => {
            msgs.push(message);
        });

        // pause the fetch loop when done
        this._consumer.once('done', () => {
            this._consumer.pause();
            cb(null, msgs);
        });

        this._consumer.once('error', error => {
            this._consumer.pause();
            this._log.error('error subscribing to topic', {
                error,
                method: 'Subscriber',
                topic: this._topic,
                partition: this._partition,
            });
            cb(errors.InternalError);
        });

        return this;
    }

    commit(cb) {
        this._consumer.commit(cb);
    }

    close() {
        this._consumer.close();
    }
}

module.exports = Subscriber;
