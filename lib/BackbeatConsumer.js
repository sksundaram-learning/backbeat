const { EventEmitter } = require('events');
const { ConsumerGroup } = require('kafka-node');
const kafkaLogging = require('kafka-node/logging');

const async = require('async');
const Logger = require('werelogs').Logger;
kafkaLogging.setLoggerProvider(new Logger('Consumer'));
const CONCURRENCY = 1;

class BackbeatConsumer extends EventEmitter {

    /**
    * constructor
    * @param {Object} config - config
    * @param {Object} config.zookeeper - zookeeper endpoint config
    * @param {string} config.zookeeper.host - zookeeper host
    * @param {number} config.zookeeper.port - zookeeper port
    * @param {boolean} [config.ssl] - ssl enabled if ssl === true
    * @param {string} [config.groupId] - consumer group id. Messages are
    ( distributed among multiple consumers belonging to the same group
    * @param {string} [config.fromOffset] - valid values latest/earliest/none
    * @param {function} config.queueProcessor - function to invoke to process
    * an item in a queue
    * @param {number} [config.concurrency] - represents the number of entries
    * that can be processed in parallel
    * @param {number} [config.fetchMaxBytes] - max. bytes to fetch in a
    * fetch loop
    * @param {object} config.log.logLevel - default log level
    * @param {object} config.log.dumpLevel - dump level for logger
    */
    constructor(config) {
        super();
        const { zookeeper, ssl, groupId, fromOffset, topic, queueProcessor,
            concurrency, fetchMaxBytes, log } = config;
        this._zookeeperEndpoint = `${zookeeper.host}:${zookeeper.port}`;
        this._log = new Logger('BackbeatConsumer', {
            level: log.logLevel,
            dump: log.dumpLevel,
        });
        this._topic = topic;
        this._groupId = groupId;
        this._queueProcessor = queueProcessor;
        this._concurrency = concurrency === undefined ?
            CONCURRENCY : concurrency;
        this._messagesConsumed = 0;
        this._consumer = new ConsumerGroup({
            host: this._zookeeperEndpoint,
            ssl,
            groupId: this._groupId,
            fromOffset,
            autoCommit: false,
            fetchMaxBytes,
        }, this._topic);
        this._consumer.on('connect', () => this.emit('connect'));
        return this;
    }

    /**
    * subscribe to messages from a topic
    * Once subscribed, the consumer does a fetch from the topic with new
    * messages. Each fetch loop can contain one or more messages, so the fetch
    * is paused until the current queue of tasks are processed. Once the task
    * queue is empty, the current offset is committed and the fetch is resumed
    * to get the next batch of messages
    * @return {this} current instance
    */
    subscribe() {
        const q = async.queue(this._queueProcessor, this._concurrency);
        let partition = null;
        let offset = null;
        // consume a message in the fetch loop
        this._consumer.on('message', entry => {
            partition = entry.partition;
            offset = entry.offset;
            this._messagesConsumed++;
            this._consumer.pause();
            q.push(entry, err => {
                if (err) {
                    this._log.error('error processing an entry', {
                        error: err,
                        method: 'BackbeatConsumer.subscribe',
                        partition,
                        offset,
                    });
                    this.emit('failedProcessing', entry);
                }
            });
        });

        // commit offset and resume fetch loop when the task queue is empty
        q.drain = () => {
            const count = this._messagesConsumed;
            this._consumer.sendOffsetCommitRequest([{
                topic: this._topic,
                partition, // default 0
                offset,
                metadata: 'm', //default 'm'
            }], () => {
                this.emit('consumed', count);
                this._messagesConsumed = 0;
                this._consumer.resume();
            });
        };

        this._consumer.on('error', error => {
            this._consumer.pause();
            this._log.error('error subscribing to topic', {
                error,
                method: 'BackbeatConsumer.subscribe',
                topic: this._topic,
                partition: this._partition,
            });
            this.emit('error', error);
        });

        return this;
    }

    /**
    * manually commit the current offset if auto-commit is disabled
    * @param {callback} cb - callback to invoke
    * @return {undefined}
    */
    close(cb) {
        this._consumer.close(true, cb);
    }
}

module.exports = BackbeatConsumer;
