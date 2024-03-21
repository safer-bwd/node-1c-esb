const { EventEmitter } = require('events');
const fetch = require('node-fetch');
const { Connection: RheaConnection, ConnectionEvents } = require('rhea-promise');
const merge = require('lodash.merge');
const debug = require('debug')('1c-esb');
const ConnectionErrors = require('./errors');

const { ConnectionError, ConnectionAbortError } = ConnectionErrors;

const defaultOpts = {
  // application URL
  // https://[hostname]:[port]/applications/[applicationName]
  url: '',

  // credentionals (oidc)
  clientKey: '',
  clientSecret: '',

  operationTimeoutInSeconds: 60,

  // rhea connection options
  // https://github.com/amqp/rhea#connectoptions
  // https://its.1c.ru/db/esbdoc3/content/20006/hdoc
  amqp: {
    port: 6698,
    max_frame_size: 1000000,
    channel_max: 7000,
    reconnect: {
      reconnect_limit: 10,
      initial_reconnect_delay: 100,
      max_reconnect_delay: 60 * 1000,
    },
  },
};

const sanitizeOptions = (options) => {
  const sanitizedOpts = merge({}, defaultOpts, options);

  if (!sanitizedOpts.amqp) {
    sanitizedOpts.amqp = defaultOpts.amqp;
  }

  if (sanitizedOpts.amqp.reconnect === true) {
    sanitizedOpts.amqp.reconnect = defaultOpts.amqp.reconnect;
  }

  if (!sanitizedOpts.operationTimeoutInSeconds) {
    sanitizedOpts.operationTimeoutInSeconds = defaultOpts.operationTimeoutInSeconds;
  }

  return sanitizedOpts;
};

class Connection extends EventEmitter {
  constructor(options = {}) {
    super();

    this._options = sanitizeOptions(options);

    // application URL
    // https://[hostname]:[port]/applications/[applicationName]
    this._url = new URL(this._options.url);

    // application unique string id per server (part of application URL)
    this._applicationId = this._url.pathname.split('/').pop();

    // application channels map
    this._channels = new Map();

    // rhea connection object
    this._connection = null;

    debug('[%s] initialized with options %o', this.applicationId, this._options);
  }

  get id() {
    if (!this._connection) {
      return '';
    }

    return this._connection.id;
  }

  get applicationId() {
    return this._applicationId;
  }

  get url() {
    return this._url.href;
  }

  get channels() {
    return [...this._channels.values()];
  }

  open(options = {}) {
    if (this.isOpen()) {
      return Promise.resolve(this);
    }

    const onOpen = () => this;
    const onError = (err) => {
      this._closeConnection().catch((error) => {
        this.emit(ConnectionEvents.error, { error });
      });
      throw err;
    };

    return this._openConnection(options)
      .then(onOpen)
      .catch(onError);
  }

  close(options = {}) {
    const onClose = () => this;
    const onError = (error) => {
      this.emit(ConnectionEvents.error, { error });
      throw error;
    };

    return this._closeConnection(options)
      .then(onClose)
      .catch(onError);
  }

  isOpen() {
    return !!(this._connection && this._connection.isOpen());
  }

  isRemoteOpen() {
    return !!(this._connection && this._connection.isRemoteOpen());
  }

  wasCloseInitiated() {
    return !!(this._connection && this._connection.wasCloseInitiated());
  }

  async createSession(options = {}) {
    if (!this.isOpen()) {
      throw new Error('Connection is closed!');
    }

    const session = await this._connection.createSession(options);

    return session;
  }

  removeAllSessions() {
    if (this._connection) {
      this._connection.removeAllSessions();
    }
  }

  getChannel(channelName) {
    return this._channels.get(channelName);
  }

  async createAwaitableSender(channelName, options = {}) {
    if (!this.isOpen()) {
      throw new Error('Connection is closed!');
    }

    const channel = this.getChannel(channelName);
    if (!channel) {
      throw new Error(`Channel '${channelName}' not found`);
    }

    const sender = await this._connection.createAwaitableSender(merge(options, {
      target: { address: channel.destination }
    }));

    return sender;
  }

  async createSender(channelName, options = {}) {
    if (!this.isOpen()) {
      throw new Error('Connection is closed!');
    }

    const channel = this.getChannel(channelName);
    if (!channel) {
      throw new Error(`Channel '${channelName}' not found`);
    }

    const sender = await this._connection.createSender(merge(options, {
      target: { address: channel.destination }
    }));

    return sender;
  }

  async createReceiver(channelName, options = {}) {
    if (!this.isOpen()) {
      throw new Error('Connection is closed!');
    }

    const channel = this.getChannel(channelName);
    if (!channel) {
      throw new Error(`Channel '${channelName}' not found`);
    }

    const receiver = await this._connection.createReceiver(merge(options, {
      source: { address: channel.destination }
    }));

    return receiver;
  }

  async _openConnection(options) {
    let token;

    debug('[%s] connecting...', this.applicationId);

    try {
      token = await this._fetchToken(options);
      const channels = await this._fetchChannels(token, options);
      this._channels = new Map(channels.map((channel) => {
        const channelName = `${channel.process}.${channel.channel}`;
        return [channelName, channel];
      }));
    } catch (error) {
      this.emit(ConnectionEvents.connectionError, { error });
      this.emit(ConnectionEvents.disconnected, { error });
      throw error;
    }

    await this._openRheaConnection(token, options);

    debug('[%s] connected', this.applicationId);
  }

  async _fetchToken(options) {
    const { abortSignal } = options;

    const {
      clientKey, clientSecret, operationTimeoutInSeconds
    } = this._options;

    const authStr = Buffer.from(`${clientKey}:${clientSecret}`, 'utf8').toString('base64');
    const url = `${this._url.origin}/auth/oidc/token`;

    debug('[%s] token: url = %s', this.applicationId, url);
    debug('[%s] token: client key = %s', this.applicationId, clientKey);
    debug('[%s] token: client secret = %s', this.applicationId, clientSecret);

    let response;
    try {
      debug('[%s] token: -> send request', this.applicationId);
      response = await fetch(url, {
        method: 'post',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          Authorization: `Basic ${authStr}`
        },
        body: 'grant_type=client_credentials',
        signal: abortSignal,
        timeout: operationTimeoutInSeconds * 1000,
      });
      debug('[%s] token: <- recieved response (status = %d)', this.applicationId, response.status);
    } catch (error) {
      debug('[%s] token: request error %o', this.applicationId, error);
      if (error.name === 'AbortError') {
        throw new ConnectionAbortError('Abort fetch token!', { url, error });
      } else {
        throw new ConnectionError(`Failed to fetch token: ${error.message}`, { url, error });
      }
    }

    let json;
    try {
      json = await response.json();
    } catch (err) {
      json = {};
    }

    if (response.status !== 200) {
      debug('[%s] token: response error %o', this.applicationId, json);
      const description = `Server returned ${response.status} ${json && json.error && json.error.message}`;
      throw new ConnectionError(`Failed to fetch token: ${description}`, { url, response });
    }

    const { id_token: token } = json;
    debug('[%s] token: %s', this.applicationId, token);

    return token;
  }

  async _fetchChannels(token, options) {
    const { abortSignal } = options;
    const { operationTimeoutInSeconds } = this._options;

    const url = `${this._url.href}/sys/esb/runtime/channels`;

    debug('[%s] channels: url = %s', this.applicationId, url);

    let response;
    try {
      debug('[%s] channels: -> send request', this.applicationId);
      response = await fetch(`${this._url.href}/sys/esb/runtime/channels`, {
        method: 'get',
        headers: { Authorization: `Bearer ${token}` },
        signal: abortSignal,
        timeout: operationTimeoutInSeconds * 1000,
      });
      debug('[%s] channels: <- recieved response (status = %d)', this.applicationId, response.status);
    } catch (error) {
      debug('[%s] channels: request error %o', this.applicationId, error);
      if (error.name === 'AbortError') {
        throw new ConnectionAbortError('Abort fetch remote channels!', { url, error });
      } else {
        throw new ConnectionError(`Failed to fetch remote channels: ${error.message}`, { url, error });
      }
    }

    if (response.status !== 200) {
      debug('[%s] channels: response error', this.applicationId);
      const message = `Failed to fetch remote channels: Server returned ${response.status}`;
      throw new ConnectionError(message, { url, response });
    }

    const json = await response.json();
    const { items } = json;
    debug('[%s] channels: %o', this.applicationId, items);

    return items;
  }

  async _openRheaConnection(token, options) {
    const { operationTimeoutInSeconds } = this._options;

    debug('[%s] amqp: connecting...', this.applicationId);

    this._connection = new RheaConnection({
      ...this._options.amqp,
      host: this._url.hostname,
      vhost: `/applications/${this.applicationId}`,
      username: token,
      password: token,
      reconnect: false, // set reconnect after open connection
      operationTimeoutInSeconds,
    });

    // bind rhea events
    Object.values(ConnectionEvents).forEach((eventName) => {
      this._connection.on(eventName, (...args) => this.emit(eventName, ...args));
    });

    try {
      await this._connection.open(options);
    } catch (error) {
      debug('[%s] amqp: error', this.applicationId, error);
      const message = `Failed to open AMQP connection: ${error.message}`;
      if (error.name === 'AbortError') {
        throw new ConnectionAbortError(message, { error });
      } else {
        throw new ConnectionError(message, { error });
      }
    }

    debug('[%s] amqp: connection (%s) opened', this.applicationId, this._connection.id);

    // set reconnect
    if (this._options.amqp.reconnect) {
      this._connection.options = merge(this._connection.options, this._options.amqp.reconnect);
      this._connection._connection.set_reconnect(true);
      debug('[%s] amqp: set reconnect true', this.applicationId);
    }
  }

  async _closeConnection(options) {
    debug('[%s] connection closing...', this.applicationId);
    await this._closeRheaConnection(options);
    this._channels = new Map();
    debug('[%s] connection closed', this.applicationId);
  }

  async _closeRheaConnection(options) {
    if (this._connection) {
      this._connection._connection.set_reconnect(false); // stop reconnect
      debug('[%s] amqp: set reconnect false', this.applicationId);

      await this._connection.close(options);
      debug('[%s] amqp: connection %s closed', this.applicationId, this._connection.id);
    }
  }
}

module.exports = {
  Connection,
  ConnectionErrors,
  ConnectionEvents,
};
