const { EventEmitter } = require('events');
const { StatusCodes: HttpStatusCodes } = require('http-status-codes');
const fetch = require('node-fetch');
const { Connection: RheaConnection, ConnectionEvents: RheaConnectionEvents } = require('rhea-promise');
const { ConnectionError, ConnectionFatalError } = require('./errors');
const { backOff, merge, get } = require('./utils');

if (!global.AbortController) {
  // eslint-disable-next-line global-require
  global.AbortController = require('node-abort-controller').AbortController;
}

const retryableHttpStatusCodes = [
  HttpStatusCodes.REQUEST_TIMEOUT,
  HttpStatusCodes.TOO_MANY_REQUESTS,
  HttpStatusCodes.INTERNAL_SERVER_ERROR,
  HttpStatusCodes.BAD_GATEWAY,
  HttpStatusCodes.SERVICE_UNAVAILABLE,
  HttpStatusCodes.GATEWAY_TIMEOUT,
];

const defaultReconnectOpts = {
  initialDelay: 100,
  maxDelay: 60 * 1000,
  limit: 10,
};

const defaultOperationTimeoutInSeconds = 1;

const defaultOpts = {
  url: '',
  clientKey: '',
  clientSecret: '',

  reconnect: defaultReconnectOpts,

  operationTimeoutInSeconds: defaultOperationTimeoutInSeconds,

  // https://github.com/amqp/rhea#connectoptions
  amqp: {
    // https://its.1c.ru/db/esbdoc3/content/20006/hdoc
    port: 6698,
    max_frame_size: 1000000,
    channel_max: 7000,
  },
};

const sanitizeOptions = (options) => {
  const sanitizedOpts = merge(defaultOpts, options);

  if (sanitizedOpts.reconnect) {
    if (typeof sanitizedOpts.reconnect === 'object') {
      sanitizedOpts.reconnect = merge(defaultReconnectOpts, sanitizedOpts.reconnect);
    } else {
      sanitizedOpts.reconnect = defaultReconnectOpts;
    }
  }

  if (!sanitizedOpts.amqp) {
    sanitizedOpts.amqp = {};
  }

  if (!sanitizedOpts.operationTimeoutInSeconds) {
    sanitizedOpts.operationTimeoutInSeconds = defaultOperationTimeoutInSeconds;
  }

  return sanitizedOpts;
};

class Connection extends EventEmitter {
  constructor(options = {}) {
    super();

    this._options = sanitizeOptions(options);

    this._url = new URL(this._options.url);
    this._application = this._url.pathname.split('/').pop();
    this._token = '';
    this._channels = [];

    this._connection = this._createRheaConnection();
    this._bindRheaEvents();

    this._abortController = null;
  }

  get application() {
    return this._application;
  }

  get url() {
    return this._url.href;
  }

  open() {
    if (this.isOpen()) {
      return Promise.resolve(this);
    }

    this._abortController = new AbortController();
    const { signal: abortSignal } = this._abortController;

    const onOpen = () => {
      this._abortController = null;
      return Promise.resolve(this);
    };

    if (!this._options.reconnect) {
      return this._connect(abortSignal).then(onOpen);
    }

    return backOff(() => this._connect(abortSignal), {
      delayFirstAttempt: false,
      startingDelay: this._options.reconnect.initialDelay,
      maxDelay: this._options.reconnect.maxDelay,
      numOfAttempts: this._options.reconnect.limit,
      retry: (err) => !(err instanceof ConnectionFatalError),
    }).then(onOpen);
  }

  close() {
    if (this._abortController) {
      this._abortController.abort();
    }

    const onClose = () => {
      this._token = '';
      this._channels = [];
      return Promise.resolve(this);
    };

    return this._connection.close().then(onClose);
  }

  isOpen() {
    return this._connection.isOpen();
  }

  createSession(options) {
    return this._connection.createSession(options);
  }

  findRemoteChannel(processName, channelName) {
    this._findChannel(processName, channelName);
  }

  async createAwaitableSender(processName, channelName, options = {}) {
    const channel = this._findChannel(processName, channelName);
    if (!channel) {
      throw new ConnectionError(`Channel '${processName}' for process '${processName}' not found`, {
        process: processName, channel: channelName
      });
    }

    const sender = await this._connection.createAwaitableSender(merge(options, {
      target: { address: channel.destination }
    }));

    return sender;
  }

  async createSender(processName, channelName, options = {}) {
    const channel = this._findChannel(processName, channelName);
    if (!channel) {
      throw new ConnectionError(`Channel '${processName}' for process '${processName}' not found`, {
        process: processName, channel: channelName
      });
    }

    const sender = await this._connection.createSender(merge(options, {
      target: { address: channel.destination }
    }));

    return sender;
  }

  async createReceiver(processName, channelName, options = {}) {
    const channel = this._findChannel(processName, channelName);
    if (!channel) {
      throw new ConnectionError(`Channel '${channelName}' for process '${processName}' not found`, {
        process: processName, channel: channelName
      });
    }

    const receiver = await this._connection.createReceiver(merge(options, {
      source: { address: channel.destination }
    }));

    return receiver;
  }

  async _connect(abortSignal) {
    await this._auth(abortSignal);
    await this._openRheaConnection(abortSignal);
  }

  async _auth(abortSignal) {
    try {
      this._token = await this._getToken(abortSignal);
      this._channels = await this._getChannels(abortSignal);
    } catch (error) {
      if (error instanceof ConnectionFatalError) {
        this.emit(RheaConnectionEvents.connectionError, { error });
        this.emit(RheaConnectionEvents.connectionClose, { error });
      } else {
        this.emit(RheaConnectionEvents.disconnected, { error });
      }
      throw error;
    }
  }

  async _getToken(abortSignal) {
    const { clientKey, clientSecret } = this._options;
    const authStr = Buffer.from(`${clientKey}:${clientSecret}`, 'utf8').toString('base64');

    let response;
    try {
      response = await fetch(`${this._url.origin}/auth/oidc/token`, {
        method: 'post',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          Authorization: `Basic ${authStr}`
        },
        body: 'grant_type=client_credentials',
        signal: abortSignal,
        timeout: this._options.operationTimeoutInSeconds * 1000,
      });
    } catch (error) {
      const message = `Failed to get token: ${error.message}`;
      if (error.name === 'AbortError') {
        throw new ConnectionFatalError(message, { error });
      } else {
        throw new ConnectionError(message, { error });
      }
    }

    const data = await response.json();

    if (response.status !== HttpStatusCodes.OK) {
      const description = get(data, 'error.message', `Server returned ${response.status}`);
      const message = `Failed to get token: ${description}`;
      if (retryableHttpStatusCodes.includes(response.status)) {
        throw new ConnectionError(message, { body: data, status: response.status });
      } else {
        throw new ConnectionFatalError(message, { body: data, status: response.status });
      }
    }

    const { id_token: token } = data;

    return token;
  }

  async _getChannels(abortSignal) {
    let response;
    try {
      response = await fetch(`${this._url.href}/sys/esb/runtime/channels`, {
        method: 'get',
        headers: { Authorization: `Bearer ${this._token}` },
        signal: abortSignal,
        timeout: this._options.operationTimeoutInSeconds * 1000,
      });
    } catch (error) {
      const message = `Failed to get token: ${error.message}`;
      if (error.name === 'AbortError') {
        throw new ConnectionFatalError(message, { error });
      } else {
        throw new ConnectionError(message, { error });
      }
    }

    if (response.status !== HttpStatusCodes.OK) {
      const message = `Failed to get remote channels: Server returned ${response.status}`;
      if (retryableHttpStatusCodes.includes(response.status)) {
        throw new ConnectionError(message, { status: response.status });
      } else {
        throw new ConnectionFatalError(message, { status: response.status });
      }
    }

    const { items } = await response.json();

    return items;
  }

  _findChannel(processName, channelName) {
    const predicate = (it) => it.process === processName && it.channel === channelName;
    return this._channels.find(predicate);
  }

  _createRheaConnection() {
    const connection = new RheaConnection({
      ...this._options.amqp,
      host: this._url.hostname,
      vhost: `/applications/${this.application}`,
      reconnect: false,
      defaultOperationTimeoutInSecond: this._options.defaultOperationTimeoutInSecond,
    });

    return connection;
  }

  _openRheaConnection(abortSignal) {
    this._connection.options.username = this._token;
    this._connection.options.password = this._token;

    return new Promise((resolve, reject) => {
      let isClosed = false;
      const onClose = () => { isClosed = true; };

      this._connection.once(RheaConnectionEvents.connectionClose, onClose);

      this._connection.open({ abortSignal })
        .then(resolve)
        .catch((error) => {
          const message = `Failed to open AMQP connection: ${error.message}`;
          if (isClosed || error.name === 'AbortError') {
            reject(new ConnectionFatalError(message, { error }));
            if (error.name === 'AbortError') {
              this.emit(RheaConnectionEvents.connectionError, { error });
              this.emit(RheaConnectionEvents.connectionClose, { error });
            }
          } else {
            reject(new ConnectionError(message, { error }));
          }
        })
        .finally(() => {
          this._connection.removeListener(RheaConnectionEvents.connectionClose, onClose);
        });
    });
  }

  _bindRheaEvents() {
    Object.values(RheaConnectionEvents).forEach((eventName) => {
      this._connection.on(eventName, (...args) => this.emit(eventName, ...args));
    });
  }
}

module.exports = {
  Connection,
  ConnectionEvents: RheaConnectionEvents
};
