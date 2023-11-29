class IConnectionError extends Error {
  constructor(message, data) {
    super(message);
    this.data = data;
    Error.captureStackTrace(this, this.constructor);
  }
}

class ConnectionError extends IConnectionError {
  constructor(message, data) {
    super(message || 'Connection error!', data);
    this.name = 'ConnectionError';
  }
}

class ConnectionAbortError extends IConnectionError {
  constructor(message, data) {
    super(message || 'Connection abort error!', data);
    this.name = 'ConnectionAbortError';
  }
}

module.exports = {
  ConnectionError,
  ConnectionAbortError,
};
