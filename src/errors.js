class ExtendableError extends Error {
  constructor(message, data) {
    super(message);
    this.name = 'ExtendableError';
    this.data = data;
    Error.captureStackTrace(this, this.constructor);
  }
}

class ConnectionError extends ExtendableError {
  constructor(...args) {
    super(...args);
    this.name = 'ConnectionError';
  }
}

class ConnectionFatalError extends ExtendableError {
  constructor(...args) {
    super(...args);
    this.name = 'ConnectionFatalError';
  }
}

module.exports = {
  ConnectionError,
  ConnectionFatalError,
};
