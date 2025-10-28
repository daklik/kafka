'use strict';

const HandlerAction = Object.freeze({
  FAIL: 'FAIL',
  CONTINUE: 'CONTINUE'
});

class ExceptionHandler {
  // eslint-disable-next-line class-methods-use-this
  async handle() {
    return HandlerAction.FAIL;
  }
}

class LogAndFailExceptionHandler extends ExceptionHandler {
  constructor(logger = console) {
    super();
    this.logger = logger;
  }

  async handle(context) {
    this.logger.error?.('[KafkaStreams][error]', context?.stage ?? 'unknown', context?.error);
    return HandlerAction.FAIL;
  }
}

class LogAndContinueExceptionHandler extends ExceptionHandler {
  constructor(logger = console) {
    super();
    this.logger = logger;
  }

  async handle(context) {
    this.logger.warn?.('[KafkaStreams][warn]', context?.stage ?? 'unknown', context?.error);
    return HandlerAction.CONTINUE;
  }
}

function createExceptionHandler(handler) {
  if (!handler) {
    return null;
  }
  if (handler instanceof ExceptionHandler) {
    return handler;
  }
  if (typeof handler === 'function') {
    if (handler.prototype && typeof handler.prototype.handle === 'function') {
      return new handler();
    }
    const fn = handler;
    return {
      async handle(context) {
        return fn(context);
      }
    };
  }
  if (typeof handler.handle === 'function') {
    return handler;
  }
  return null;
}

module.exports = {
  ExceptionHandler,
  LogAndFailExceptionHandler,
  LogAndContinueExceptionHandler,
  HandlerAction,
  createExceptionHandler
};
