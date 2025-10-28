'use strict';

const { JoinWindows } = require('./join-windows');
const { SlidingWindows } = require('./sliding-windows');
const { TimeWindows } = require('./time-windows');
const { SessionWindows } = require('./session-windows');
const { UnlimitedWindows } = require('./unlimited-windows');

module.exports = {
  JoinWindows,
  SlidingWindows,
  TimeWindows,
  SessionWindows,
  UnlimitedWindows
};
