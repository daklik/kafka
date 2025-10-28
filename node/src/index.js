'use strict';

const { StreamsBuilder } = require('./streams-builder');
const { KafkaStreams } = require('./kafka-streams');
const { Serde } = require('./serde');
const state = require('./state');
const { TaskManager } = require('./runtime/task-manager');
const { Named } = require('./named');
const { Materialized } = require('./materialized');
const { StreamsConfig } = require('./config/streams-config');
const { KTable, GlobalKTable } = require('./ktable');
const { Joined } = require('./joined');
const { ValueJoiner } = require('./value-joiner');
const { KGroupedStream } = require('./kgrouped-stream');
const windows = require('./windows');
const metrics = require('./metrics');
const errors = require('./errors');
const query = require('./query');

module.exports = {
  StreamsBuilder,
  KafkaStreams,
  Serde,
  Named,
  Materialized,
  KTable,
  GlobalKTable,
  Joined,
  ValueJoiner,
  KGroupedStream,
  TaskManager,
  StreamsConfig,
  metrics,
  errors,
  query,
  windows,
  JoinWindows: windows.JoinWindows,
  SlidingWindows: windows.SlidingWindows,
  TimeWindows: windows.TimeWindows,
  SessionWindows: windows.SessionWindows,
  UnlimitedWindows: windows.UnlimitedWindows,
  ...state
};
