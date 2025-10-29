'use strict';

const { StreamsBuilder } = require('./streams-builder');
const { KafkaStreams } = require('./kafka-streams');
const { Serde } = require('./serde');
const state = require('./state');
const { TaskManager, TaskState } = require('./runtime/task-manager');
const { StateStoreManager } = require('./runtime/state-store-manager');
const { StreamThread } = require('./runtime/stream-thread');
const { TransactionManager } = require('./runtime/transaction-manager');
const assignment = require('./runtime/assignment-coordinator');
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
const { Suppressed } = require('./suppressed');
const processor = require('./processor');

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
  TaskState,
  StateStoreManager,
  StreamThread,
  TransactionManager,
  StreamsConfig,
  metrics,
  errors,
  query,
  windows,
  Suppressed,
  assignment,
  BufferConfig: Suppressed.BufferConfig,
  JoinWindows: windows.JoinWindows,
  SlidingWindows: windows.SlidingWindows,
  TimeWindows: windows.TimeWindows,
  SessionWindows: windows.SessionWindows,
  UnlimitedWindows: windows.UnlimitedWindows,
  processor,
  Processor: processor.Processor,
  ProcessorSupplier: processor.ProcessorSupplier,
  Transformer: processor.Transformer,
  ValueTransformer: processor.ValueTransformer,
  ProcessorContext: processor.ProcessorContext,
  RecordContext: processor.RecordContext,
  ForwardingDisabledException: processor.ForwardingDisabledException,
  To: processor.To,
  PunctuationType: processor.PunctuationType,
  ...state
};
