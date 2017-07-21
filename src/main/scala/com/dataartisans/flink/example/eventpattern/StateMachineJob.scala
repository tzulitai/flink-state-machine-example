/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.example.eventpattern

import com.dataartisans.flink.example.eventpattern.kafka.EventDeSerializer
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

/**
 * Demo streaming program that receives (or generates) a stream of events and evaluates
 * a state machine (per originating IP address) to validate that the events follow
 * the state machine's rules.
 *
 * Basic invocation line:
 * --input-topic <>
 * --bootstrap.servers localhost:9092
 * --zookeeper.servers localhost:2181 (only needed for older Kafka versions)
 * --checkpointDir <>
 *
 * StateBackend-related options:
 * --stateBackend: file or rocksdb (default: file)
 * --asyncCheckpoints: true or false (only file backend, default: false)
 * --incrementalCheckpoints: true or false (only RocksDB backend, default: false)
 * --externalizedCheckpoints: true or false (default: false)
 * --restartDelay: <int> (default: 0)
 * --checkpointInterval: <int in ms> (default: 5000)
 */
object StateMachineJob {

  def main(args: Array[String]): Unit = {

    // retrieve input parameters
    val pt: ParameterTool = ParameterTool.fromArgs(args)
    
    // create the environment to create streams and configure execution
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(pt.getInt("checkpointInterval", 5000))
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Int.MaxValue, pt.getInt("restartDelay", 0)))
    if (pt.has("externalizedCheckpoints") && pt.getBoolean("externalizedCheckpoints", false)) {
      env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    }

    env.setParallelism(pt.getInt("parallelism", 1))
    env.setMaxParallelism(pt.getInt("maxParallelism", pt.getInt("parallelism", 1)))

    val stateBackend = pt.get("stateBackend", "file")
    val checkpointDir = pt.getRequired("checkpointDir")

    stateBackend match {
      case "file" =>
        val asyncCheckpoints = pt.getBoolean("asyncCheckpoints", false)
        env.setStateBackend(new FsStateBackend(checkpointDir, asyncCheckpoints))
      case "rocksdb" =>
        val incrementalCheckpoints = pt.getBoolean("incrementalCheckpoints", false)
        env.setStateBackend(new RocksDBStateBackend(checkpointDir, incrementalCheckpoints))
    }

    val stream = env.addSource(
      new FlinkKafkaConsumer010[Event](
        pt.getRequired("input-topic"), new EventDeSerializer(), pt.getProperties))

    val alerts = stream
      // partition on the address to make sure equal addresses
      // end up in the same state machine flatMap function
      .keyBy("sourceAddress")
      
      // the function that evaluates the state machine over the sequence of events
      .flatMap(new StateMachineMapper())


    // if we get any alert, fail
    alerts.flatMap { any =>
      throw new RuntimeException(s"Got an alert: $any.")
      "Make type checker happy."
    }
    

    // trigger program execution
    env.execute()
  }
}

/**
 * The function that maintains the per-IP-address state machines and verifies that the
 * events are consistent with the current state of the state machine. If the event is not
 * consistent with the current state, the function produces an alert.
 */
class StateMachineMapper extends RichFlatMapFunction[Event, Alert] {
  
  private[this] var currentState: ValueState[State] = _
    
  override def open(config: Configuration): Unit = {
    currentState = getRuntimeContext.getState(
      new ValueStateDescriptor("state", classOf[State], InitialState))
  }
  
  override def flatMap(t: Event, out: Collector[Alert]): Unit = {
    val state = currentState.value()
    val nextState = state.transition(t.event)
    
    nextState match {
      case InvalidTransition =>
        out.collect(Alert(t.sourceAddress, state, t.event))
      case x if x.terminal =>
        currentState.clear()
      case x =>
        currentState.update(nextState)
    }
  }
}
