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

import java.util.UUID

import com.dataartisans.flink.example.eventpattern.kafka.EventDeSerializer
import grizzled.slf4j.Logger
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.util.XORShiftRandom


/**
  * This Flink job runs the following:
  * The EventsGeneratorSource from the state machine
  * an operator that adds "event time" to the stream (just counting elements)
  * a Kafka sink writing the resulting stream to a topic
  *
  * Local invocation line: --numKeys <> --topic statemachine --bootstrap.servers localhost:9092
 */
object DataGeneratorJob {

  def main(args: Array[String]): Unit = {
    // retrieve input parameters
    val pt: ParameterTool = ParameterTool.fromArgs(args)

    // create the environment to create streams and configure execution
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(pt.getInt("parallelism", 16))

    // this statement enables the checkpointing mechanism with an interval of 5 sec
    env.enableCheckpointing(pt.getInt("checkpointInterval", 5000))

    val stream = env.addSource(new KeyedEventsGeneratorSource(pt.getInt("numKeys", 200)))

    stream.addSink(
      new FlinkKafkaProducer09[Event](
        pt.getRequired("topic"), new EventDeSerializer(), pt.getProperties))

    // trigger program execution
    env.execute("Kafka events generator")
  }
}

class KeyedEventsGeneratorSource(numKeys: Int)
  extends RichParallelSourceFunction[Event] with CheckpointedFunction {

  @transient var log = Logger(getClass)

  // startKey is inclusive, endKey is exclusive
  case class KeyRange(startKey: Int, endKey: Int, keyState: scala.collection.mutable.Map[Int, State])

  var running = true
  val rnd = new XORShiftRandom()

  @transient var localKeyRanges: Seq[KeyRange] = Seq()
  @transient var keyPrefix: String = UUID.randomUUID().toString

  override def cancel(): Unit = {
    running = false
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {

    log = Logger(getClass)

    keyPrefix = UUID.randomUUID().toString

    // we always initialize from zero, never snapshot state

    // initialize our initial operator state based on the number of keys and the parallelism
    val subtaskIndex = getRuntimeContext.getIndexOfThisSubtask
    val numSubtasks = getRuntimeContext.getNumberOfParallelSubtasks

    // create maxSubtasks/numSubtasks operator states so that we can scale a bit
    val numKeyRanges = getRuntimeContext.getMaxNumberOfParallelSubtasks / numSubtasks
    // we might not get exactly the number of requested keys because of this, doesn't matter...
    val keysPerKeyRange = Math.ceil(numKeys / numKeyRanges.toFloat).toInt

    println(s"KEY STATS $numKeyRanges $numKeys $keysPerKeyRange")

    // which are our key ranges
    localKeyRanges = 0.until(numKeyRanges)
      .filter { range => range % numSubtasks == subtaskIndex }
      .map { rangeIndex =>
        val startIndex = rangeIndex * keysPerKeyRange
        val endIndex = rangeIndex * keysPerKeyRange + keysPerKeyRange
        println(s"START $startIndex until $endIndex")
        val states: Seq[(Int, State)] = startIndex.until(endIndex).map(i => i -> InitialState)
        KeyRange(startIndex, endIndex, scala.collection.mutable.Map[Int, State](states: _*))
      }

    log.info(s"Event source $subtaskIndex/$numSubtasks has key ranges $localKeyRanges.")

  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    // we don't actually checkpoint but restart fresh on every run with a new key prefix
    // this avoids breaking a downstream state machine because we might re-emit events to Kafka
    // No exactly-once producing yet ... :-(
  }

  override def run(ctx: SourceContext[Event]): Unit = {

    while(running) {
      val keyRangeIndex = rnd.nextInt(localKeyRanges.size)
      val keyRange = localKeyRanges(keyRangeIndex)
      val key = rnd.nextInt(keyRange.endKey - keyRange.startKey) + keyRange.startKey

      ctx.getCheckpointLock.synchronized {
        val keyState = keyRange.keyState.get(key)
        val (nextEvent, newState) = keyState.get.randomTransition(rnd)
        if (newState == TerminalState) {
          keyRange.keyState += (key -> InitialState)
        } else {
          keyRange.keyState += (key -> newState)
        }
        ctx.collect(Event(keyPrefix + "##" + key, nextEvent))
      }
    }
  }
}

