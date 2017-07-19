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
import grizzled.slf4j.Logger
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
//  @transient var state: ListState[KeyRange]

  override def cancel(): Unit = {
    running = false
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {

    @transient var log = Logger(getClass)

    if (!context.isRestored) {
      // initialize our initial operator state based on the number of keys and the parallelism
      val subtaskIndex = getRuntimeContext.getIndexOfThisSubtask
      val numSubtasks = getRuntimeContext.getMaxNumberOfParallelSubtasks

      // create numSubtasks * 2 operator states so that we can scale a bit
      val numKeyRanges = numSubtasks * 2
      // we might not get exactly the number of requested keys because of this, doesn't matter...
      val keysPerKeyRange = numKeyRanges / numKeys

      // which are our key ranges
      localKeyRanges = 0.to(numKeyRanges)
        .filter { range => range % numSubtasks == subtaskIndex }
        .map { rangeIndex =>
          val startIndex = rangeIndex * keysPerKeyRange
          val endIndex = rangeIndex * keysPerKeyRange + keysPerKeyRange
          val states: Seq[(Int, State)] = startIndex.to(endIndex).map(i => i -> InitialState)
          KeyRange(startIndex, endIndex, scala.collection.mutable.Map[Int, State](states: _*))
        }

      log.info("Event source {}/{} has key ranges {}.", subtaskIndex, numSubtasks, localKeyRanges)
    }

  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
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
        }
        ctx.collect(Event(key, nextEvent))
      }

      val p = rnd.nextDouble()

    }
  }
}

