package org.apache.flink.streaming.scala.examples

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object wordCount {

  def main(args: Array[String]): Unit ={
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // Set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // Get input data
    val text =
      //read the text file from given input path
    if(params.has("input")) env.readTextFile(params.get("input")) else{
      println("Executing WordCount example with default inputs data et.")
      println("Use --input to specify file input.")

      //get default test text data
      env.fromElements(WordCountData.WORDS:_*)
    }

    val counts = text
    // Split up the lines in pairs (2-tuples) containing: (word, 1)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))

    //group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
      .sum(1)

    // emit result
    if(params.has("output")) counts.writeAsText(params.get("output")) else{
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    // execute program
    env.execute("Streaming WordCount")

  }
}
