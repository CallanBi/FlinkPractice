package com.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketTextStreamWordCount {
  public static void main(String[] args) throws Exception {
    // check that the port is set
    if (args.length != 2) {
      System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
      return;
    }

    String hostname = args[0];
    int port = Integer.parseInt(args[1]);


    // set up the streaming execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // get input data
    // Creates a new data stream that contains the strings received infinitely from a socket. Received strings are decoded by the system's default character set, using"\n" as delimiter. The reader is terminated immediately when the socket is down.
    DataStreamSource<String> stream = env.socketTextStream(hostname, port);

    // parse the data, group it, window it, and aggregate the counts
    // key by the first letter of the string, then count the words
    // the keyBy(0) is deprecated in the new version of Flink, so we use the new keySelector() instead
    SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter()).keyBy(new KeySelect()).sum(1);
    sum.print();
    env.execute("Java WordCount from SocketTextStream Example");
  }


  public static final class KeySelect implements KeySelector<Tuple2<String, Integer>, String> {
    @Override
    public String getKey(Tuple2<String, Integer> in) throws Exception {
      // return the first position of the tuple so that the keyBy operation can group the line by the same word
      return in.f0;
    }
  }

  public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
      // find all words in the line
      String[] tokens = s.toLowerCase().split("\\W+");

      for (String token : tokens) {
        if (token.length() > 0) {
          // emit the word and 1 as a Tuple2 for the count
          collector.collect(new Tuple2<String, Integer>(token, 1));
        }
      }
    }
  }
}