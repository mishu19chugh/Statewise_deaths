package com.Statewise_deaths;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WC_Reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text state, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {

             Text key = state;

        int death=0;
        while(values.hasNext()){
            death=death+values.next().get();
        }
            output.collect(key, new IntWritable(death));
    }
}