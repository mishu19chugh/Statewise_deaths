package com.Statewise_deaths;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.mortbay.util.StringUtil;

public class WC_Mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
{


    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
                    Reporter reporter) throws IOException {

        String valueString = value.toString();

        if (valueString.startsWith("date")) {
            // Line is the header, ignore it
            return;
        }

        String[] DeathsData=valueString.split(",");
        /*int death= Integer.parseInt(DeathsData[2]);*/
        int death=StringUtils.isNotBlank(DeathsData[2])?Integer.parseInt(DeathsData[2]):0;
        output.collect(new Text(DeathsData[1]),new IntWritable(death));
    }
}
