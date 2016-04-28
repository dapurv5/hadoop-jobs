/**
 *  Copyright (c) 2016 Apurv Verma
 */
package org.gatech.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class was written for processing http://snap.stanford.edu/data/memetracker9.html
 * It emits the (u,d) pairs where 'u' is the primary url obtained by stripping off all params
 * from the url and 'd' is the degree of the node.
 */
public class NodeDegree {

  public static class UrlCountMapper 
  extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static Text pUrl = new Text(); //current p_url
    private final static Text lUrl = new Text(); //current l_url
    private final static IntWritable unity = new IntWritable(1);

    private String extractUrl(String url) throws URISyntaxException {
      URI uri = new URI(url);
      String domain = uri.getHost();
      return domain.startsWith("www.") ? domain.substring(4) : domain;
    }

    private void parseLine(String line) throws URISyntaxException {
      String[] arr = line.split("\t");
      if(arr[0].equals("P")) {
        pUrl.set(extractUrl(arr[1]));
      } else if(arr[0].equals("L")) {
        lUrl.set(extractUrl(arr[1]));
      }      
    }

    @Override
    public void map(LongWritable key, Text val, Context context) 
        throws IOException, InterruptedException {
      String line = val.toString();
      try{
        if(line.length() > 0) {
          parseLine(line);
        } else {
          pUrl.clear();
        }
      } catch(URISyntaxException e) {
        //If there is an error in parsing the url
        context.getCounter(Stats.BAD_URL).increment(1);
        e.printStackTrace();
        return;
      } catch(IllegalStateException e) {
        //If there is an error in parsing the time
        context.getCounter(Stats.BAD_TIME).increment(1);
        e.printStackTrace();
        return;
      } catch(Exception e) {
        //If there is some other kind of error
        context.getCounter(Stats.BAD_MISC).increment(1);
        e.printStackTrace();
        return;
      }

      if(pUrl.toString().length() > 0 && lUrl.toString().length() > 0) {
        //increment degree for each page by one
        context.write(pUrl, unity);
        context.write(lUrl, unity);
        lUrl.clear();
      }

    }
  }

  public static class UrlCountReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final static IntWritable degree = new IntWritable();

    @Override
    protected void reduce(Text url, Iterable<IntWritable> degrees, Context context)
        throws IOException, InterruptedException {
      int sumDegrees = 0;
      for(IntWritable adj: degrees) {
        sumDegrees += adj.get();
      }
      degree.set(sumDegrees);
      context.write(url, degree);
    }
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    //args = new String[2];
    //args[0] = "/home/dapurv5/Downloads/quotes_2008-08-small.txt.gz";
    //args[1] = "/home/dapurv5/Desktop/hdfs-output/meme_tracker";

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "node_degree");
    job.setJarByClass(NodeDegree.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(UrlCountMapper.class);
    job.setReducerClass(UrlCountReducer.class);
    job.setCombinerClass(UrlCountReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    System.exit(job.waitForCompletion(true)?0:1);
  }

}
