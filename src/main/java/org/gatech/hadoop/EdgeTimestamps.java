/**
 *  Copyright (c) 2016 Apurv Verma
 */
package org.gatech.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * This class was written for processing http://snap.stanford.edu/data/memetracker9.html
 * It emits the (edge,timestamp) pairs where 'edge' is the concatenation of 2 urls and
 * timestamp is the time when this edge was formed
 * We limit the output to top 500 urls by degree, the top urls are specified as the 3rd 
 * parameter to the program
 */
public class EdgeTimestamps {

  public static class EdgeTimestampMapper 
  extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final static Text pUrl = new Text(); //current p_url
    private final static Text lUrl = new Text(); //current l_url
    private final static LongWritable timestamp = new LongWritable();
    private final static Text edge = new Text();
    private final static Set<String> topUrls = new HashSet<>();
    private final DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    protected void setup(Context context) throws IOException {
      URI[] localPaths = context.getCacheFiles();
      FileSystem fs = FileSystem.get(context.getConfiguration());
      InputStream in = fs.open(new Path(localPaths[0]));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line = null;
      while((line = br.readLine()) != null) {
        String[] arr = line.split("\t");
        topUrls.add(arr[0]);        
      }
    }

    private String extractUrl(String url) {
      String[] arr = url.split("\\?");
      return arr[0];
    }

    private void parseLine(String line) {
      String[] arr = line.split("\t");
      if(arr[0].equals("P")) {
        pUrl.set(extractUrl(arr[1]));
      } else if(arr[0].equals("L")) {
        lUrl.set(extractUrl(arr[1]));
      } else if(arr[0].equals("T")) {
        DateTime dt = f.parseDateTime(arr[1]);
        timestamp.set(dt.getMillis()/1000); //convert to seconds since epoch
      }
    }
    
    /**
     * Constructs a url pair such that the first url is always smaller than 
     * the second url (lexicographically)
     */
    private String constructEdge(String pUrl, String lUrl) {
      if(pUrl.compareTo(lUrl) < 0) {
        return pUrl + "," + lUrl;
      } else {
        return lUrl + "," + pUrl;
      }
    }

    @Override
    public void map(LongWritable key, Text val, Context context) 
        throws IOException, InterruptedException {
      String line = val.toString();
      line = line.trim();
      try{
        if(line.length() > 0) {
          parseLine(line);
        } else {
          pUrl.clear();
          timestamp.set(-1);
        }
      } catch(Exception e) {
        //If there is an error in parsing the time
        e.printStackTrace();
      }

      if(pUrl.toString().length() > 0 &&
          lUrl.toString().length() > 0 && timestamp.get() > -1) {
        //construct edge
        edge.set(constructEdge(pUrl.toString(), lUrl.toString()));
        //Write the results only if both p and l are top urls.
        if(topUrls.contains(pUrl.toString()) 
            && topUrls.contains(lUrl.toString())) {
          context.write(edge, timestamp);
        }
        
        lUrl.clear();
      }

    }
  }

  public static class EdgeTimestampReducer
  extends Reducer<Text, LongWritable, Text, LongWritable> {
    private final static LongWritable timestamp = new LongWritable();

    @Override
    protected void reduce(Text url, Iterable<LongWritable> timestamps, Context context)
        throws IOException, InterruptedException {
      long minTime = Long.MAX_VALUE;
      for(LongWritable t: timestamps) {
        minTime = Math.min(minTime, t.get());
      }
      timestamp.set(minTime);
      context.write(url, timestamp);
    }
  }

  public static void main(String[] args) throws IOException,
  ClassNotFoundException, InterruptedException, URISyntaxException {
    //args = new String[3];
    //args[0] = "/home/dapurv5/Downloads/quotes_2008-08-small.txt.gz";
    //args[1] = "/home/dapurv5/Desktop/hdfs-output/meme_tracker";
    //args[2] = "file:///home/dapurv5/Downloads/meme_tracker/nodes-subset-500.tsv";
    //args[2] = "/home/dapurv5/Downloads/meme_tracker/nodes-subset-500.tsv";

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "edge_timestamps");
    job.setJarByClass(EdgeTimestamps.class);

    //Use this instead of distributed cache
    job.addCacheFile(new URI(args[2]));

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(EdgeTimestampMapper.class);
    job.setReducerClass(EdgeTimestampReducer.class);
    job.setCombinerClass(EdgeTimestampReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    System.exit(job.waitForCompletion(true)?0:1);
  }

}
