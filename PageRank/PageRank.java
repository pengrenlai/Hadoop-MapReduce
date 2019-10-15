package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.*;

public class PageRank {
    
    public static class Mapper1
        extends Mapper<Object, Text, Text, Text>{

    private Text src = new Text();
    private Text dest = new Text();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        /*(col, row)*/
        while (itr.hasMoreTokens()) {
        String tokens[] = itr.nextToken().split("\\s+");
        src.set(tokens[0]);
        dest.set(tokens[1]);
        context.write(src, dest);
        }
    }
}

/*initialization.  Prepare initial values for matrix M and rank*/
public static class Reducer1
        extends Reducer<Text,Text,Text,Text> {
    
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private Text outputRankKey = new Text();
    private Text outputRankValue = new Text();
    
    private boolean isRankAdded = false;
    private int num_v = 10876;
    private double init = (double)1 / num_v;
    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
        if(!isRankAdded)
        {
            for(int i = 0 ;i < num_v ; i++)
            {
                outputRankKey.set(Integer.toString(i));
                outputRankValue.set("r," + Double.toString(init));
                context.write(outputRankKey, outputRankValue);
            }
            isRankAdded = true;
        }
        String list = "";
        for(Text val : values)
            list = list + val.toString() + " ";
            
        StringTokenizer itr = new StringTokenizer(list, " ");
        double v = (double)1 / itr.countTokens();
        double beta = 0.8;
        v = v * beta;
        while (itr.hasMoreTokens())
        {
            outputValue.set("M," + itr.nextToken() + "," + Double.toString(v));
            context.write(key, outputValue);
        } 
    }
}

/*rank multiply every element in the corresponding column*/
public static class Reducer2
        extends Reducer<Text,Text,Text,Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
                        
        ArrayList<String> matrixValue = new ArrayList<String>();
        double rank = 0.0;    
        for(Text val : values)
        {
            String tokens[] = val.toString().split(",");
            if(tokens[0].equals("r"))
              rank = Double.parseDouble(tokens[1]); // initial rank 1/N,  N is the number of vertex
            else
            {
              matrixValue.add(val.toString()); // store (M,node,1/d)
              context.write(key, val); //need to record (beta * M) matrix again
            }
        }
        
        for(int i = 0 ; i < matrixValue.size() ; i++)
        {
            String tokens[] = matrixValue.get(i).split(",");
            double r = (double)(rank * Double.parseDouble(tokens[2])); // (initial rank) * ( every element of matrix (beta*M) )
            outputKey.set(tokens[1]);
            outputValue.set("r," + Double.toString(r)); //  for all values with "r", if the values' keys are the same, then they should be added together
            context.write(outputKey, outputValue);
        }
    }
}

/*sum all terms of inner product calculation up      and      write redundant ("sum", rank) for S calculation*/
public static class Reducer3
        extends Reducer<Text,Text,Text,Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
        
        double rank = 0.0;  // rank is the outcome of adding all terms of inner product
        for(Text val : values)
        {
            String tokens[] = val.toString().split(",");
            if(tokens[0].equals("r"))
                rank += Double.parseDouble(tokens[1]);
            else
                context.write(key, val);
        }
        
        outputValue.set("r," + Double.toString(rank));
        
        context.write(key, outputValue);
        outputKey.set("sum");
        context.write(outputKey, outputValue);
    }
}

/*calculate renormalization term*/
public static class Reducer4
        extends Reducer<Text,Text,Text,Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {

        double rank_sum = 0.0;
        
        for(Text val : values)
        {
          String tokens[] = val.toString().split(",");
          if(key.toString().equals("sum"))
            rank_sum += Double.parseDouble(tokens[1]);
          else
            context.write(key, val);
        }
        
        if(key.toString().equals("sum"))
        {
          int num_v = 10876;
          double re_norm = (double)((1 - rank_sum) / num_v);
          for(int i = 0 ; i < num_v ; i++)
          {
            outputKey.set(Integer.toString(i));
            outputValue.set("n," + Double.toString(re_norm)); // record renormalization terms
            context.write(outputKey, outputValue);
          }  
        }
    }
}
/*update rank*/
public static class Reducer5
        extends Reducer<Text,Text,Text,Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
        
        double rank = 0.0;
        double re_norm = 0.0;
        for(Text val : values)
        {
          String tokens[] = val.toString().split(",");
          if(tokens[0].equals("M"))
            context.write(key, val);
          else if(tokens[0].equals("r"))
            rank = Double.parseDouble(tokens[1]);
          else
            re_norm = Double.parseDouble(tokens[1]);
        }
        
        rank += re_norm; // update to get the new r
        
        outputValue.set("r," + Double.toString(rank));
        context.write(key, outputValue);
    }
}
/**/
public static class Mapper2
        extends Mapper<Object, Text, Text, Text>{

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        
        while (itr.hasMoreTokens()) {
            String tokens[] = itr.nextToken().split("\\s+");
            String values[] = tokens[1].split(",");
            outputKey.set(values[0]);
            outputValue.set(tokens[0] + "," + values[1]);
            context.write(outputKey, outputValue);
          }
        }
}


/*extract rank data to form the correct output file*/
public static class Reducer6
        extends Reducer<Text,Text,Text,Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
        
        if(key.toString().equals("r"))
        {
          Map<Double, String> map = new TreeMap<Double, String>(Collections.reverseOrder());
          for(Text val : values)
          {
              if(key.toString().equals("r"))
              {
                String tokens[] = val.toString().split(",");
                map.put(Double.parseDouble(tokens[1]), tokens[0]);
              } 
          }
        
          int i = 0;
          for(Map.Entry<Double, String> entry : map.entrySet())
          {
            if(i >= 10)
              break;
            outputKey.set(entry.getValue());
            outputValue.set(Double.toString(entry.getKey()));
            context.write(outputKey, outputValue);
            i++;
          }
        }
    }
}



public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
        System.err.println("Usage: pangrank <in> <out>");
        System.exit(2);
    }
    Job job = new Job(conf, "page rank");
    job.setJarByClass(PageRank.class);
    job.setMapperClass(Mapper1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // have to be "output/1"
    
    job.waitForCompletion(true);
    
  /**/  
  int final_loc = 0;
  for(int i = 0 ; i < 20 ; i++)
  {
    Configuration conf1 = new Configuration();   
    Job job1 = new Job(conf1, "page rank");
    job1.setJarByClass(PageRank.class);
    job1.setMapperClass(Mapper1.class);
    job1.setReducerClass(Reducer2.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path("output/out" + Integer.toString((i * 4) + 1)));
    FileOutputFormat.setOutputPath(job1, new Path("output/out" + Integer.toString((i * 4) + 2)));  
    job1.waitForCompletion(true);
    
    Configuration conf2 = new Configuration();
    Job job2 = new Job(conf2, "page rank");
    job2.setJarByClass(PageRank.class);
    job2.setMapperClass(Mapper1.class);
    job2.setReducerClass(Reducer3.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path("output/out" + Integer.toString((i * 4) + 2)));
    FileOutputFormat.setOutputPath(job2, new Path("output/out" + Integer.toString((i * 4) + 3)));
    job2.waitForCompletion(true);
    
    Configuration conf3 = new Configuration();
    Job job3 = new Job(conf3, "page rank");
    job3.setJarByClass(PageRank.class);
    job3.setMapperClass(Mapper1.class);
    job3.setReducerClass(Reducer4.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path("output/out" + Integer.toString((i * 4) + 3)));
    FileOutputFormat.setOutputPath(job3, new Path("output/out" + Integer.toString((i * 4) + 4)));
    job3.waitForCompletion(true);
    
    Configuration conf4 = new Configuration();
    Job job4 = new Job(conf4, "page rank");
    job4.setJarByClass(PageRank.class);
    job4.setMapperClass(Mapper1.class);
    job4.setReducerClass(Reducer5.class);
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job4, new Path("output/out" + Integer.toString((i * 4) + 4)));
    FileOutputFormat.setOutputPath(job4, new Path("output/out" + Integer.toString((i * 4) + 5)));
    job4.waitForCompletion(true);
    
    final_loc = (i * 4) + 5;
  }
    /**/
    Configuration conf5 = new Configuration();
    Job job5 = new Job(conf5, "page rank");
    job5.setJarByClass(PageRank.class);
    job5.setMapperClass(Mapper2.class);
    job5.setReducerClass(Reducer6.class);
    job5.setOutputKeyClass(Text.class);
    job5.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job5, new Path("output/out" + Integer.toString(final_loc)));
    FileOutputFormat.setOutputPath(job5, new Path("output/pagerank"));
    job5.waitForCompletion(true);
    }
}