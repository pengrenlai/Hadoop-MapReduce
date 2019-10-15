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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.*;

public class Kmeans {

    public static class Mapper1 extends Mapper<Object, Text, Text, Text>{

    private String flag;

    protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getName();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
      int dataFlag = 1;
      int centroidFlag = 0;
      
      if(flag.substring(0,1).equals("d"))
      {
          context.write(new Text("fuck"), new Text(Integer.toString(dataFlag) + "," + value.toString()));
      }
      else
      {
          context.write(new Text("fuck"), new Text(Integer.toString(centroidFlag) + "," + value.toString()));
      }
    
    
      //context.write(shingle, new Text("D_" + flag.substring(0, 3)));

  }
}
  public static class Reducer1 extends Reducer<Text,Text,Text,Text> {
  
        int data_count = 1;
        int centroid_count = -1;
  
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            
            for(Text value : values)
            {
                String[] tokens = value.toString().split(",");
                if(Integer.parseInt(tokens[0]) == 1)
                {
                    context.write(new Text(Integer.toString(data_count)), new Text(tokens[1]));
                    data_count += 1;
                }
                else
                {
                    context.write(new Text(Integer.toString(centroid_count)), new Text(tokens[1]));
                    centroid_count -= 1;
                }
            }
  
        }
    }
  public static class Mapper2 extends Mapper<Object, Text, Text, Text>{

      private final int n = 4601;
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          String[] tokens = value.toString().split("\t");
          String k = tokens[0];
          String v = tokens[1];
          if(!k.equals("#"))
          {
              if(Integer.parseInt(k) > 0) //data point
              {
                  context.write(new Text(k), new Text(k + "," + v));
              }
              else
              {
                  for(int i = 1 ; i <= n ; i++) // centroid
                  {
                      context.write(new Text(Integer.toString(i)), new Text(k + "," + v));
                  }
              }
          }
    }
}

  public static class Reducer2 extends Reducer<Text,Text,Text,Text> {
  
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        
        String node = "";  // position
        ArrayList<String> centroid_list = new ArrayList<String>();    
        for(Text value : values)
        {
            String[] tokens = value.toString().split(",");
            if(Integer.parseInt(tokens[0]) > 0) //data point
            {
                node = value.toString();
            }
            else
            {
                centroid_list.add(value.toString());
            }
        }
        
        double min_dist = Double.MAX_VALUE;
        
        String cluster_id = "";
        
        for(String centroid : centroid_list)
        {
            String[] node_feature = node.split(",")[1].split(" ");
            String[] c = centroid.split(",");
            String centroid_id = c[0];
            String[] centroid_feature = c[1].split(" ");
            
            double dist = 0.0;  
            for(int i = 0 ; i < node_feature.length ; i++)
            {
                //dist = dist + Math.pow((Double.parseDouble(node_feature[i]) - Double.parseDouble(centroid_feature[i])) , 2);    //Eu
                dist += Math.abs((Double.parseDouble(node_feature[i]) - Double.parseDouble(centroid_feature[i])));    //Ma
            }
            //dist = Math.sqrt(dist); //Eu
            if(dist < min_dist)
            {
                min_dist = dist;
                cluster_id = centroid_id;
            }
        }
        
        context.write(new Text("#"), new Text(Double.toString(min_dist)));
        context.write(new Text(cluster_id), new Text(node));
    }
  }
  
   public static class Mapper3 extends Mapper<Object, Text, Text, Text>{

 

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
       String[] tokens = value.toString().split("\t");
       context.write(new Text(tokens[0]), new Text(tokens[1]));

  }
}
  public static class Reducer3 extends Reducer<Text,Text,Text,Text> {
  
        private final int dim = 58;
  
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        
            
            if(key.toString().equals("#"))
            {
                double dist = 0.0;
                for(Text value : values)
                {
                     //dist += Math.pow(Double.parseDouble(value.toString()), 2);     //Eu
                     dist += Double.parseDouble(value.toString());     //Ma
                }
                context.write(new Text("#"), new Text(Double.toString(dist)));
            }
            else
            {
                double[] sum_centroid = new double[dim];
                for(int i = 0 ; i < dim ; i++)
                    sum_centroid[i] = 0.0;
                
                int node_num = 0;
                for(Text value : values)
                {
                    node_num += 1;
                    String[] tokens = value.toString().split(",");
                    context.write(new Text(tokens[0]), new Text(tokens[1])); // write data point
                    String[] features = tokens[1].split(" ");
                    for(int i = 0 ; i < features.length ; i++)
                    {
                        sum_centroid[i] += Double.parseDouble(features[i]);
                    }
                }
                
                String new_centroid = "";
                for(int i = 0 ; i < dim ; i++)
                {
                    sum_centroid[i] = sum_centroid[i] / node_num;
                    new_centroid = new_centroid + Double.toString(sum_centroid[i]) + " ";
                }
                
                new_centroid = new_centroid.substring(0, new_centroid.length() - 1);
                
                context.write(key, new Text(new_centroid));
            }
  
        }
    }
  
public static void main(String[] args) throws Exception {
/**
    Configuration conf1 = new Configuration();
    Job job1 = new Job(conf1, "Kmeans");
    job1.setJarByClass(Kmeans.class);
    job1.setMapperClass(Mapper1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path("data/c2.txt"));
    FileInputFormat.addInputPath(job1, new Path("data/data.txt"));
    FileOutputFormat.setOutputPath(job1, new Path("output/kmeans@@_1"));
    job1.waitForCompletion(true);
*/
    for(int i = 1 ; i < 20 ; i++)
    {
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "Kmeans");
        job2.setJarByClass(Kmeans.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("output/kmeans@@_" + Integer.toString(2 * i + 1)));
        FileOutputFormat.setOutputPath(job2, new Path("output/kmeans@@_" + Integer.toString(2 * i + 2)));
        job2.waitForCompletion(true);
    
        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "Kmeans");
        job3.setJarByClass(Kmeans.class);
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path("output/kmeans@@_" + Integer.toString(2 * i + 2)));
        FileOutputFormat.setOutputPath(job3, new Path("output/kmeans@@_" + Integer.toString(2 * i + 3)));
        job3.waitForCompletion(true);
    }
    
    }
}
