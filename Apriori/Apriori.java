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
import org.apache.hadoop.mapred.JobConf;
import java.util.*;

public class Apriori {

    public static class Mapper1 extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new Text("#") , new Text(">," + value.toString()));
        String[] line = value.toString().split(",");
        for(int i = 1; i< line.length; i++)
        {
            String items = line[i];
            context.write(new Text(items),new Text("1"));
        }
    }
  }

public static class Reducer1 extends Reducer<Text,Text,Text,Text> {

      private final int threshold = 60;
      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
         
         if(key.toString().equals("#"))
         {
             for(Text val : values)
             {
                 context.write(key, val);
             }
         }
         else
         {
             int support = 0;
             for(Text val : values)
             {
                 support += Integer.parseInt(val.toString());
             }
             if(support >= threshold)
                 context.write(new Text("i," + key.toString()), new Text(Integer.toString(support)));
         }
         
          
      }

  
  }

public static class Mapper2 extends Mapper<Object, Text, Text, Text>{

    private String flag;

    protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getName();
    }

    private ArrayList<String> freq_items = new ArrayList<String>();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
        if(flag.substring(0,1).equals("f"))
        {
            String[] line = value.toString().split("\t");
            if(line[0].equals("#"))
            {
                context.write(new Text(line[0]), new Text(line[1]));
            }
            else
            {
                String[] tokens = line[0].split(",");
                freq_items.add(tokens[1]);
                context.write(new Text("@"), new Text("iiii," + tokens[1]));
            }
        }
        else
        {
            String[] line = value.toString().split("\t");
            context.write(new Text("@"), new Text("pppp," + line[0]));
        }
            
    }
        
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        if(context.getConfiguration().get("mode").equals("first"))
        {
            for(int i = 0 ; i < freq_items.size() - 1 ; i++)
            {
                for(int j = i + 1 ; j < freq_items.size() ; j++)
                {
                    context.write(new Text("#"), new Text(freq_items.get(i) + "," + freq_items.get(j)));
                }
            }
        }
        
    }
}



    public static class Reducer2 extends Reducer<Text,Text,Text,Text> {

      private ArrayList<String> freq_items = new ArrayList<String>();
    
      private ArrayList<String> freq_pairs = new ArrayList<String>();
      
      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
      
          if(key.toString().equals("#"))
          {
              for(Text val : values)
              {
                  context.write(key, val);
              }
          }
          else if(key.toString().equals("@"))
          {
              for(Text val : values)
              {
                  String[] tokens = val.toString().split(",");
                  if(tokens[0].equals("iiii"))
                  {
                      freq_items.add(tokens[1]);
                  }
                  else
                  {
                      String str = "";
                      for(int i = 1 ; i < tokens.length ; i++)
                          str = str + tokens[i] + ",";
                      freq_pairs.add(str.substring(0, str.length() - 1));
                  }
              }
              
              for(String pr : freq_pairs)
              {
                  for(String item : freq_items)
                  { 
                      
                      ArrayList<String> p = new ArrayList<String>();
                      String[] tokens = pr.split(",");
                      for(int i = 0 ; i < tokens.length ; i++)
                      {
                          p.add(tokens[i]);
                      }
                      int s = p.size();
                      if(!p.contains(item))
                          p.add(item);
                      if(p.size() == s + 1)
                      {
                          String output = "";
                          for(String val : p)
                          {
                              output = output+val+",";
                          
                          }
    
                          context.write(new Text("#"), new Text(output.substring(0, output.length() - 1)));
                      }
                      
                  }
              }
          }
          

          
      }
  }


public static class Mapper3 extends Mapper<Object, Text, Text, Text>{

    
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
        String[] line = value.toString().split("\t");
        context.write(new Text(line[0]), new Text(line[1]));
    }
        
}

    

    public static class Reducer3 extends Reducer<Text,Text,Text,Text> {

      private ArrayList< ArrayList<String> > transactions = new ArrayList< ArrayList<String> >();
      private ArrayList< ArrayList<String> > pairs = new ArrayList< ArrayList<String> >();
      private final int threshold = 60;
      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
      
      
           for(Text val : values)
          {
              String[] line = val.toString().split(",");
              if(line[0].equals(">"))  // transaction
              {
                  ArrayList<String> tr = new ArrayList<String>();
                  for(String item : line)
                  {
                      tr.add(item);
                  }
                  transactions.add(tr);
              }
              else
              {
                  ArrayList<String> pr = new ArrayList<String>();
                  for(String item : line)
                  {
                      pr.add(item);
                  }
                  pairs.add(pr);
              }
          }
          
          // calculate support
          ArrayList< Set<String> > all_pairs = new ArrayList< Set<String> >();
          ArrayList<Integer> all_support = new ArrayList<Integer>();
          for(int i = 0 ; i < pairs.size() ; i++)
          {
              int support = 0;
              
              for(int j = 0 ; j < transactions.size() ; j++)
              {
                  boolean isContains = true;
                  for(String item : pairs.get(i))
                  {
                      if(transactions.get(j).contains(item))
                      {
                          isContains = isContains && true;
                      }
                      else
                      {
                          isContains = isContains && false;
                      }
                  }
                  if(isContains)
                    support += 1;
              }
              
              
              if (support >= threshold)
              {
                  Set<String> pair_set = new HashSet<String>();
                  for(String item : pairs.get(i))
                  {
                      pair_set.add(item);
                  }
                  all_pairs.add(pair_set);
                  all_support.add(support);
              }
          }
          
          boolean[] flags = new boolean[all_pairs.size()]; // indicate if the pair is duplicate
          Arrays.fill(flags, false);
      
          //eliminate duplicate pairs
          for(int i = 0 ; i < all_pairs.size() - 1 ; i++)
          {
              for(int j = i + 1 ; j < all_pairs.size() ; j++)
              {
                  if(all_pairs.get(j).equals(all_pairs.get(i)))
                  {
                      flags[j] = true;
                  }
              }
          }
          
          //output
          int i = 0;
          for(Set<String> s : all_pairs)
          {
              if(!flags[i])
              {
                  String p = "";
                  for(String item : s)
                  {
                      p = p + item + ",";
                  }
                  p = p.substring(0, p.length() - 1);
                  context.write(new Text(p), new Text(Integer.toString(all_support.get(i))));
              }
              i++;
          }
        
      }
  }



  

public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    Job job1 = new Job(conf1, "Apriori");
    job1.getConfiguration().set("mapreduce.output.basename", "fuck");
    job1.setJarByClass(Apriori.class);
    job1.setMapperClass(Mapper1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path("data/groceries.txt"));
    FileOutputFormat.setOutputPath(job1, new Path("output/Apriori_1"));
    job1.waitForCompletion(true);

    for(int i = 0 ; i < 3 ; i++)
    {
        Configuration conf2 = new Configuration();
        if(i == 0)
            conf2.set("mode", "first");
        else
            conf2.set("mode", "not first");
        Job job2 = new Job(conf2, "Apriori");
        job2.setJarByClass(Apriori.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        if(i > 0)
            FileInputFormat.addInputPath(job2, new Path("output/Apriori_1"));
        FileInputFormat.addInputPath(job2, new Path("output/Apriori_"  + Integer.toString(2*i + 1)));
        FileOutputFormat.setOutputPath(job2, new Path("output/Apriori_" + Integer.toString(2*i + 2)));

        job2.waitForCompletion(true);
        
        Configuration conf3 = new Configuration();
        Job job3 = new Job(conf3, "Apriori");
        job3.setJarByClass(Apriori.class);
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path("output/Apriori_" + Integer.toString(2*i + 2)));
        FileOutputFormat.setOutputPath(job3, new Path("output/Apriori_" + Integer.toString(2*i + 3)));
        job3.waitForCompletion(true);
    }
  }
}

