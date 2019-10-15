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

public class LSH {

    public static class Mapper1 extends Mapper<Object, Text, Text, Text>{

    private String flag;

    protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getName();
    }


    private final int k = 3; // k-gram

    Text shingle = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


        String doc = value.toString();
        doc = doc.replaceAll("\\.", "");
        doc = doc.replaceAll(",", "");
        doc = doc.replaceAll("\\-", "");
        doc = doc.replaceAll("\"", "");
        doc = doc.replaceAll("\\s+"," ");

        String[] doc_split = doc.split(" ");

        for(int i = 0 ; i < doc_split.length - k ; i++)
        {
            String s = "";
            for(int j = i ; j < i + k ; j++)
                s = s + doc_split[j] + " ";
            shingle.set(s);
            context.write(shingle, new Text("D_" + flag.substring(0, 3)));
        }
    }
  }

public static class Reducer1 extends Reducer<Text,Text,Text,Text> {

      private int shingles_num = 0;

      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
          ArrayList<String> docs = new ArrayList<String>();
          for (Text value : values) {
              if(!docs.contains(value.toString()))
                docs.add(value.toString());
          }

          String doc_list = "";
          for(String doc : docs)
            doc_list = doc_list + doc + ",";

          context.write(key, new Text(doc_list));

          shingles_num += 1;
      }

      protected void cleanup(Context context)
      {
          context.getCounter("shingles_num", "shingles_num").increment(shingles_num);
      }
  }



  public static class Mapper2
        extends Mapper<Object, Text, Text, Text>{

    private int row_num = 1;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

            String doc_list = value.toString().split("\t")[1];
            StringTokenizer itr = new StringTokenizer(doc_list, ",");

            int shingles_num = Integer.parseInt(context.getConfiguration().get("shingles_num"));
			int prime = 12569;
			
            while (itr.hasMoreTokens()) {
                String doc = itr.nextToken();
                for(int i = 1 ; i <= 100 ; i++)
                {
                    int hash = (((i * row_num) + (102 - i)) % prime ) % shingles_num;   // 100 hash function
                    String v = Integer.toString(i) + "," + Integer.toString(hash); // <key, value> = <doc_id, (i, hi(row_num))>
                    context.write(new Text(doc), new Text(v));
                }
            }

            row_num += 1;

        }
    }



    public static class Reducer2 extends Reducer<Text,Text,Text,Text> {

      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

          String sig[] = new String[100];
                int index = 0;
                int hashvalue = 0;
                for(int i = 0 ; i < 100; i++)
                {
                    sig[i] = "999999";
                }
                for(Text value : values)
                {
                    String str = value.toString();
                    String[] index_sig = str.split(",");
                    index = Integer.valueOf(index_sig[0]);
                    hashvalue = Integer.valueOf(index_sig[1]);
                    if(Integer.valueOf(sig[index-1])>hashvalue) 
                        sig[index-1] = String.valueOf(hashvalue);
                }
                String sigvalue = "";
                for(int i = 0 ; i < 100 ; i++)
                {
                    sigvalue = sigvalue+sig[i]+",";
                }
                context.write(key,new Text(sigvalue));

      }
  }


  public static class Mapper3
        extends Mapper<Object, Text, Text, Text>{

    private final int r = 2;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

            String tokens[] = value.toString().split("\t");
            String sig_col_vec = tokens[1];
            String doc_id = tokens[0];
            StringTokenizer itr = new StringTokenizer(sig_col_vec, ",");
            int i = 0;
            String sig = "";
            while (itr.hasMoreTokens())
            {
                sig = sig +  itr.nextToken() + ",";
                i += 1;
                if(i % r == 0)
                {
                    int band = i / r;
                    context.write(new Text(Integer.toString(band) + "_" + sig + "@@@@@"), new Text(doc_id));
                    sig = "";
                }
            }

        }
    }



    public static class Reducer3 extends Reducer<Text,Text,Text,Text> {

      private ArrayList<String> candidate = new ArrayList<String>();

      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
          ArrayList<String> docs = new ArrayList<String>();
          for (Text value : values) {
              docs.add(value.toString());
          }

          if(docs.size() > 1)
          {
              String pairs = "";
              String pairs_reverse = "";
              for(int i = 0 ; i < docs.size() - 1 ; i++)
              {
                  for(int j = i + 1 ; j < docs.size() ; j++)
                  {
                      pairs = docs.get(i) + "," + docs.get(j);
                      pairs_reverse = docs.get(j) + "," + docs.get(i);
                      if(!candidate.contains(pairs) && !candidate.contains(pairs_reverse))
                      {
                          candidate.add(pairs);
                          context.write(new Text("candidate pairs"), new Text(pairs));
                      }
                  }
              }
          }

      }

  }

  public static class Mapper4
        extends Mapper<Object, Text, Text, Text>{

    private String flag;

    protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

            if(flag.equals("lsh_2"))
            {
                String tokens[] = value.toString().split("\t");
                context.write(new Text("whatever"), new Text(tokens[0] + "#" + tokens[1]));
            }
            else if(flag.equals("lsh_3"))
            {
                String tokens[] = value.toString().split("\t");
                context.write(new Text("whatever"), new Text(tokens[1]));
            }

        }


}


  public static class Reducer4 extends Reducer<Text,Text,Text,Text> {
      
      private Map<String, String> sig_matrix = new HashMap<String, String>();
      private ArrayList<String> pairs = new ArrayList<String>();
      
      public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
          for (Text value : values) {
              String tokens[] = value.toString().split(",");
              if(tokens.length > 2)
              {
                  String[] sigs = value.toString().split("#");
                  sig_matrix.put(sigs[0], sigs[1]);
              }
              else
              {
                  pairs.add(value.toString());
              }
          }
          
          Map<Float, String> sorted_pairs = new TreeMap<Float, String>(Collections.reverseOrder());
          
          for(String pair : pairs)
          {
              String[] p = pair.split(",");
              
              String[] doc_vec_1 = sig_matrix.get(p[0]).split(",");
              String[] doc_vec_2 = sig_matrix.get(p[1]).split(",");
              
              int intersection = 0;
              for(int i = 0 ; i < doc_vec_1.length ; i++)
              {
                  if(doc_vec_1[i].equals(doc_vec_2[i]))
                      intersection++;
              }
              
              float sim = (float)intersection / doc_vec_1.length;
              
              if(sorted_pairs.containsKey(sim))
              {
                  String dup_pairs = sorted_pairs.get(sim);
                  dup_pairs = dup_pairs + "#" + pair;
                  sorted_pairs.remove(sim);
                  sorted_pairs.put(sim, dup_pairs);
              }
              else
                  sorted_pairs.put(sim, pair);
          }
          
          for(Map.Entry<Float, String> entry : sorted_pairs.entrySet())
          {
              String v = entry.getValue();
              float k = entry.getKey();
              String[] dup_pairs = v.split("#");
              for(String dup_pair : dup_pairs) 
                  context.write(new Text("(" + dup_pair + ")"), new Text(Float.toString(k)));
          }
      }

  }

public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    Job job1 = new Job(conf1, "lsh");
    job1.setJarByClass(LSH.class);
    job1.setMapperClass(Mapper1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    for(int i = 1 ; i <= 50 ; i++)
    {
        FileInputFormat.addInputPath(job1, new Path("data/" + String.format("%03d", i) + ".txt"));
    }
    FileOutputFormat.setOutputPath(job1, new Path("output/lsh_1"));
    job1.waitForCompletion(true);

    int shingles_num = (int)job1.getCounters().findCounter("shingles_num", "shingles_num").getValue();

    Configuration conf2 = new Configuration();
    conf2.set("shingles_num", Integer.toString(shingles_num));
    Job job2 = new Job(conf2, "lsh");
    job2.setJarByClass(LSH.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path("output/lsh_1"));
    FileOutputFormat.setOutputPath(job2, new Path("output/lsh_2"));
    job2.waitForCompletion(true);

    Configuration conf3 = new Configuration();
    Job job3 = new Job(conf3, "lsh");
    job3.setJarByClass(LSH.class);
    job3.setMapperClass(Mapper3.class);
    job3.setReducerClass(Reducer3.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path("output/lsh_2"));
    FileOutputFormat.setOutputPath(job3, new Path("output/lsh_3"));
    job3.waitForCompletion(true);

    Configuration conf4 = new Configuration();
    Job job4 = new Job(conf4, "lsh");
    job4.setJarByClass(LSH.class);
    job4.setMapperClass(Mapper4.class);
    job4.setReducerClass(Reducer4.class);
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job4, new Path("output/lsh_2"));
    FileInputFormat.addInputPath(job4, new Path("output/lsh_3"));
    FileOutputFormat.setOutputPath(job4, new Path("output/lsh_4"));
    job4.waitForCompletion(true);
    }
}
