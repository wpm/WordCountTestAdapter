package wpmcn.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Algorithm adapter version of the MapReduce word count example.
 *
 * @author <a href="mailto:billmcn@gmail.com">W.P. McNeill</a>
 */
public class WordCount extends Configured implements Tool {

   public int run(String[] args) throws Exception {
      Configuration configuration = getConf();
      Job job = new Job(configuration, "Word Count");
      job.setJarByClass(WordCount.class);

      job.setMapperClass(WordCountMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(LongWritable.class);

      job.setCombinerClass(WordCountReducer.class);
      job.setReducerClass(WordCountReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);

      for (int i = 0; i < args.length - 1; i++)
         FileInputFormat.addInputPath(job, new Path(args[i]));
      if (args.length != 0)
         FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

      return job.waitForCompletion(true) ? 0 : -1;
   }

   static public void main(String[] args) throws Exception {
      int run = ToolRunner.run(new WordCount(), args);
      System.exit(run);
   }

}
