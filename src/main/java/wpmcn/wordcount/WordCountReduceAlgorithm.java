package wpmcn.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import wpmcn.mradapter.ReduceAlgorithm;

import java.io.IOException;

/**
 * Total the counts for a token.
 *
 * @author <a href="mailto:billmcn@gmail.com">W.P. McNeill</a>
 */
abstract public class WordCountReduceAlgorithm extends ReduceAlgorithm<Text, LongWritable, Text, LongWritable> {
   public WordCountReduceAlgorithm(Configuration configuration) {
      super(configuration);
   }

   @Override
   protected void reduce(Text token, Iterable<LongWritable> counts) throws IOException,
         InterruptedException {
      long tokenCount = 0;
      for (LongWritable count : counts)
         tokenCount += count.get();
      write(token, new LongWritable(tokenCount));
   }
}
