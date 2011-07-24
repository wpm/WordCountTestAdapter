package wpmcn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Total the counts for a token.
 *
 * @author <a href="mailto:billmcn@gmail.com">W.P. McNeill</a>
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
   @Override
   protected void reduce(Text token, Iterable<LongWritable> counts, Context context) throws IOException,
         InterruptedException {
      long tokenCount = 0;
      for (LongWritable count : counts)
         tokenCount += count.get();
      context.write(token, new LongWritable(tokenCount));
   }
}
