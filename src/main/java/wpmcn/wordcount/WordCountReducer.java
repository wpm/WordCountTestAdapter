package wpmcn.wordcount;

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
      write(token, new LongWritable(tokenCount), context);
   }

   /**
    * Write the token and count to the MapReduce output.
    *
    * @param token   a token
    * @param count   the token count
    * @param context MapReduce context
    * @throws IOException
    * @throws InterruptedException
    */
   protected void write(Text token, LongWritable count, Context context) throws IOException, InterruptedException {
      context.write(token, count);
   }
}
