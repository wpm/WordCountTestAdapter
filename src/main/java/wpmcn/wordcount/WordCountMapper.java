package wpmcn.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Parse lines of text into tokens and emit a count of one for each token.
 *
 * @author <a href="mailto:billmcn@gmail.com">W.P. McNeill</a>
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
   private LongWritable one = new LongWritable(1);

   @Override
   protected void map(LongWritable lineNumber, Text line, Context context) throws IOException,
         InterruptedException {
      StringTokenizer tokenizer = new StringTokenizer(line.toString());
      while (tokenizer.hasMoreTokens()) {
         Text token = new Text(tokenizer.nextToken().toLowerCase());
         write(token, one, context);
      }
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
