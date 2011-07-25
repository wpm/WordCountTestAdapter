package wpmcn.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import wpmcn.mradapter.MapAlgorithm;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Parse lines of text into tokens and emit a count of one for each token.
 *
 * @author <a href="mailto:billmcn@gmail.com">W.P. McNeill</a>
 */
abstract public class WordCountMapAlgorithm extends MapAlgorithm<LongWritable, Text, Text, LongWritable> {
   private LongWritable one = new LongWritable(1);

   public WordCountMapAlgorithm(Configuration configuration) {
      super(configuration);
   }

   protected void map(LongWritable lineNumber, Text line) throws IOException, InterruptedException {
      StringTokenizer tokenizer = new StringTokenizer(line.toString());
      while (tokenizer.hasMoreTokens()) {
         Text token = new Text(tokenizer.nextToken().toLowerCase());
         write(token, one);
      }
   }
}
