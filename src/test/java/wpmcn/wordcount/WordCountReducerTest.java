package wpmcn.wordcount;

import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 * @author <a href="mailto:billmcn@gmail.com">W.P. McNeill</a>
 */
public class WordCountReducerTest {
   /**
    * This test harness subclasses the write method to ignore the MapReduce context and instead write the output to a
    * list of (key, value) pairs.
    */
   private class WordCountReducerTestHarness extends WordCountReducer {
      public List<Pair<String, Long>> keyValuePairs = new ArrayList<Pair<String, Long>>();

      @Override
      protected void write(Text token, LongWritable count, Context context) {
         keyValuePairs.add(new Pair<String, Long>(token.toString(), count.get()));
      }

      public void reduce(Text token, Iterable<LongWritable> counts) throws IOException, InterruptedException {
         reduce(token, counts, null);
      }
   }

   private WordCountReducerTestHarness wordCountReducer;

   @Before
   public void setUp() throws Exception {
      wordCountReducer = new WordCountReducerTestHarness();
   }

   @Test
   public void testReduce() throws Exception {
      wordCountReducer.reduce(new Text("to"), counts(2));
      wordCountReducer.reduce(new Text("be"), counts(1, 1));
      wordCountReducer.reduce(new Text("or"), counts(1));
      wordCountReducer.reduce(new Text("not"), counts(1));
      assertEquals(new Pair<String, Long>("to", 2L), wordCountReducer.keyValuePairs.get(0));
      assertEquals(new Pair<String, Long>("be", 2L), wordCountReducer.keyValuePairs.get(1));
      assertEquals(new Pair<String, Long>("or", 1L), wordCountReducer.keyValuePairs.get(2));
      assertEquals(new Pair<String, Long>("not", 1L), wordCountReducer.keyValuePairs.get(3));
   }

   private Iterable<LongWritable> counts(long... counts) {
      List<LongWritable> iterable = new ArrayList<LongWritable>(counts.length);
      for (long count : counts)
         iterable.add(new LongWritable(count));
      return iterable;
   }
}
