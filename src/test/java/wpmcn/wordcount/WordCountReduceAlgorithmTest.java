package wpmcn.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

/**
 * @author <a href="mailto:billmcn@gmail.com">W.P. McNeill</a>
 */
public class WordCountReduceAlgorithmTest {
   private class WordCountReduceTestHarness extends WordCountReduceAlgorithm {
      public Map<String, Long> tokenCounts = new HashMap<String, Long>();

      public WordCountReduceTestHarness() {
         super(null);
      }

      @Override
      protected void write(Text token, LongWritable count) throws IOException, InterruptedException {
         String t = token.toString();
         if (!tokenCounts.containsKey(t))
            tokenCounts.put(t, 0L);
         tokenCounts.put(t, tokenCounts.get(t) + count.get());
      }
   }

   private WordCountReduceTestHarness wordCountReducer;

   @Before
   public void setUp() throws Exception {
      wordCountReducer = new WordCountReduceTestHarness();
   }

   @Test
   public void testReduce() throws Exception {
      wordCountReducer.reduce(new Text("to"), counts(2));
      wordCountReducer.reduce(new Text("be"), counts(1, 1));
      wordCountReducer.reduce(new Text("not"), counts(1));
      wordCountReducer.reduce(new Text("or"), counts(1));
      assertEquals(new Long(2), wordCountReducer.tokenCounts.get("to"));
      assertEquals(new Long(2), wordCountReducer.tokenCounts.get("be"));
      assertEquals(new Long(1), wordCountReducer.tokenCounts.get("not"));
      assertEquals(new Long(1), wordCountReducer.tokenCounts.get("or"));
   }

   private Iterable<LongWritable> counts(long... counts) {
      List<LongWritable> iterable = new ArrayList<LongWritable>(counts.length);
      for (long count : counts)
         iterable.add(new LongWritable(count));
      return iterable;
   }
}
