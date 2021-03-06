package wpmcn.wordcount;

import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 * @author <a href="mailto:billmcn@gmail.com">W.P. McNeill</a>
 */
public class WordCountMapperTest {
   /**
    * This test harness subclasses the write method to ignore the MapReduce context and instead write the output to a
    * list of (key, value) pairs.
    */
   private class WordCountMapperTestHarness extends WordCountMapper {
      public List<Pair<String, Long>> keyValuePairs = new ArrayList<Pair<String, Long>>();

      @Override
      protected void write(Text token, LongWritable count, Context context) {
         keyValuePairs.add(new Pair<String, Long>(token.toString(), count.get()));
      }

      public void map(LongWritable lineNumber, Text text) throws IOException, InterruptedException {
         map(lineNumber, text, null);
      }
   }

   private WordCountMapperTestHarness wordCountMapper;

   @Before
   public void setUp() throws Exception {
      wordCountMapper = new WordCountMapperTestHarness();
   }

   @Test
   public void testMap() throws Exception {
      wordCountMapper.map(new LongWritable(1), new Text("To be or not to be"));
      assertEquals(6, wordCountMapper.keyValuePairs.size());
      assertEquals(new Pair<String, Long>("to", (long) 1), wordCountMapper.keyValuePairs.get(0));
      assertEquals(new Pair<String, Long>("be", (long) 1), wordCountMapper.keyValuePairs.get(1));
      assertEquals(new Pair<String, Long>("or", (long) 1), wordCountMapper.keyValuePairs.get(2));
      assertEquals(new Pair<String, Long>("not", (long) 1), wordCountMapper.keyValuePairs.get(3));
      assertEquals(new Pair<String, Long>("to", (long) 1), wordCountMapper.keyValuePairs.get(4));
      assertEquals(new Pair<String, Long>("be", (long) 1), wordCountMapper.keyValuePairs.get(5));
   }
}
