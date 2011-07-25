package wpmcn.mradapter;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * @author <a href="mailto:billmcn@gmail.com">W.P. McNeill</a>
 */
abstract public class ReduceAlgorithm<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
      extends MapReduceAlgorithm<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
   public ReduceAlgorithm(Configuration configuration) {
      super(configuration);
   }

   abstract protected void reduce(KEYIN key, Iterable<VALUEIN> values) throws IOException, InterruptedException;
}
