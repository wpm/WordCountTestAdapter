package wpmcn.mradapter;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * @author <a href="mailto:billmcn@gmail.com">W.P. McNeill</a>
 */
abstract public class MapAlgorithm<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
      extends MapReduceAlgorithm<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
   public MapAlgorithm(Configuration configuration) {
      super(configuration);
   }

   abstract protected void map(KEYIN key, VALUEIN value) throws IOException, InterruptedException;
}
