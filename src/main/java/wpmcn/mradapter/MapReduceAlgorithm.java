package wpmcn.mradapter;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * @author <a href="mailto:billmcn@gmail.com">W.P. McNeill</a>
 */
abstract public class MapReduceAlgorithm<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
   protected Configuration configuration;

   protected MapReduceAlgorithm(Configuration configuration) {
      this.configuration = configuration;
   }

   abstract protected void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException;
}
