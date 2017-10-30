package org.aksw.limes.core;

import java.io.File;
import org.aksw.limes.core.controller.Controller;
import org.junit.Test;

public class FullLimesTest {

  @Test
  public void testMain() throws Exception {
    String configPath = "src/test/resources/simple.xml";
    Controller.main(new String[]{configPath});
  }
}
