package dk.emstar.data.datacrawler;

import static org.assertj.core.api.Fail.fail;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import dk.emstar.data.datacrawler.configuration.SpringConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { SpringConfig.class })
@Ignore
public class SomeITest {
    @Test
    public void testName() throws Exception {
        fail("Test not implemented");
    }
}
