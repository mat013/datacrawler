package dk.emstar.data.datadub;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(classes = {SpringConfig.class})
public class SomeTest {
    @Test
    public void testName() throws Exception {
    	Table<Long, String, Object> table = HashBasedTable.create();
    	table.put(1L, "ABC", 1L);
    	table.put(1L, "ABC1", 1L);
    	table.put(2L, "ABC", 2L);
    	table.put(2L, "ABC1", 2L);
    	
    	Map<Long, Map<String, Object>> rowMap = table.rowMap();
		rowMap.remove(1L);
    	
    	int actual = table.rowKeySet().size();
    	assertThat(actual).isEqualTo(1);

    	
    }
    
}
