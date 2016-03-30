package dk.emstar.data.datadub;

import java.util.Map;

import org.assertj.core.util.Lists;
import org.junit.Test;

import com.google.common.collect.Maps;

public class ETest {

	@Test
	public void testName() throws Exception {
		System.out.println(Lists.newArrayList(5L).equals(Lists.newArrayList(2L)));
		System.out.println(Lists.newArrayList(5L).equals(Lists.newArrayList(5L)));

		Map<String, String> map1 = Maps.newHashMap();
		Map<String, String> map2 = Maps.newHashMap();
		Map<String, String> map3 = Maps.newHashMap();
		
		map1.put("a", "b");
		map2.put("b", "b");
		map3.put("a", "b");
		map3.put("a2", "b");
		System.out.println(map1.equals(map2));
		System.out.println(map1.equals(map3));
		
	}
}
