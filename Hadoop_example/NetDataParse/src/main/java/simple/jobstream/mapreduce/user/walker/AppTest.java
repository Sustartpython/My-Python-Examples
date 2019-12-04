package simple.jobstream.mapreduce.user.walker;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {
	/**
	 * Create the test case
	 * 
	 * @param testName
	 *            name of the test case
	 */
	public AppTest(String testName) {
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite(AppTest.class);
	}

	public void setUp() throws Exception {

	}

	public void testAny() {
		String text = "DEC 20";
		
		System.out.println(text.replaceAll("[^A-Z]", ""));
		
		System.out.println("haha");
	}
}
