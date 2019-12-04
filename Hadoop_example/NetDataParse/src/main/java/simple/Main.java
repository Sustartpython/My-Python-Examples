package simple;

import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.process.frame.JobStreamRun;

public class Main
{
	static final Logger LOG = Logger.getLogger(Main.class);
	public static String plugClassPath = "simple.SimplePlugObject";
	

	public static void main(String[] args) throws Throwable
	{
//		System.setProperty("HADOOP_USER_NAME", "qhy");  
		
		//String[] tmpargs = {"-jobStreamName=SimpleJob"};
		String[] tmpargs = {"-listJobStream"};
		if (args.length < 1) {
			args = tmpargs;
		}

		Log.info("SimplePlugObject");
		String[] extargs = new String[args.length + 1];
		for (int i = 0; i < args.length; i++)
		{
			extargs[i] = args[i];
		}
		
		extargs[args.length] = "-jarNameForAnalysisPlugCP=" + plugClassPath;
		JobStreamRun.main(extargs);
		
	}
}
