package simple.jobstream.mapreduce.user.walker.base_obj_meta_a;

import java.net.URI;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.process.frame.JobStreamRun;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;

public class Main
{
	static final Logger LOG = Logger.getLogger(Main.class);
	public static String plugClassPath = "simple.SimplePlugObject";
	

	public static void main(String[] args) throws Throwable
	{
		String nowDate = DateTimeHelper.getNowDate();
		String nowTime = DateTimeHelper.getNowTime();
		System.out.println("nowDate:" + nowDate);
		System.out.println("nowTime:" + nowTime);
		
		String srcDir = "/RawData/_base_obj_db3/meta";
		String dstDir = "/BaseData/base_obj_meta_a/db3/" + nowDate;
		
		Configuration conf = new Configuration();
		for (Entry<Object, Object> entry : JobStreamRun.getCustomProperties().entrySet())
		{
			conf.set(entry.getKey().toString(), entry.getValue().toString());
		}
		URI uri = URI.create(conf.get("fs.defaultFS"));
		FileSystem hdfs;
		hdfs = FileSystem.get(uri, conf);
		Path pathPattern = new Path(srcDir + "/*.db3"); 	//文件或目录路径
		FileStatus[] db3List = hdfs.globStatus(pathPattern);
		System.out.println("db3 count:" + db3List.length);
		if (db3List.length < 1) {		// 没有 db3 要处理
			System.out.println("do nothing");
			System.exit(0);
		}
		System.out.println(srcDir + "/*.db3 --->>> " + dstDir);
		Path dstDirPath = new Path(dstDir);		
		if (!hdfs.exists(dstDirPath)) {
			System.out.println("mkdirs: " + dstDir);
			if (!hdfs.mkdirs(dstDirPath)) {
				System.out.println("mkdirs failed: " + dstDir);
				System.exit(-1);
			}
		}
		
		Path[] pathList = FileUtil.stat2Paths(db3List);
		for (Path srcPath : pathList) {			
			Path dstPath = new Path(dstDir + "/" + srcPath.getName());
//			System.out.println(srcPath + "\n--->>>\n" + dstPath + "\n");
			if (!hdfs.rename(srcPath, dstPath)) {
				System.out.println("!!!!!!error mv file:" + srcPath);
				System.exit(-1);
			}
		}
		
		
//		System.setProperty("HADOOP_USER_NAME", "qhy");  
		String[] tmpargs = {"-listJobStream"};
		if (args.length < 1) {
			args = tmpargs;
		}

		Log.info("SimplePlugObject");
		String[] extargs = new String[args.length + 1];
		for (int i = 0; i < args.length; i++)
		{
			extargs[i] = args[i];
			System.out.println(args[i]);
		}
		
		extargs[args.length] = "-jarNameForAnalysisPlugCP=" + plugClassPath;
		JobStreamRun.main(extargs);
		
	}
}
