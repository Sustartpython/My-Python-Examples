package simple.jobstream.mapreduce.common.vip;

import java.net.URI;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import com.process.frame.JobStreamRun;
import com.vipcloud.JobNode.DirectRunJobInfoV2;
import com.vipcloud.JobNode.JobNode;

/**
 * <p>
 * Description: 移动目录，如果目标路径存在会被删除
 * </p>
 * 
 * @author qiuhongyang 2018年12月10日 下午3:21:11
 */
public class MoveDir implements DirectRunJobInfoV2 {
	String srcDir = "";
	String dstDir = "";

	@Override
	public void beforeProcess(JobNode node) {
		srcDir = node.extargs.get("srcDir");
		dstDir = node.extargs.get("dstDir");
	}

	@Override
	public boolean RunProcess() {
		System.out.println("\n*************************************");
		System.out.println(srcDir + "\n--->>>\n" + dstDir);
		System.out.println("*************************************\n");
		try {
			Configuration conf = new Configuration();
			for (Entry<Object, Object> entry : JobStreamRun.getCustomProperties().entrySet()) {
				conf.set(entry.getKey().toString(), entry.getValue().toString());
			}
			URI uri = URI.create(conf.get("fs.default.name"));
			System.out.println("**************************" + conf.get("fs.default.name"));
			FileSystem hdfs;
			hdfs = FileSystem.get(uri, conf);
			Path srcPath = new Path(srcDir);
			Path dstPath = new Path(dstDir);
			if (!hdfs.exists(srcPath)) {
				System.out.println("\n*************************************");
				System.out.println("Error: not existes " + srcDir);
				System.out.println("*************************************\n");
//				System.exit(-1);
				return false;
			}
			
			if (hdfs.exists(dstPath)) {
				System.out.println("\n*************************************");
				System.out.println("Will delete " + dstDir + " ...");
				System.out.println("*************************************\n");
				hdfs.delete(dstPath, true);
			}
			hdfs.rename(srcPath, dstPath);

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("\n*************************************");
			System.out.println("Error occured when move directory");
			System.out.println("*************************************\n");
//			System.exit(-1);
			return false;
		}
		System.out.println("\n*************************************");
		System.out.println("Move Dir Success!");
		System.out.println("*************************************\n");
		
		return true;
	}

	@Override
	public void pre(Job job) {
		// 假象：不能用 job
	}

	@Override
	public void SetMRInfo(Job job) {
		// 假象：不能用 job
	}

	@Override
	public void post(Job job) {
		// 假象：不能用 job
	}
}
