package simple.jobstream.mapreduce.user.walker;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @description: 上传本地到hdfs inputPath outputPath
 * @author tanlin
 * @date 2014-8-8
 * @version 1.0
 */
public class Upload2HDFS {

	public static void main(String[] args) throws IOException {

		// 上传数据
		try {
			// String instanceDir = args[0];
			String instanceDir = "D:\\Python3Project\\wanfang_qk_gather\\pack";
			Configuration conf = new Configuration();
			URI uri = URI.create(conf.get("fs.default.name"));
			FileSystem hdfs = FileSystem.get(uri, conf);

			Path srcDir = new Path(instanceDir);
			// Path dstDir = new Path("/TL/refmerge/refaddtxt/");
			// Path dstDir = new Path(args[1]);
			Path dstDir = new Path("/walker/test/");

			// hdfs.delete(dstDir, true);
			hdfs.copyFromLocalFile(false, true, srcDir, dstDir);

			System.out.println("Upload over!");
		} catch (Exception e) {
			System.out.println("Error occured when copy files");
			e.printStackTrace();
		}
	}
}
