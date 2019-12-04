package simple.jobstream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HAUtil;

import com.process.frame.JobStreamImpl;
import com.process.frame.JobStreamRun;
import com.vipcloud.JobNode.JobNode;


public class SimpleJobStream extends JobStreamImpl {
	/**
	 * 简单的测试统计用例
	 */
	public LinkedHashSet<JobNode> SimpleJob() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		JobNode SimpleJob = new JobNode("SimpleJob", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.ExportLngid");
		
//		SimpleJob.setConfig("inputHdfsPath", "/RawData/cnki/qk/detail/latest");
//		SimpleJob.setConfig("outputHdfsPath", "/vipuser/walker/output/tmp20180418");

		result.add(SimpleJob);

		return result;
	}

	/**
	 * 提取期刊pdf路径（for 徐勇）
	 */
	public LinkedHashSet<JobNode> ExtractFulltextpath() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		JobNode ExtractFulltextpath = new JobNode("SimpleJob", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.QK.ExtractFulltextpath");
		
		JobNode ExtractFulltextpath4Error = new JobNode("SimpleJob", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.QK.ExtractFulltextpath4Error");

		result.add(ExtractFulltextpath);
		result.add(ExtractFulltextpath4Error);

		return result;
	}

	// EI解析
	public LinkedHashSet<JobNode> EIParse() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		String rawHtmlDir = "/RawData/EI/big_htm/2017/20170720";
		String rawXXXXObjectDir = "/RawData/EI/XXXXObject";
		String latest_tempDir = "/RawData/EI/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/EI/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/EI/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdEIDir = "/RawData/EI/new_data/StdEI";

		/**/
		JobNode Html2XXXXObject = new JobNode("EIParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.EI.Html2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeEIXXXXObject2Temp = new JobNode("EIParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.EI.MergeEIXXXXObject2Temp");
		MergeEIXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeEIXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("EIParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.EI.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode StdEI = new JobNode("EIParse", defaultRootDir, 0, "simple.jobstream.mapreduce.walker.EI.StdEI");
		StdEI.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdEI.setConfig("outputHdfsPath", stdEIDir);

		// 备份累积数据
		JobNode Temp2Latest = new JobNode("EIParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.EI.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		// *
		// 正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeEIXXXXObject2Temp);
		MergeEIXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdEI);
		// StdEI.addChildJob(Temp2Latest);
		// */

		/*
		 * //导出完整数据 StdEI.setConfig("inputHdfsPath", latestDir);
		 * //StdEI.setConfig("outputHdfsPath", stdEIDir);
		 * StdEI.setConfig("outputHdfsPath", "/vipuser/walker/output/20170411");
		 * result.add(StdEI); //
		 */

		// *
		// 导出增量数据
		// result.add(StdEI);
		// */

		return result;
	}

	public long getCount(String dir, String counterName) {
		System.out.println("***********: " + dir + "\t" + counterName);

		long count = 0;

		try {
			Configuration conf = new Configuration();
			for (Entry<Object, Object> entry : JobStreamRun.getCustomProperties().entrySet()) {
				conf.set(entry.getKey().toString(), entry.getValue().toString());
			}
			URI uri = URI.create(conf.get("fs.default.name"));
			FileSystem hdfs;
			hdfs = FileSystem.get(uri, conf);

			InetSocketAddress active = HAUtil.getAddressOfActive(hdfs);
			System.out.println("hdfs host:" + active.getHostName()); // hadoop001
			System.out.println("hdfs port:" + active.getPort()); // 9000
			InetAddress address = active.getAddress();
			System.out.println("hdfs://" + address.getHostAddress() + ":" + active.getPort()); // hdfs://192.168.8.21:9000

			Path path = new Path(dir);
			FileStatus[] files = hdfs.listStatus(path);

			for (FileStatus file : files) {
				if (file.getPath().getName().endsWith(".txt")) {
					// System.out.println(file.getPath().getName());

					BufferedReader bufRead = null;

					FSDataInputStream fin = hdfs.open(file.getPath());

					bufRead = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
					String line = null;
					while ((line = bufRead.readLine()) != null) {
						line = line.trim();
						if (line.startsWith(counterName)) {
							String item = line.substring(counterName.length() + 1).trim();
							count += Long.parseLong(item);
						}
					}

					fin.close();
					bufRead.close();
				}
			}
			hdfs.close();

			System.out.println("****** " + counterName + ":" + count);

		} catch (Exception ex) {
			System.out.println("*****************exit****************:");
			ex.printStackTrace();
			System.exit(-1);
		}

		// return (int) Math.ceil(1.0 * total / 1000000);
		return count;
	}

	// EI（json）解析
	public LinkedHashSet<JobNode> EIJsonParse() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		String rawJsonDir = "/RawData/EI_JSON/big_json/2017/20171123";
		String rawXXXXObjectDir = "/RawData/EI_JSON/XXXXObject";
		String latest_tempDir = "/RawData/EI_JSON/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/EI_JSON/latest"; // 成品目录
		String newXobjDir = "/RawData/EI_JSON/new_data"; // 本次新增的数据（XXXXObject）
		String updateXobjDir = "/RawData/EI_JSON/update_data";
		String newStdDir = "/RawData/EI_JSON/new_data_db3"; // 本次新增的数据（题录db3目录）
		String db3Dir = "/RawData/EI_JSON/db3";
		String updateStdDir = "/RawData/EI_JSON/update_data_db3"; // 本次更新数据（题录db3目录）

		String infoDir = "/RawData/EI_JSON/info";

		/**/
		JobNode Json2XXXXObject = new JobNode("EIJsonParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.EI_JSON.Json2XXXXObject");
		Json2XXXXObject.setConfig("inputHdfsPath", rawJsonDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("EIJsonParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.EI_JSON.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		MergeXXXXObject2Temp.setConfig("newDataDir", newXobjDir);
		MergeXXXXObject2Temp.setConfig("updateDataDir", updateXobjDir);
		MergeXXXXObject2Temp.setConfig("infoDir", infoDir);

		JobNode New2Db3 = new JobNode("EIJsonParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.EI_JSON.New2Db3");
		New2Db3.setConfig("inputHdfsPath", newXobjDir);
		New2Db3.setConfig("outputHdfsPath", newStdDir);
		New2Db3.setConfig("infoDir", infoDir);

		JobNode Update2Db3 = new JobNode("EIJsonParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.EI_JSON.Update2Db3");
		Update2Db3.setConfig("inputHdfsPath", updateXobjDir);
		Update2Db3.setConfig("outputHdfsPath", updateStdDir);
		Update2Db3.setConfig("infoDir", infoDir);

		JobNode Std2Db3 = new JobNode("EIJsonParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.EI_JSON.New2Db3");
		Std2Db3.setConfig("inputHdfsPath", newXobjDir + "," + updateXobjDir);
		Std2Db3.setConfig("outputHdfsPath", db3Dir);
		Std2Db3.setConfig("infoDir", infoDir);

		// 备份累积数据
		JobNode Temp2Latest = new JobNode("EIJsonParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.EI_JSON.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		/*
		 * //正常更新（区分new和update） result.add(Json2XXXXObject);
		 * Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		 * MergeXXXXObject2Temp.addChildJob(New2Db3);
		 * MergeXXXXObject2Temp.addChildJob(Update2Db3);
		 * MergeXXXXObject2Temp.addChildJob(Temp2Latest); //
		 */

		/*
		 * //正常更新（不区分new和update） result.add(Json2XXXXObject);
		 * Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		 * MergeXXXXObject2Temp.addChildJob(Std2Db3); Std2Db3.addChildJob(Temp2Latest);
		 * //
		 */

		Std2Db3.setConfig("inputHdfsPath", latestDir);
		result.add(Std2Db3);

		return result;
	}

	// wos解析
	/*
	 * 新增数据时注意将所有新数据都要重新刷一遍，因为相同ID的新数据lib可能并不一样。
	 * 
	 */
	public LinkedHashSet<JobNode> WOSParse() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		String rawJsonDir = "/RawData/WOS/big_json/2017/20171123";
		String rawXXXXObjectDir = "/RawData/WOS/XXXXObject"; // 新到json数据转换的BXXXXObject
		String latestTempDir = "/RawData/WOS/latest_temp"; // 临时成品目录
		String newXobjDir = "/RawData/WOS/new_data";
		String updateXobjDir = "/RawData/WOS/update_data";
		String latestDir = "/RawData/WOS/latest"; // 成品目录
		String newStdDir = "/RawData/WOS/new_data_db3"; // 本次新增的数据（题录db3目录）
		String updateStdDir = "/RawData/WOS/update_data_db3"; // 本次更新数据（题录db3目录）
		String db3Dir = "/RawData/WOS/db3";
		String newStdREFDir = "/RawData/WOS/new_data_ref_db3"; // 本次新增的数据（引文db3目录）

		/* 将新到的json数据转换为 BXXXXObject */
		JobNode Json2XXXXObject = new JobNode("WOSParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.WOS.Json2XXXXObject");
		Json2XXXXObject.setConfig("inputHdfsPath", rawJsonDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("WOSParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.WOS.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latestTempDir);
		MergeXXXXObject2Temp.setConfig("newDataDir", newXobjDir);
		MergeXXXXObject2Temp.setConfig("updateDataDir", updateXobjDir);

		// 生成题录db3（新数据）
		JobNode New2Db3 = new JobNode("WOSParse", defaultRootDir, 0, "simple.jobstream.mapreduce.walker.WOS.New2Db3");
		New2Db3.setConfig("inputHdfsPath", newXobjDir);
		New2Db3.setConfig("outputHdfsPath", newStdDir);

		// 生成题录db3（更新数据）
		JobNode Update2Db3 = new JobNode("WOSParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.WOS.Update2Db3");
		Update2Db3.setConfig("inputHdfsPath", updateXobjDir);
		Update2Db3.setConfig("outputHdfsPath", updateStdDir);

		// 生成引文db3
		JobNode StdWOS_REF = new JobNode("WOSParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.WOS.StdWOS_REF");
		StdWOS_REF.setConfig("inputHdfsPath", newXobjDir);
		StdWOS_REF.setConfig("outputHdfsPath", newStdREFDir);

		JobNode Std2Db3 = new JobNode("WOSParse", defaultRootDir, 0, "simple.jobstream.mapreduce.walker.WOS.New2Db3");
		Std2Db3.setConfig("inputHdfsPath", newXobjDir + "," + updateXobjDir);
		Std2Db3.setConfig("outputHdfsPath", db3Dir);

		// 备份累积数据
		JobNode Temp2Latest = new JobNode("WOSParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.WOS.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latestTempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		/*
		 * 正常更新 （区分new和update） result.add(Json2XXXXObject); //将新到的json数据转换为 BXXXXObject
		 * Json2XXXXObject.addChildJob(MergeXXXXObject2Temp); //将历史累积数据和新数据合并去重
		 * MergeXXXXObject2Temp.addChildJob(New2Db3); //不能是“总数据-老数据”，新数据要全刷一遍。
		 * New2Db3.addChildJob(Update2Db3); //生成题录db3
		 * Update2Db3.addChildJob(Temp2Latest); //备份累积数据 //
		 */

		/*
		 * 正常更新 （不区分new和update） result.add(Json2XXXXObject); //将新到的json数据转换为 BXXXXObject
		 * Json2XXXXObject.addChildJob(MergeXXXXObject2Temp); //将历史累积数据和新数据合并去重
		 * MergeXXXXObject2Temp.addChildJob(Std2Db3); Std2Db3.addChildJob(Temp2Latest);
		 * //备份累积数据 //
		 */

		Std2Db3.setConfig("inputHdfsPath", latestDir);
		result.add(Std2Db3);
		Std2Db3.addChildJob(Temp2Latest); // 备份累积数据

		return result;
	}

	// sd qk解析
	public LinkedHashSet<JobNode> SDQKParse() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		String rawHtmlDir = "/RawData/elsevier/sd_qk/big_json/2018/20181009";
		String rawXXXXObjectDir = "/RawData/elsevier/sd_qk/XXXXObject";
		String latest_tempDir = "/RawData/elsevier/sd_qk/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/elsevier/sd_qk/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/elsevier/sd_qk/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir = "/RawData/elsevier/sd_qk/new_data/StdSDQK";

		/**/
		JobNode Html2XXXXObject = new JobNode("SDQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.sd_qk.Html2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("SDQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.sd_qk.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("SDQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.sd_qk.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode Std2Db3 = new JobNode("SDQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.sd_qk.Std2Db3");
		Std2Db3.setConfig("inputHdfsPath", newXXXXObjectDir);
		Std2Db3.setConfig("outputHdfsPath", stdDir);
		Std2Db3.setConfig("reduceNum", "1");

		// 备份累积数据
		JobNode Temp2Latest = new JobNode("SDQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.sd_qk.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		// 正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2Db3);
		Std2Db3.addChildJob(Temp2Latest);

		// 单轮测试
//		result.add(Html2XXXXObject);
//		Std2Db3.setConfig("inputHdfsPath", rawXXXXObjectDir);
//		Html2XXXXObject.addChildJob(Std2Db3);

		// 导出所有
//		StdSDQK.setConfig("inputHdfsPath", latestDir);
//		StdSDQK.setConfig("reduceNum", "20");
//		result.add(StdSDQK);

		return result;
	}

	// cnki期刊解析
	public LinkedHashSet<JobNode> CNKIQKParse() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		String rawHtmlDir = "/RawData/cnki/qk/detail/big_json/2018/20181009";
		String rawXXXXObjectDir = "/RawData/cnki/qk/detail/XXXXObject";
		String latest_tempDir = "/RawData/cnki/qk/detail/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/cnki/qk/detail/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/cnki/qk/detail/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir = "/RawData/cnki/qk/detail/new_data/StdCNKIQK";
		String countDir = "/RawData/cnki/qk/detail/count";

		JobNode Html2XXXXObject = new JobNode("CNKIQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk.Html2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("CNKIQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("CNKIQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode StdCNKIQK = new JobNode("CNKIQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk.StdCNKIQK");
		StdCNKIQK.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdCNKIQK.setConfig("outputHdfsPath", stdDir);
		StdCNKIQK.setConfig("reduceNum", "1");

		// 备份累积数据
		JobNode Temp2Latest = new JobNode("CNKIQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		// 统计单期文章量
		JobNode CountArticleGroupByBookid = new JobNode("CNKIQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk.CountArticleGroupByBookid");
		// CountArticleGroupByBookid.setConfig("inputHdfsPath", latestDir);
		CountArticleGroupByBookid.setConfig("inputHdfsPath", newXXXXObjectDir);
		CountArticleGroupByBookid.setConfig("outputHdfsPath", countDir);
		CountArticleGroupByBookid.setConfig("reduceNum", "1");

		// 统计文章量到access
		JobNode CountArticleGroupByBookid2Access = new JobNode("CNKIQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk.CountArticleGroupByBookid2Access");
		CountArticleGroupByBookid2Access.setConfig("export2access.inputTablePath", latestDir);
		CountArticleGroupByBookid2Access.setConfig("export2access.outputTablePath", countDir);
		CountArticleGroupByBookid2Access.setConfig("export2access.rednum", "1");
		String sql = "CREATE TABLE ArticleCount(pykm text(100), years text(100), num text(100), bookid text(100), cnt int)";
		CountArticleGroupByBookid2Access.setConfig("export2access.sSqlCreate", sql);

		// 正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdCNKIQK);
		StdCNKIQK.addChildJob(Temp2Latest);
		Temp2Latest.addChildJob(CountArticleGroupByBookid2Access);

		// 单次测试
//		result.add(Html2XXXXObject);
//		StdCNKIQK.setConfig("inputHdfsPath", rawXXXXObjectDir);
//		Html2XXXXObject.addChildJob(StdCNKIQK);

		return result;
	}

	// cnki期刊引文解析
	public LinkedHashSet<JobNode> CNKIQKRefParse() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		String rawHtmlDir = "/RawData/cnki/qk/ref/big_json/2018/20180927";
		String rawXXXXObjectDir = "/RawData/cnki/qk/ref/XXXXObject";
		String latest_tempDir = "/RawData/cnki/qk/ref/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/cnki/qk/ref/latest"; // 成品目录
		String latestExportDir = "/RawData/cnki/qk/ref/latest_export"; // 成品db3目录
		String newXXXXObjectDir = "/RawData/cnki/qk/ref/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir = "/RawData/cnki/qk/ref/new_data/StdCNKIQKRef";

		/**/
		JobNode Html2XXXXObject = new JobNode("CNKIQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk_ref.Html2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("CNKIQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk_ref.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("CNKIQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk_ref.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode StdCNKIQKRef = new JobNode("CNKIQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk_ref.StdCNKIQKRef");
		StdCNKIQKRef.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdCNKIQKRef.setConfig("outputHdfsPath", stdDir);
		StdCNKIQKRef.setConfig("reduceNum", "1");

		// 备份累积数据
		JobNode Temp2Latest = new JobNode("CNKIQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk_ref.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		// 导入latest里面的完整数据到db3
		JobNode ExportLatest = new JobNode("CNKIQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.cnki_qk_ref.ExportLatest");
		ExportLatest.setConfig("inputHdfsPath", latestDir);
		ExportLatest.setConfig("outputHdfsPath", latestExportDir);

		// 正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdCNKIQKRef);
		StdCNKIQKRef.addChildJob(Temp2Latest);

//		result.add(Html2XXXXObject);
//		StdCNKIQKRef.setConfig("inputHdfsPath", rawXXXXObjectDir);
//		Html2XXXXObject.addChildJob(StdCNKIQKRef);

		return result;
	}

	// wanfang期刊解析
	public LinkedHashSet<JobNode> WFQKParse() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		String rawHtmlDir = "/RawData/wanfang/qk/detail/big_json/2018/20181009";
		String rawXXXXObjectDir = "/RawData/wanfang/qk/detail/XXXXObject";
		String latestTempDir = "/RawData/wanfang/qk/detail/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/wanfang/qk/detail/latest"; // 成品目录
		String citedLatestDir = "/RawData/wanfang/qk/cited/latest"; // 引文量、被引量的 latest
		String newXXXXObjectDir = "/RawData/wanfang/qk/detail/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir = "/RawData/wanfang/qk/detail/new_data/StdWFQK";

		JobNode Json2XXXXObject = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk.Json2XXXXObject");
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latestTempDir);

		// 合并引文量/被引量到主题录
		JobNode MergeCited2Latest = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk.MergeCited2Latest");
		MergeCited2Latest.setConfig("inputHdfsPath", latestTempDir + "," + citedLatestDir);
		MergeCited2Latest.setConfig("outputHdfsPath", latestDir);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latestDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode Std2Db3 = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk.Std2Db3");
		Std2Db3.setConfig("inputHdfsPath", newXXXXObjectDir);
		Std2Db3.setConfig("outputHdfsPath", stdDir);
		Std2Db3.setConfig("reduceNum", "1");

		// 正常更新
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(MergeCited2Latest);
//		MergeCited2Latest.addChildJob(GenNewData);
//		GenNewData.addChildJob(Std2Db3);
		
		// 测试单个解析
//		result.add(Json2XXXXObject);
//		Std2Db3.setConfig("inputHdfsPath", rawXXXXObjectDir);
//		Json2XXXXObject.addChildJob(Std2Db3);

		// 万方医学
		JobNode Std2Db3Med = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk.Std2Db3Med");
		Std2Db3Med.setConfig("inputHdfsPath", newXXXXObjectDir);
		Std2Db3Med.setConfig("outputHdfsPath", "/RawData/wanfang/qk/detail/med/new_data");
		Std2Db3Med.setConfig("reduceNum", "1");
		
		JobNode CountMed = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk.CountMed");
		CountMed.setConfig("inputHdfsPath", "/RawData/wanfang/qk/detail/latest");	
		CountMed.setConfig("outputHdfsPath", "/RawData/wanfang/qk/detail/med/CountMed");
		
		result.add(Std2Db3Med);
		Std2Db3Med.addChildJob(CountMed);

		return result;
	}

	// wanfang期刊引文量/被引量解析
	public LinkedHashSet<JobNode> WFQKCitedParse() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		String rawHtmlDir = "/RawData/wanfang/qk/cited/big_json/20180820";
		String rawXXXXObjectDir = "/RawData/wanfang/qk/cited/XXXXObject";
		String latest_tempDir = "/RawData/wanfang/qk/cited/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/wanfang/qk/cited/latest"; // 成品目录

		JobNode Json2XXXXObject = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk_cited.Json2XXXXObject");
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk_cited.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		// 备份累积数据
		JobNode Temp2Latest = new JobNode("WFQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk_cited.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(Temp2Latest);

		return result;
	}

	// wanfang期刊引文解析
	public LinkedHashSet<JobNode> WFQKRefParse() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		String rawHtmlDir = "/RawData/wanfang/qk/ref/big_json/2018/20180820";
		String rawXXXXObjectDir = "/RawData/wanfang/qk/ref/XXXXObject";
		String latest_tempDir = "/RawData/wanfang/qk/ref/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/wanfang/qk/ref/latest"; // 成品目录
		String latestExportDir = "/RawData/wanfang/qk/ref/latest_export"; // 成品db3目录
		String newXXXXObjectDir = "/RawData/wanfang/qk/ref/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir = "/RawData/wanfang/qk/ref/new_data/StdWFQKRef";

		JobNode Html2XXXXObject = new JobNode("WFQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk_ref.Json2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("WFQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk_ref.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("WFQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk_ref.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode StdWFQKRef = new JobNode("WFQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk_ref.StdWFQKRef");
		StdWFQKRef.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdWFQKRef.setConfig("outputHdfsPath", stdDir);
		StdWFQKRef.setConfig("reduceNum", "50");

		// 备份累积数据
		JobNode Temp2Latest = new JobNode("WFQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk_ref.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		// 导入latest里面的完整数据到db3
		JobNode ExportLatest = new JobNode("WFQKRefParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.wf_qk_ref.ExportLatest");
		ExportLatest.setConfig("inputHdfsPath", latestDir);
		ExportLatest.setConfig("outputHdfsPath", latestExportDir);

		// *
		// 正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdWFQKRef);
		StdWFQKRef.addChildJob(Temp2Latest);
		// */

		return result;
	}

	// iop期刊解析
	public LinkedHashSet<JobNode> IOPQKParse() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		String rawJsonDir = "/RawData/iopjournal/big_json/2018/20181008";
		String rawXXXXObjectDir = "/RawData/iopjournal/XXXXObject";
		String latest_tempDir = "/RawData/iopjournal/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/iopjournal/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/iopjournal/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String stdDir = "/RawData/iopjournal/new_data/StdWFQK";

		JobNode Json2XXXXObject = new JobNode("IOPQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.iopjournal.Json2XXXXObject");
		Json2XXXXObject.setConfig("inputHdfsPath", rawJsonDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = new JobNode("IOPQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.iopjournal.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("IOPQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.iopjournal.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode Std2DB3 = new JobNode("IOPQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.iopjournal.Std2Db3");
		Std2DB3.setConfig("inputHdfsPath", newXXXXObjectDir);
		Std2DB3.setConfig("outputHdfsPath", stdDir);
		Std2DB3.setConfig("reduceNum", "1");

		// 备份累积数据
		JobNode Temp2Latest = new JobNode("IOPQKParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.walker.iopjournal.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2DB3);
		Std2DB3.addChildJob(Temp2Latest);



		// 单次测试
//		result.add(Json2XXXXObject);
//		Std2DB3.setConfig("inputHdfsPath", rawXXXXObjectDir);
//		Json2XXXXObject.addChildJob(Std2DB3);

		return result;
	}

	
		
	public void initJobStreamData(String genDataTag) throws IOException {
		this.addSubJobStream("SimpleJob", SimpleJob());

		this.addSubJobStream("ExtractFulltextpath", ExtractFulltextpath());

		this.addSubJobStream("WOSParse", WOSParse());

		this.addSubJobStream("EIParse", EIParse());

		this.addSubJobStream("EIJsonParse", EIJsonParse());

		this.addSubJobStream("SDQKParse", SDQKParse());

		this.addSubJobStream("CNKIQKParse", CNKIQKParse());

		this.addSubJobStream("CNKIQKRefParse", CNKIQKRefParse());

		this.addSubJobStream("WFQKParse", WFQKParse());

		this.addSubJobStream("WFQKCitedParse", WFQKCitedParse());

		this.addSubJobStream("WFQKRefParse", WFQKRefParse());

		this.addSubJobStream("IOPQKParse", IOPQKParse());
		
		this.addSubJobStream("RscParse", simple.jobstream.mapreduce.site.rscbook.SingleJobStream.getJobStream());
	}
}
