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

import simple.jobstream.mapreduce.common.vip.JobNodeModel;

//import simple.jobstream.mapreduce.site.cnki_qk.SingleJobStream;

/*
 * 调度单个 JobStream
 */
public class JobStreamSchedule extends JobStreamImpl {

	static Configuration configuration;

	/**
	 * <p>
	 * Description: 用来测试的 JobStream
	 * </p>
	 * 
	 * @author qiuhongyang 2018年12月10日 下午5:08:03
	 * @return
	 */
	public LinkedHashSet<JobNode> SimpleJob() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();

		JobNode SimpleJob = new JobNode("", "", 0,
				"simple.jobstream.mapreduce.user.walker.FilterHdfsData");

		result.add(SimpleJob);

		return result;
	}

	/**
	 * 过滤智立方 type 99/98 的数据
	 */
	public LinkedHashSet<JobNode> FilterZlf_99_98() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";

		JobNode Filter_99_98_merge = new JobNode("", defaultRootDir, 0,
				"simple.jobstream.mapreduce.user.walker.Filter_99_98_merge");
		JobNode Filter_99_98_fz = new JobNode("", defaultRootDir, 0,
				"simple.jobstream.mapreduce.user.walker.Filter_99_98_fz");

		result.add(Filter_99_98_merge);
//		result.add(Filter_99_98_fz);

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

	public void initJobStreamData(String genDataTag) throws IOException {
		this.addSubJobStream("SimpleJob", SimpleJob());

		this.addSubJobStream("base_obj_meta_a",
				simple.jobstream.mapreduce.user.walker.base_obj_meta_a.SingleJobStream.getJobStream());

		this.addSubJobStream("FilterZlf_99_98", FilterZlf_99_98());

		this.addSubJobStream("ExtractFulltextpath", ExtractFulltextpath());

		this.addSubJobStream("EIParse", EIParse());

		this.addSubJobStream("EIJsonParse", EIJsonParse());

		// sd 期刊解析
//		this.addSubJobStream("SDQKParse", simple.jobstream.mapreduce.site.sd_qk.SingleJobStream.getJobStream());
		this.addSubJobStream("SDQKParse", simple.jobstream.mapreduce.user.walker.sd_qk.SingleJobStream.getJobStream());

		// cnki 期刊解析
		this.addSubJobStream("CNKIQKParse", simple.jobstream.mapreduce.user.walker.cnki_qk.SingleJobStream.getJobStream());

		// cnki 期刊引文解析
		this.addSubJobStream("CNKIQKRefParse",
				simple.jobstream.mapreduce.user.walker.cnki_qk_ref.SingleJobStream.getJobStream());

		// 万方期刊解析
//		this.addSubJobStream("WFQKParse", simple.jobstream.mapreduce.site.wf_qk.SingleJobStream.getJobStream());
		this.addSubJobStream("WFQKParse", simple.jobstream.mapreduce.user.walker.wf_qk.SingleJobStream.getJobStream());

		// wanfang期刊引文量/被引量解析
//		this.addSubJobStream("WFQKCitedParse",
//				simple.jobstream.mapreduce.site.wf_qk_cited.SingleJobStream.getJobStream());
		this.addSubJobStream("WFQKRelParse",
				simple.jobstream.mapreduce.user.walker.wf_qk_rel.SingleJobStream.getJobStream());

		// 万方期刊引文解析
//		this.addSubJobStream("WFQKRefParse", simple.jobstream.mapreduce.site.wf_qk_ref.SingleJobStream.getJobStream());
		this.addSubJobStream("WFQKRefParse", simple.jobstream.mapreduce.user.walker.wf_qk_ref.SingleJobStream.getJobStream());
		
		// 万方医学期刊
		this.addSubJobStream("WFQKMedParse", simple.jobstream.mapreduce.user.walker.wf_qk_med.SingleJobStream.getJobStream());

		// IOP 期刊解析
		this.addSubJobStream("IOPQKParse", simple.jobstream.mapreduce.site.iopjournal.SingleJobStream.getJobStream());

		// Cambridge 期刊解析
		this.addSubJobStream("CambridgeJournal",
				simple.jobstream.mapreduce.site.cambridge.SingleJobStream.getJobStream());

		// WOS 期刊解析
		this.addSubJobStream("WOSParse", simple.jobstream.mapreduce.site.WOS.SingleJobStream.getJobStream());

		// Science 期刊解析
		this.addSubJobStream("ScienceJournal", simple.jobstream.mapreduce.site.science.SingleJobStream.getJobStream());

		// APS 期刊解析
		this.addSubJobStream("APSJournal", simple.jobstream.mapreduce.site.aps.SingleJobStream.getJobStream());

		// NbuSslibraryParse
		this.addSubJobStream("NbuSslibraryParse",
				simple.jobstream.mapreduce.site.nbusslibrary.SingleJobStream.getJobStream());

		// AIP 期刊解析
		this.addSubJobStream("Aipjournal", simple.jobstream.mapreduce.site.aip.SingleJobStream.getJobStream());

		// tanf_qk
		this.addSubJobStream("TandfQkParse", simple.jobstream.mapreduce.site.tanf_qk.SingleJobStream.TandfQkParse());

		// vip_bz
		this.addSubJobStream("VIPBZParse", simple.jobstream.mapreduce.site.vip_bz.SingleJobStream.VIPBZParse());

		// jstor_qk
		this.addSubJobStream("JstorQKParse", simple.jobstream.mapreduce.site.jstor_qk.SingleJobStream.JstorQKParse());

		// 重庆交大 汇雅图书镜像解析
		this.addSubJobStream("CqjtuSslibraryParse",
				simple.jobstream.mapreduce.site.cqjtusslibrarybook.SingleJobStream.getJobStream());

		// Spring Book 解析
		this.addSubJobStream("StandardSpringerBookDealFunc",
				simple.jobstream.mapreduce.site.springerbook.SingleJobStream.getJobStream());

		// SpringJournal 解析
		this.addSubJobStream("SpringerJournalParse",
				simple.jobstream.mapreduce.site.springer.SingleJobStream.getJobStream());
		
		//ebsco_a9h 解析
		this.addSubJobStream("EBSCOa9hParse", simple.jobstream.mapreduce.site.ebsco_a9h.SingleJobStream.getJobStream());
		//ebsco_aph 解析
		this.addSubJobStream("EBSCOaphParse", simple.jobstream.mapreduce.site.ebsco_aph.SingleJobStream.getJobStream());
		//ebsco_bth 解析
		this.addSubJobStream("EBSCObthParse", simple.jobstream.mapreduce.site.ebsco_bth.SingleJobStream.getJobStream());
		//ebsco_buh 解析
		this.addSubJobStream("EBSCObuhParse", simple.jobstream.mapreduce.site.ebsco_buh.SingleJobStream.getJobStream());
		//ebsco_eric 解析
		this.addSubJobStream("EBSCOericParse", simple.jobstream.mapreduce.site.ebsco_eric.SingleJobStream.getJobStream());
		//ebsco_lxh 解析
		this.addSubJobStream("EBSCOlxhParse", simple.jobstream.mapreduce.site.ebsco_lxh.SingleJobStream.getJobStream());
		//ebsco_ddu 解析
		this.addSubJobStream("EBSCOdduParse", simple.jobstream.mapreduce.site.ebsco_ddu.SingleJobStream.getJobStream());
		//ebsco_8gh 解析
		this.addSubJobStream("EBSCO8ghParse", simple.jobstream.mapreduce.site.ebsco_8gh.SingleJobStream.getJobStream());
		//ebsco_trh 解析
		this.addSubJobStream("EBSCOtrhParse", simple.jobstream.mapreduce.site.ebsco_trh.SingleJobStream.getJobStream());
		//ebsco_cmedm 解析
		this.addSubJobStream("EBSCOcmedmParse", simple.jobstream.mapreduce.site.ebsco_cmedm.SingleJobStream.getJobStream());
		
		//academic 解析
		this.addSubJobStream("academicjournal", simple.jobstream.mapreduce.site.academicjournal.SingleJobStream.getJobStream());
		
		//optics 解析
		this.addSubJobStream("opticsjournal", simple.jobstream.mapreduce.site.opticsjournal.SingleJobStream.getJobStream());

		// RscParse 解析
		this.addSubJobStream("RscParse", simple.jobstream.mapreduce.site.rscbook.SingleJobStream.getJobStream());

		// cnkibs解析
		this.addSubJobStream("cnkibs", simple.jobstream.mapreduce.site.cnkibs.SingleJobStream.getJobStream());

		// EmeraldJournal 解析
		this.addSubJobStream("EmeraldJournal",
				simple.jobstream.mapreduce.site.emeraldjournal.SingleJobStream.getJobStream());

		// 读秀图书 解析
		this.addSubJobStream("DuxiuParse", simple.jobstream.mapreduce.site.duxiuBook.SingleJobStream.getJobStream());

		// musejournal 解析
		this.addSubJobStream("musejournalParse",
				simple.jobstream.mapreduce.site.musejournal.SingleJobStream.getJobStream());

		// 兰州理工汇雅图书 解析
		this.addSubJobStream("GsutSslibraryParse",
				simple.jobstream.mapreduce.site.gsutsslibrary.SingleJobStream.getJobStream());

		// Rsc 期刊解析
		this.addSubJobStream("RscJournal", simple.jobstream.mapreduce.site.rscjournal.SingleJobStream.getJobStream());

		// 书香中国（高校版/西南科大）
		this.addSubJobStream("xnkj", simple.jobstream.mapreduce.site.sxxnkd.SingleJobStream.getJobStream());

		// cnki 标准
		this.addSubJobStream("CNKIBZParse", simple.jobstream.mapreduce.site.cnki_bz.SingleJobStream.CNKIBZParse());

		// cnki 成果
		this.addSubJobStream("CnkiCgParse", simple.jobstream.mapreduce.site.cnki_cg.SingleJobStream.CnkiCgParse());

		// 中国专利 来自江苏
		this.addSubJobStream("CnipaPatent", simple.jobstream.mapreduce.site.cnipapatent.SingleJobStream.cnipaPatent());

		// Bionone 期刊
		this.addSubJobStream("BiooneJournal",
				simple.jobstream.mapreduce.site.bioonejournal.SingleJobStream.getJobStream());

		// 博看
		this.addSubJobStream("BookanQkData", simple.jobstream.mapreduce.site.bookan_hp.SingleJobStream.getJobStream());

		// 劳动关系学院汇雅图书
		this.addSubJobStream("CirrSslibraryParse",
				simple.jobstream.mapreduce.site.ciirSslibrary.SingleJobStream.getJobStream());

		// 重大汇雅图书
		this.addSubJobStream("SslibraryParse",
				simple.jobstream.mapreduce.site.sslibrary.SingleJobStream.getJobStream());

		// 北大法宝法规
		this.addSubJobStream("PkuLawData", simple.jobstream.mapreduce.site.pkulawlaw.SingleJobStream.getJobStream());

		// 北大法意案例
		this.addSubJobStream("LawyeeCase",
				simple.jobstream.mapreduce.site.pkuyeecaselaw.SingleJobStream.getJobStream());

		// 兰州财经 汇雅图书镜像解析
		this.addSubJobStream("LzufeSslibraryParse",
				simple.jobstream.mapreduce.site.lzufesslibrary.SingleJobStream.getJobStream());

		// 万方成果
		this.addSubJobStream("WFCgParse", simple.jobstream.mapreduce.site.wanfang_cg.SingleJobStream.WFCgParse());
		// ei_zt
		this.addSubJobStream("EIZTParse", simple.jobstream.mapreduce.site.ei_zt.SingleJobStream.EIZTParse());
		// jstor_book
		this.addSubJobStream("JstorBookParse",
				simple.jobstream.mapreduce.site.jstor_book.SingleJobStream.getJobStream());

		// 北大法宝案例
		this.addSubJobStream("PkuLawCase", simple.jobstream.mapreduce.site.pkulawcase.SingleJobStream.getJobStream());

		// 畅想之星图书
		this.addSubJobStream("Cxstarbook", simple.jobstream.mapreduce.site.cxstar.SingleJobStream.getJobStream());

		// cnki 少儿期刊解析mirrordqlibckrdjournal
		this.addSubJobStream("mirrordqlibckrdjournal",
				simple.jobstream.mapreduce.site.mirrordqlibckrdjournal.SingleJobStream.getJobStream());

		// aguwileyjournal期刊解析aguwileyjournal
		this.addSubJobStream("aguwileyjournal",
				simple.jobstream.mapreduce.site.aguwileyjournal.SingleJobStream.getJobStream());

		// 国研网
		this.addSubJobStream("Drcnet", simple.jobstream.mapreduce.site.drcnet.SingleJobStream.getJobStream());

		// 超星期刊
		this.addSubJobStream("ChaoxingJournal",
				simple.jobstream.mapreduce.site.chaoxingjournal.SingleJobStream.getJobStream());

		// siamjournal
		this.addSubJobStream("SiamJournal", simple.jobstream.mapreduce.site.siamjournal.SingleJobStream.getJobStream());
		
		this.addSubJobStream("SageJournal", simple.jobstream.mapreduce.site.sagejournal.SingleJobStream.getJobStream());
		
		// pubmed解析
		this.addSubJobStream("PubmedJournal", simple.jobstream.mapreduce.site.pubmed.SingleJobStream.getJobStream());

		// CSSCI期刊
		this.addSubJobStream("CssciJournal", simple.jobstream.mapreduce.site.cssci.SingleJobStream.getJobStream());
		
		// jamajournal 期刊解析
		this.addSubJobStream("jamajournal", simple.jobstream.mapreduce.site.jamajournal.SingleJobStream.getJobStream());
		
		// acsjournal 期刊解析
		this.addSubJobStream("acsjournal", simple.jobstream.mapreduce.site.acsjournal.SingleJobStream.getJobStream());

		// 中经网
		this.addSubJobStream("govceiinfo", simple.jobstream.mapreduce.site.govceiinfo.SingleJobStream.getJobStream());
		
		// MSP 美国数学科学出版社期刊
		this.addSubJobStream("mspjournal", simple.jobstream.mapreduce.site.mspjournal.SingleJobStream.getJobStream());
		
		//万方标准
		this.addSubJobStream("WanfangBz", simple.jobstream.mapreduce.site.wanfang_bz.SingleJobStream.getJobStream());
		
		//naturejournal
		this.addSubJobStream("naturejournal", simple.jobstream.mapreduce.site.naturejournal.SingleJobStream.getJobStream());
		
		//scirpjournal
		this.addSubJobStream("scirpjournal", simple.jobstream.mapreduce.site.scirp.SingleJobStream.getJobStream());
		
		//plosjournal
		this.addSubJobStream("plosjournal", simple.jobstream.mapreduce.site.plosjournal.SingleJobStream.getJobStream());
		
		//皮书资讯
		this.addSubJobStream("PiShuInfo", simple.jobstream.mapreduce.site.pishuinfo.SingleJobStream.getJobStream());
		
		//汇雅图书 海洋信息中心图书馆
		this.addSubJobStream("SealibraryParse", simple.jobstream.mapreduce.site.sealibrary.SingleJobStream.getJobStream());
 
		//scopusjournal
		this.addSubJobStream("scopusjournal", simple.jobstream.mapreduce.site.scopusjournal.SingleJobStream.getJobStream());
 
//		this.addSubJobStream("apajourna", simple.jobstream.mapreduce.site.apajournal.SingleJobStream.getJobStream());
		
		//汇雅图书 陕师大
		this.addSubJobStream("SsdlibraryParse", simple.jobstream.mapreduce.site.ssdSslibrary.SingleJobStream.getJobStream());
		
		//重庆交通大学（超星）
		this.addSubJobStream("cqjtubookParse", simple.jobstream.mapreduce.site.cqjtubook.SingleJobStream.getJobStream());
		
		//重庆交通大学（超星）
		this.addSubJobStream("test",simple.jobstream.mapreduce.user.qianjun.SingleJobStream.getJobStream());
		
		this.addSubJobStream("cniprpatent", simple.jobstream.mapreduce.site.cniprpatent.SingleJobStream.cniprPatent());
		
		// 专利
		this.addSubJobStream("cnki_zl", simple.jobstream.mapreduce.site.cnki_zl.SingleJobStream.getJobStream());
		
		// cnki报纸
		this.addSubJobStream("cnkiccndpaper", simple.jobstream.mapreduce.site.cnkiccndpaper.SingleJobStream.getJobStream());
		
		// 万方专利
		this.addSubJobStream("wanfang_zl", simple.jobstream.mapreduce.site.wanfang_zl.SingleJobStream.getJobStream());
		
		// cnki法规
		this.addSubJobStream("cnki_fg", simple.jobstream.mapreduce.site.cnki_fg.SingleJobStream.getJobStream());
		
		// cnki会议
		this.addSubJobStream("cnki_hy", simple.jobstream.mapreduce.site.cnki_hy.SingleJobStream.getJobStream());
		
		// 万方会议
		this.addSubJobStream("wanfang_hy",simple.jobstream.mapreduce.site.wanfang_hy.SingleJobStream.getJobStream());
		
		// cnki专利
		this.addSubJobStream("cnki_zl",simple.jobstream.mapreduce.site.cnki_zl.SingleJobStream.getJobStream());
		
		//万方博硕
		this.addSubJobStream("wanfang_bs",simple.jobstream.mapreduce.site.wfbs.SingleJobStream.getJobStream());
		//人大报刊
		this.addSubJobStream("rucjournal", simple.jobstream.mapreduce.site.rucjournal.SingleJobStream.getJobStream());

		//新版人大报刊
		this.addSubJobStream("rucjournalnew", simple.jobstream.mapreduce.site.newrucjournal.SingleJobStream.getJobStream());
		
		// 兰州理工大学simple.jobstream.mapreduce.site.gsutsslibrary
		this.addSubJobStream("gsutsslibrary",simple.jobstream.mapreduce.site.gsutsslibrary.SingleJobStream.getJobStream());
		
		// 汇雅图书 广东科技职业技术学院 gditsslibrarybook
		this.addSubJobStream("gditsslibrarybook",simple.jobstream.mapreduce.site.gditsslibrarybook.SingleJobStream.getJobStream());
		
		//wordcount的测试
		this.addSubJobStream("wordcount",simple.jobstream.mapreduce.user.suh.SingleJobStream.getJobStream());
		
		//AIAA会议
		this.addSubJobStream("AIAAHY",simple.jobstream.mapreduce.site.aiaa_meeting.SingleJobStream.getJobStream());
		
		//ASCE会议
		this.addSubJobStream("ASCEHY",simple.jobstream.mapreduce.site.asceProceedings.SingleJobStream.getJobStream());
		 
		//ASCE期刊
		this.addSubJobStream("ascejournal",simple.jobstream.mapreduce.site.ascejournal.SingleJobStream.getJobStream());
		
		//生物医学-博硕论文
		this.addSubJobStream("sinomedthesis",simple.jobstream.mapreduce.site.sinomed_lw.SingleJobStream.getJobStream());
		//生物医学-科普期刊
		this.addSubJobStream("sinomedkpjournal",simple.jobstream.mapreduce.site.sinomed_kp.SingleJobStream.getJobStream());
		//生物医学-中文期刊
		this.addSubJobStream("sinomedzhjournal",simple.jobstream.mapreduce.site.sinomedzhjournal.SingleJobStream.getJobStream());
	}
}
