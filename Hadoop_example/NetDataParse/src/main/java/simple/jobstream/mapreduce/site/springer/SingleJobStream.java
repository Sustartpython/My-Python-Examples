package simple.jobstream.mapreduce.site.springer;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String rootDir = "";
		// "/RawData/springer/springerjournal/big_json/2018/20181012","/RawData/springer/springerjournal/big_json/2018/20181019",
		// "/RawData/springer/springerjournal/big_json/2018/20181024","/RawData/springer/springerjournal/big_json/2018/20181102",
		// "/RawData/springer/springerjournal/big_json/2018/20181109","/RawData/springer/springerjournal/big_json/2018/20181116",
		// "/RawData/springer/springerjournal/big_json/2018/20181123","/RawData/springer/springerjournal/big_json/2018/20181130",
		// "/RawData/springer/springerjournal/big_json/2018/20181207","/RawData/springer/springerjournal/big_json/2018/20181214",
		// String rawDataDir =
		// "/RawData/springer/springerjournal/big_json/2018/20181012,/RawData/springer/springerjournal/big_json/2018/20181019,/RawData/springer/springerjournal/big_json/2018/20181024,/RawData/springer/springerjournal/big_json/2018/20181102,/RawData/springer/springerjournal/big_json/2018/20181109,/RawData/springer/springerjournal/big_json/2018/20181116,/RawData/springer/springerjournal/big_json/2018/20181123,/RawData/springer/springerjournal/big_json/2018/20181130,/RawData/springer/springerjournal/big_json/2018/20181207,/RawData/springer/springerjournal/big_json/2018/20181214";
		// // 带解析新数据路径
		String rawDataDir = "/RawData/springer/springerjournal/big_json/2019/20190712";
		String rawDataXXXXObjectDir = "/RawData/springer/springerjournal/xxxxobject"; // 新数据解析后的XXXXObject格式数据路径
		String latest_tempDir = "/RawData/springer/springerjournal/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/springer/springerjournal/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用

		String new_data_xxxxobject = "/RawData/springer/springerjournal/new_data/xxxxobject"; // 去重后得到的新数据
		String new_data_stdDir = "/RawData/springer/springerjournal/new_data/stdfile"; // 新数据转换为DB3格式存放目录

		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("SpringerJournal.Json2XXXXObject",
				DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.springer.Html2XXXXObject",
				rawDataDir, rawDataXXXXObjectDir, 10);

		// 将历史累积数据和新数据合并去重

		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("SpringerJournal.MergeXXXXObject2Temp",
				rawDataXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("SpringerJournal.GenNewData",
				rawDataXXXXObjectDir, latest_tempDir, new_data_xxxxobject, 200);
//
//		// 导出数据到 db3
		JobNode StdXXXXObject = JobNodeModel.getJobNode4Std2Db3("SpringerJournal.Std",
				"simple.jobstream.mapreduce.site.springer.StdSpringer", new_data_xxxxobject, new_data_stdDir,
				"SpringerJournal", "/RawData/_rel_file/zt_template.db3", 1);

		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("SpringerJournal.Temp2Latest", latest_tempDir,
				latestDir);

		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdXXXXObject);
		StdXXXXObject.addChildJob(Temp2Latest);

		// 单次测试
		//result.add(Json2XXXXObject);
		//Json2XXXXObject.addChildJob(StdXXXXObject);

		// 只导出DB3
		// 用于单点测试，直接导出db3,直接导出智图，更换了A层lngid
//		JobNode StdXXXXObjecttest = JobNodeModel.getJobNode4Std2Db3("SpringerJournal.Std",
//		"simple.jobstream.mapreduce.site.springer.StdSpringerClear", new_data_xxxxobject, new_data_stdDir,
//		"SpringerJournal", "/RawData/_rel_file/zt_template.db3", 1);
//		result.add(StdXXXXObjecttest);
		
		// 清理latest中没有title的数据
//		String latestDirClear = "/RawData/springer/springerjournal/latestClear";
//		String task = "simple.jobstream.mapreduce.site.springer.DealLatest";
//		JobNode clearLatest= new JobNode("clearLatest", rootDir, 0,task);
//		clearLatest.setConfig("inputHdfsPath", latestDir);
//		clearLatest.setConfig("outputHdfsPath", latestDirClear);
//		result.add(clearLatest);
		
		// 根据条件导出具体的某一条
//		JobNode StdXXXXObjecttest = JobNodeModel.getJobNode4Std2Db3("SpringerJournal.Std",
//		"simple.jobstream.mapreduce.site.springer.StdSpringer2DelJname", latestDir, new_data_stdDir,
//		"SpringerJournal", "/RawData/_rel_file/zt_template.db3", 4);
//		result.add(StdXXXXObjecttest);
		
		// 提出jid,jname,jid_url
//		JobNode springerInfoParse = new JobNode("springerInfo", rootDir, 0,
//				"simple.jobstream.mapreduce.site.springer.ExportJouranl");
//		springerInfoParse.setConfig("inputHdfsPath", "/RawData/springer/springerjournal/latest");
//		springerInfoParse.setConfig("outputHdfsPath", "/user/qianjun/output/springerjournalInfo");
//
//		result.add(springerInfoParse);
		
		// 根据年份导出db3
//		// 导出数据到 db3
//		JobNode StdXXXXObjectByYear = JobNodeModel.getJobNode4Std2Db3("SpringerJournal.Std",
//				"simple.jobstream.mapreduce.site.springer.StdSpringerPort", latestDir, new_data_stdDir,
//				"SpringerJournal", "/RawData/_rel_file/zt_template.db3", 1);
//		result.add(StdXXXXObjectByYear);

		return result;
	}

}
