package simple.jobstream.mapreduce.site.sagejournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String rootDir = "";
		
		//String rawDataDir = "/RawData/sagejournal/big_json/2018/20181026,/RawData/sagejournal/big_json/2018/20181025,/RawData/sagejournal/big_json/2018/20181015,/RawData/sagejournal/big_json/2018/20181018,/RawData/sagejournal/big_json/2018/20181019"; 
		String rawDataDir = "/RawData/sagejournal/big_json/2019/20190705";
		String rawDataXXXXObjectDir = "/RawData/sagejournal/xxxxobject"; // 新数据解析后的XXXXObject格式数据路径

		
		String latest_tempDir = "/RawData/sagejournal/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/sagejournal/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用

		String newXXXXObjectDir = "/RawData/sagejournal/new_data/xxxxobject"; // 去重后得到的新数据
		String stdDir = "/RawData/sagejournal/new_data/stdFile"; // 新数据转换为DB3格式存放目录

//		老版本
//		JobNode Html2XXXXObject = new JobNode("SageJournal", rootDir, 0,
//				"simple.jobstream.mapreduce.site.sagejournal.Jsonxxxxobject");
//		Html2XXXXObject.setConfig("inputHdfsPath", rawDataDir);
//		Html2XXXXObject.setConfig("outputHdfsPath", rawDataXXXXObjectDir);
		
		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("SageJournal.Json2XXXXObject",
				DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.sagejournal.Jsonxxxxobject",
				rawDataDir, rawDataXXXXObjectDir, 10);

		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("SageJournal.MergeXXXXObject2Temp",
				rawDataXXXXObjectDir, latestDir, latest_tempDir, 200);

		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("SageJournal.GenNewData", 
				rawDataXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 200);		

		
		// 导出数据到 db3
		JobNode StdXXXXObject = JobNodeModel.getJobNode4Std2Db3("SageJournaJournal.Std",
				"simple.jobstream.mapreduce.site.sagejournal.StdXXXXObject", newXXXXObjectDir, stdDir,
				"SageJournal", "/RawData/_rel_file/zt_template.db3", 1);


		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("SageJournal.Temp2Latest", latest_tempDir, latestDir);
	

		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdXXXXObject);
		StdXXXXObject.addChildJob(Temp2Latest);
		
		// 测试
//		JobNode StdXXXXObject= new JobNode("SageJournalParse", rootDir, 0, "simple.jobstream.mapreduce.site.sagejournal.StdXXXXObject");
//		StdXXXXObject.setConfig("inputHdfsPath", latestDir);
//		StdXXXXObject.setConfig("outputHdfsPath", stdDir);
//		StdXXXXObject.setConfig("reduceNum", "10");
//		result.add(StdXXXXObject);
		

		return result;
	}

}
