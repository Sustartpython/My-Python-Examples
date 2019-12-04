package simple.jobstream.mapreduce.site.springerbook;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String rootDir = "";
		
		String rawDataDir = "/RawData/springer/springerbook/big_htm/20190717"; // 带解析新数据路径
		String rawDataXXXXObjectDir = "/RawData/springer/springerbook/xxxxobject"; // 新数据解析后的XXXXObject格式数据路径
		String latest_tempDir = "/RawData/springer/springerbook/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/springer/springerbook/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用
		String new_data_xxxxobject = "/RawData/springer/springerbook/new_data/xxxxobject"; // 去重后得到的新数据
		String new_data_stdDir = "/RawData/springer/springerbook/new_data/stdFile"; // 新数据转换为DB3格式存放目录
	

//
		JobNode Html2XXXXObject = new JobNode("Springerbook", rootDir, 0,
				"simple.jobstream.mapreduce.site.springerbook.Html2xxxxobject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawDataDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawDataXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
//
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("Springerbook.MergeXXXXObject2Temp",
				rawDataXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("Springerbook.GenNewData", 
				rawDataXXXXObjectDir, latest_tempDir,new_data_xxxxobject, 200);		

		// 导出数据到 db3
		// 导出数据到 db3
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("Springerbook.Std","simple.jobstream.mapreduce.site.springerbook.StdXXXXObject", 
				rawDataXXXXObjectDir, new_data_stdDir, "springerbook","/RawData/_rel_file/zt_template.db3",1);;

//		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("Springerbook.Temp2Latest", latest_tempDir, latestDir);
	

		// 正常更新
		result.add(Html2XXXXObject);	
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdNew);
		StdNew.addChildJob(Temp2Latest);
		
		// 测试
//		JobNode StdXXXXObject= new JobNode("SageJournalParse", rootDir, 0, "simple.jobstream.mapreduce.site.sagejournal.StdXXXXObject");
//		StdXXXXObject.setConfig("inputHdfsPath", latestDir);
//		StdXXXXObject.setConfig("outputHdfsPath", stdDir);
//		StdXXXXObject.setConfig("reduceNum", "10");
//		result.add(StdXXXXObject);
		

		return result;
	}

}
