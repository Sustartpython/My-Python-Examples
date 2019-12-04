package simple.jobstream.mapreduce.site.mirrordqlibckrdjournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		

		
		String rawDataDir = "/RawData/mirrordqlibckrdjournal/big_json/2018/20181205"; // 带解析新数据路径
		String rawDataXXXXObjectDir = "/RawData/mirrordqlibckrdjournal/xxxxobject"; // 新数据解析后的XXXXObject格式数据路径
		String latest_tempDir = "/RawData/mirrordqlibckrdjournal/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/mirrordqlibckrdjournal/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用

		String new_data_xxxxobject = "/RawData/mirrordqlibckrdjournal/new_data/xxxxobject"; // 去重后得到的新数据
		String new_data_stdDir = "/RawData/mirrordqlibckrdjournal/new_data/stdfile"; // 新数据转换为DB3格式存放目录



		JobNode Json2XXXXObject = new JobNode("mirrordqlibckrdjournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.mirrordqlibckrdjournal.Json2XXXXObject");		
		Json2XXXXObject.setConfig("inputHdfsPath", rawDataDir);
		Json2XXXXObject.setConfig("outputHdfsPath",  rawDataXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
//		JobNode MergeXXXXObject2Temp  = new JobNode("PkuLawCase", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.pkulawcase.MergeXXXXObject2Temp");
//		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
//		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("mirrordqlibckrdjournal.MergeXXXXObject2Temp",  rawDataXXXXObjectDir, latestDir, latest_tempDir, 100);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("mirrordqlibckrdjournal.GenNewData", rawDataXXXXObjectDir, latest_tempDir, new_data_xxxxobject, 100);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("mirrordqlibckrdjournal.Temp2Latest", latest_tempDir, latestDir);
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
//		JobNode GenNewData = new JobNode("PkuLawCase", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.pkulawcase.GenNewData");
//		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
//		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("mirrordqlibckrdjournal.Std","simple.jobstream.mapreduce.site.mirrordqlibckrdjournal.StdXXXXObject", 
				rawDataXXXXObjectDir,new_data_stdDir, "mirrordqlibckrdjournal","/RawData/_rel_file/zt_template.db3",2);
		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
		
		
		return result;
	}
}
