package simple.jobstream.mapreduce.site.sealibrary;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		String rawHtmlDir = "/RawData/chaoxing/sslibrary/nmdissslibrarybook/big_json/2019/20190704";
		String rawXXXXObjectDir = "/RawData/chaoxing/sslibrary/nmdissslibrarybook/xxxxobject";
		String latest_tempDir = "/RawData/chaoxing/sslibrary/nmdissslibrarybook/latest_temp";
		String latestDir = "/RawData/chaoxing/sslibrary/nmdissslibrarybook/latest";
		String newXXXXObjectDir = "/RawData/chaoxing/sslibrary/nmdissslibrarybook/new_data/xxxxobject";
		String stdDir = "/RawData/chaoxing/sslibrary/nmdissslibrarybook/new_data/StdDir";
		
//		String rawDataDir = "/RawData/sealibrary/big_json/2019/20190403"; // 带解析新数据路径
//		String rawDataXXXXObjectDir = "/RawData/sealibrary/xxxxobject"; // 新数据解析后的XXXXObject格式数据路径
//
//		
//		String latest_tempDir = "/RawData/agujournal/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
//		String latestDir = "/RawData/agujournal/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用
//
//		String new_data_xxxxobject = "/RawData/agujournal/new_data/xxxxobject"; // 去重后得到的新数据
//		String new_data_stdDir = "/RawData/agujournal/new_data/stdFile"; // 新数据转换为DB3格式存放目录

		JobNode Json2XXXXObject = new JobNode("sealibrary", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.sealibrary.Json2XXXXObject");
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("nmdissslibrarybook.MergeXXXXObject2Temp",rawXXXXObjectDir, latestDir, latest_tempDir, 10);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("nmdissslibrarybook.GenNewData", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 10);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("nmdissslibrarybook.Temp2Latest", latest_tempDir, latestDir);
		
		
		JobNode StdSealibrary = new JobNode("sealibrary", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.sealibrary.StdSslibrary");
		StdSealibrary.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdSealibrary.setConfig("outputHdfsPath", stdDir);
		
//
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdSealibrary);
		StdSealibrary.addChildJob(Temp2Latest);
//      直接导出db3
//		JobNode StdSealibrarytest = new JobNode("sealibrary", defaultRootDir, 0,
//				"simple.jobstream.mapreduce.site.sealibrary.StdSslibrary");
//		 StdSealibrarytest.setConfig("inputHdfsPath", rawXXXXObjectDir);
//		 StdSealibrarytest.setConfig("outputHdfsPath", stdDir);
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(StdSealibrarytest);
//		result.add(MergeXXXXObject2Temp);
		return result;
	}
}
