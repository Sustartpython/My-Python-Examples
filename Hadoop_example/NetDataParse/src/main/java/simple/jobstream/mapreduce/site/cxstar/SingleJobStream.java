package simple.jobstream.mapreduce.site.cxstar;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/cxstar/big_json/2019/20190128";
		String rawXXXXObjectDir = "/RawData/cxstar/XXXXObject";
		String latest_tempDir = "/RawData/cxstar/latest_temp";	//临时成品目录
		String latestDir = "/RawData/cxstar/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/cxstar/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/cxstar/new_data/StdCXstar";

		JobNode Json2XXXXObject = new JobNode("Cxstarbook", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cxstar.Json2XXXXObject");		
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("cxstar.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 5);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("cxstar.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 5);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("cxstar.Copy", latest_tempDir, latestDir);

		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("cxstar.Std","simple.jobstream.mapreduce.site.cxstar.StdCXstar", 
				newXXXXObjectDir, stdDir, "cxstar","/RawData/_rel_file/zt_template.db3",1);
								
		
		JobNode First2Latest = new JobNode("cxstar", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cxstar.Temp2Latest");
		First2Latest.setConfig("inputHdfsPath", rawXXXXObjectDir);
		First2Latest.setConfig("outputHdfsPath", latestDir);
		
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
