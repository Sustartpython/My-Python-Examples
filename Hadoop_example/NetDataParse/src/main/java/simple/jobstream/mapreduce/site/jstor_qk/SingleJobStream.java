package simple.jobstream.mapreduce.site.jstor_qk;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> JstorQKParse(){
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/jstor/qk/big_json/2019/20190712";
		String rawXXXXObjectDir = "/RawData/jstor/qk/XXXXObject";
		String latest_tempDir = "/RawData/jstor/qk/latest_temp";	//临时成品目录
		String latestDir = "/RawData/jstor/qk/latest";	//成品目录
		String newXobjDir = "/RawData/jstor/qk/new_data";	
		String db3Dir = "/RawData/jstor/qk/Stdqk";
		
		
		/**/
		JobNode html2XXXXObject = new JobNode("EIJsonParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.jstor_qk.jstorJson2XXXXObject3");		
		html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("EIJsonParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.jstor_qk.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		JobNode GenNewData  = new JobNode("EIJsonParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.jstor_qk.GenNewData");
		GenNewData.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latest_tempDir);
		GenNewData.setConfig("outputHdfsPath", newXobjDir);
		
		JobNode GenUpdateData  = new JobNode("EIJsonParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.jstor_qk.GenUpdateData");
		GenUpdateData.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		GenUpdateData.setConfig("outputHdfsPath", newXobjDir);
		
		JobNode Std2Db3 = new JobNode("EIJsonParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.jstor_qk.Std2Db3");		
		Std2Db3.setConfig("inputHdfsPath", newXobjDir);
		Std2Db3.setConfig("outputHdfsPath", db3Dir);
		Std2Db3.setConfig("reduceNum", "1");
		
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("EIJsonParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.jstor_qk.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		
		
//		result.add(html2XXXXObject);
//		Std2Db3.setConfig("inputHdfsPath", rawXXXXObjectDir);
//		Std2Db3.setConfig("reduceNum", "2");
//		html2XXXXObject.addChildJob(Std2Db3);
		
		//正常更新
		// 更改了代码 记得重新全部运行一次 现在没时间运行
		//添加备份了contentIssue 但还没有运行
		result.add(html2XXXXObject);
		html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2Db3);
		Std2Db3.addChildJob(Temp2Latest);
		
//		Std2Db3.setConfig("inputHdfsPath", latestDir);
//		result.add(Std2Db3);
//		Std2Db3.addChildJob(Temp2Latest);
		
		
		//由于解析更改后的update
//		result.add(html2XXXXObject);
//		html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenUpdateData);
//		Std2Db3.setConfig("reduceNum", "1");
//		GenUpdateData.addChildJob(Std2Db3);
//		Std2Db3.addChildJob(Temp2Latest);
		
//		Std2Db3.setConfig("inputHdfsPath", latestDir);
//		result.add(Std2Db3);
		
//		result.add(MergeXXXXObject2Temp);
//		Std2Db3.setConfig("inputHdfsPath", latest_tempDir);
//		Std2Db3.setConfig("reduceNum", "1");
//		MergeXXXXObject2Temp.addChildJob(Std2Db3);
	
		
		return result;
	}
	
}
