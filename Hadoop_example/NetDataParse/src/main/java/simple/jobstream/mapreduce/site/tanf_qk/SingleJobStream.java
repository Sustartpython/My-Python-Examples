package simple.jobstream.mapreduce.site.tanf_qk;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> TandfQkParse(){
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/tandfjournal/qk/big_json/2019/20190708";
		String rawXXXXObjectDir = "/RawData/tandfjournal/qk/XXXXObject";
		String latest_tempDir = "/RawData/tandfjournal/qk/latest_temp";	//临时成品目录
		String latestDir = "/RawData/tandfjournal/qk/latest";	//成品目录
		String newXobjDir = "/RawData/tandfjournal/qk/new_data";
		String db3Dir = "/RawData/tandfjournal/qk/stdqk";
		
		/**/
		JobNode html2XXXXObject = new JobNode("EIJsonParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.tanf_qk.tanfJson2XXXXObject2");		
		html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("EIJsonParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.tanf_qk.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		JobNode GenNewData  = new JobNode("EIJsonParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.tanf_qk.GenNewData");
		GenNewData.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latest_tempDir);
		GenNewData.setConfig("outputHdfsPath", newXobjDir);
		
		JobNode Std2Db3 = new JobNode("EIJsonParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.tanf_qk.Std2Db3");		
		Std2Db3.setConfig("inputHdfsPath", newXobjDir);
		Std2Db3.setConfig("outputHdfsPath", db3Dir);
		Std2Db3.setConfig("reduceNum", "1");
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("EIJsonParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.tanf_qk.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		
		result.add(html2XXXXObject);
		html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2Db3);
		Std2Db3.addChildJob(Temp2Latest);
		
//		Std2Db3.setConfig("inputHdfsPath", latestDir);
//		Std2Db3.setConfig("reduceNum", "1");
//		result.add(Std2Db3);
		
		return result;
	}
}
