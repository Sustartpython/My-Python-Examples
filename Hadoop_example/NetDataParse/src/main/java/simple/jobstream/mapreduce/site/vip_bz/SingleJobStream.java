package simple.jobstream.mapreduce.site.vip_bz;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> VIPBZParse()
	{
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/cqvip/bz/big_json/2019/20190717";
		String rawXXXXObjectDir = "/RawData/cqvip/bz/XXXXObject";
		String latest_tempDir = "/RawData/cqvip/bz/latest_temp";	//临时成品目录
		String latestDir = "/RawData/cqvip/bz/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/cqvip/bz/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/cqvip/bz/new_data/StdZLFBZ";
		

		JobNode Html2XXXXObject = new JobNode("VIPBZParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.vip_bz.Json2XXXXObject");		
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("VIPBZParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.vip_bz.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("VIPBZParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.vip_bz.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdVIPBZ = new JobNode("VIPBZParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.vip_bz.StdZLFBZ");		
		StdVIPBZ.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdVIPBZ.setConfig("outputHdfsPath", stdDir);
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("VIPBZParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.vip_bz.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		
		//*正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdVIPBZ);
		StdVIPBZ.addChildJob(Temp2Latest);
		/*/
		

		//导出完整数据到db3
		StdVIPBZ.setConfig("inputHdfsPath", latestDir);
		StdVIPBZ.setConfig("outputHdfsPath", stdDir);
		result.add(StdVIPBZ);
		//*/
		
		return result;
	}	
}
