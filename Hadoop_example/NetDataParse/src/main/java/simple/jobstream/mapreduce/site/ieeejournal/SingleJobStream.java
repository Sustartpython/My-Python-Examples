package simple.jobstream.mapreduce.site.ieeejournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/CQU/ieee/big_htm/big_json_20181024";
		//String rawHtmlDir2 = "/RawData/CQU/ieee/big_htm/big_json_20170314";//文件输入目录
		String rawXXXXObjectDir = "/RawData/CQU/ieee/XXXXObject";//新XXXXOBj 输出目录
		String latest_tempDir = "/RawData/CQU/ieee/latest_temp";//新数据合并老数据后生成，新一轮的总数据存放地址
		String latestDir = "/RawData/CQU/ieee/latest";//历史数据
		String newXXXXObjectDir = "/RawData/CQU/ieee/new_data";//本次新增的数据（XXXXObject）
		String stdhyDir = "/RawData/CQU/ieee/stdieee";//DB3目录
		//解析big_json
	    JobNode	XXXXObjectHY = new JobNode("ieeeParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.ieeejournal.JsonXXXXObjectIEEE");
	    XXXXObjectHY.setConfig("inputHdfsPath", rawHtmlDir+","+rawHtmlDir);
		XXXXObjectHY.setConfig("outputHdfsPath", rawXXXXObjectDir);
		//将历史累积数据和新数据合并去重
		JobNode MergehyXXXXObject2Temp = new JobNode("ieeeParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.ieeejournal.MergehyXXXXObject2Temp");
		MergehyXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergehyXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("ieeeParse", defaultRootDir, 
				0,  "simple.jobstream.mapreduce.site.ieeejournal.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		//生成DB3
		JobNode StdIEEE = new JobNode("ieeeParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.ieeejournal.StdIEEE");
		StdIEEE.setConfig("inputHdfsPath", rawXXXXObjectDir);
		StdIEEE.setConfig("outputHdfsPath", stdhyDir);

		//备份累积数据
		JobNode  Temp2Latest = new JobNode("ieeeParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.ieeejournal.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		//正常更新
		result.add(XXXXObjectHY);
		//StdIEEE.addChildJob(Temp2Latest);
		XXXXObjectHY.addChildJob(MergehyXXXXObject2Temp);
		MergehyXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdIEEE);
		StdIEEE.addChildJob(Temp2Latest);
		return result;
	}
}
