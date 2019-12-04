package simple.jobstream.mapreduce.site.cnki_cg;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;
//static String
public class SingleJobStream {
	public static LinkedHashSet<JobNode> CnkiCgParse()
	{
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/cnki/cg/big_htm/20190627";
		String rawXXXXObjectDir = "/RawData/cnki/cg/XXXXObject";
		String latest_tempDir = "/RawData/cnki/cg/latest_temp";	//临时成品目录
		String latestDir = "/RawData/cnki/cg/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/cnki/cg/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/cnki/cg/new_data/cgdb3";

		JobNode Html2XXXXObject = new JobNode("CnkiCgData", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cnki_cg.CnkiHtml2XXXXObject");		
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("CnkiCgData", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cnki_cg.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("CnkiCgData", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cnki_cg.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdDb3 = new JobNode("CnkiCgData", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cnki_cg.StdZTCG");	
		StdDb3.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdDb3.setConfig("outputHdfsPath", stdDir);
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("CnkiCgData", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cnki_cg.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		//*
		//正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		/*/
		
		//导出数据到db3
		StdDb3.setConfig("inputHdfsPath", latestDir);
		StdDb3.setConfig("outputHdfsPath", stdDir);
		result.add(StdDb3);
		//*/
//		StdDb3.setConfig("inputHdfsPath", latestDir);
//		StdDb3.setConfig("outputHdfsPath", stdDir);
//		result.add(StdDb3);

		
		return result;
	}
}
