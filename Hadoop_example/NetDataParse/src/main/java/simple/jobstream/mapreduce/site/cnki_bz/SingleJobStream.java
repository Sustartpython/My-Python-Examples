package simple.jobstream.mapreduce.site.cnki_bz;

import java.util.LinkedHashSet;

import org.apache.commons.collections.map.StaticBucketMap;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> CNKIBZParse()
	{
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		

		String rawHtmlDir = "/RawData/cnki/bz/big_htm/2019/20190702";
		String rawXXXXObjectDir = "/RawData/cnki/bz/bzXXXXObject";
		String latest_tempDir = "/RawData/cnki/bz/latest_temp";	//临时成品目录
		String latestDir = "/RawData/cnki/bz/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/cnki/bz/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/cnki/bz/new_data/StdBZ";
		String latestDir_new = "/RawData/cnki/bz/latest_new";

		JobNode Html2XXXXObject = new JobNode("CNKIBZParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cnki_bz.CnkiHtml2XXXXObject");		
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("CNKIBZParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cnki_bz.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("CNKIBZParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cnki_bz.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdCNKIBZ = new JobNode("CNKIBZParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cnki_bz.StdZLFBZ");		
		StdCNKIBZ.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdCNKIBZ.setConfig("outputHdfsPath", stdDir);
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("CNKIBZParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cnki_bz.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		//*
		//正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdCNKIBZ);
		StdCNKIBZ.addChildJob(Temp2Latest);
		/*/
		

		//导出数据到db3
		StdCNKIBZ.setConfig("inputHdfsPath", latestDir);
		StdCNKIBZ.setConfig("outputHdfsPath", stdDir);
		result.add(StdCNKIBZ);
		//*/
		
//		
//		JobNode StdCNKIBZ2 = new JobNode("CNKIBZParse", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.cnki_bz.StdBZZT");		
//		StdCNKIBZ2.setConfig("outputHdfsPath", stdDir);
//		//导出完整数据到db3
//		StdCNKIBZ.setConfig("inputHdfsPath", latestDir_new);
//		result.add(StdCNKIBZ);


		
		//清理失效数据 目前已清理并将latestDir_new目录改为了 latestDir目录
//		JobNode Temp2Latest_delete = new JobNode("CNKIBZParse", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.cnki_bz.Temp2Latest_delete");
//		Temp2Latest_delete.setConfig("inputHdfsPath", latestDir);
//		Temp2Latest_delete.setConfig("outputHdfsPath", latestDir_new);
//		
//		result.add(Temp2Latest_delete);
//		StdCNKIBZ.setConfig("inputHdfsPath", latestDir);
//		result.add(StdCNKIBZ);
		
		return result;
	}	
}
