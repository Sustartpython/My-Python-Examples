package simple.jobstream.mapreduce.site.cnki_fg;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String  defaultRootDir = "";
		String rawHtmlDir = "/RawData/cnki/fg/big_htm/big_htm_20190702";//文件输入目录
		String rawXXXXObjectDir = "/RawData/cnki/fg/xobj";//新XXXXOBj 输出目录
		String latest_tempDir = "/RawData/cnki/fg/latest_temp";	//新数据合并老数据后生成，新一轮的总数据存放地址
		String latestDir = "/RawData/cnki/fg/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/cnki/fg/newobj";//本次新增的数据（XXXXObject）
		String stdDirZT = "/RawData/cnki/fg/db";//智图DB3目录
			
		
		JobNode fg_xxobj = new JobNode("cnki_fg", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cnki_fg.XXXXObject_cnkifg");		
		fg_xxobj.setConfig("inputHdfsPath",rawHtmlDir);
		fg_xxobj.setConfig("outputHdfsPath",rawXXXXObjectDir);
		
		// 将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("cnki_fg.MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("cnki_fg.GenNewData", rawXXXXObjectDir,
				latest_tempDir, newXXXXObjectDir, 200);
		// 生成db3
		JobNode fg_db = new JobNode("cnki_fg", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cnki_fg.cnki_fgdb2_new");		
		fg_db.setConfig("inputHdfsPath",rawXXXXObjectDir);
		fg_db.setConfig("outputHdfsPath", stdDirZT);
		
		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("cnki_hy.Temp2Latest", latest_tempDir,
				latestDir);
		// 正常更新
		result.add(fg_xxobj);
		fg_xxobj.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(fg_db);
		fg_db.addChildJob(Temp2Latest);
	
//		JobNode fg_db_all = new JobNode("cnki_fg", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.cnki_fg.cnki_fgdb2_new");		
//		fg_db_all.setConfig("inputHdfsPath", "/RawData/cnki/fg/latest");
//		fg_db_all.setConfig("outputHdfsPath", "/RawData/cnki/fg/db");
//		result.add(fg_db_all);
//		
		return result;
	}
}
