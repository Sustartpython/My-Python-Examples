package simple.jobstream.mapreduce.site.rucjournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		
		String rawHtmlDir = "/RawData/ruc/exuezhe/big_htm/20190717";
		String rawXXXXObjectDir = "/RawData/ruc/exuezhe/xxxxobject";
		String latest_tempDir = "/RawData/ruc/exuezhe/latest_temp";	//临时成品目录
		String latestDir_all = "/RawData/ruc/exuezhe/latest_all";	//成品目录
		String newXXXXObjectDir = "/RawData/ruc/exuezhe/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/ruc/exuezhe/new_data/StdQk";
		String stdDirnew = "/RawData/ruc/exuezhe/new_data/StdQknew";
		String stdDirAtable = "/RawData/ruc/exuezhe/new_data/StdQkA";
		String stdDirAtablenew = "/RawData/ruc/exuezhe/new_data/StdQkAnew";
		String latestDir = "/RawData/ruc/exuezhe/latest"; // a表目录
		

		JobNode Html2XXXXObject =JobNodeModel.getJobNode4Parse2XXXXObject(
				"rucjournalhtml2xxxxobject",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.rucjournal.Html2XXXXObject",
				rawHtmlDir,
				rawXXXXObjectDir,
				100);

		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject(
				"rucjournalMergeXXXXObj", 
				rawXXXXObjectDir, 
				latestDir_all, 
				latest_tempDir,
				100);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject(
				"rucjournalGenNew",
				rawXXXXObjectDir,
				latest_tempDir,
				newXXXXObjectDir,
				100);
		
		
//		JobNode StdDb3ZLF = JobNodeModel.getJobNode4Std2Db3(
//				"rucjournalstdDb3",
//				"simple.jobstream.mapreduce.site.rucjournal.StdZLF", 
//				newXXXXObjectDir, 
//				stdDir, 
//				"ezuezhe",
//				"/RawData/exuezhe/template/exuezhe_zlf_template.db3",
//				10);
		
		
		JobNode StdDb3ZT = JobNodeModel.getJobNode4Std2Db3(
				"rucjournalstdDb3",
				"simple.jobstream.mapreduce.site.rucjournal.StdZT", 
				newXXXXObjectDir, 
				stdDir, 
				"ezuezhe",
				"/RawData/_rel_file/zt_template.db3",
				1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject(
				"rucjournal.Temp2Latest",
				latest_tempDir,
				latestDir_all);
		
		JobNode XXXXobject2Atable =JobNodeModel.getJobNode4Parse2XXXXObject(
				"rucjournaltoatable",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.rucjournal.Last2A",
				latestDir_all,
				latestDir,
				100);
		
		
		JobNode StdDb3A = JobNodeModel.getJobNode4Std2Db3(
				"rucjournal.StdDb3A",
				"simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				latestDir, 
				stdDirAtable,
				"rucjournal",
				"/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",
				2);
	
//		result.add(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdDb3ZT);
//		StdDb3ZT.addChildJob(Temp2Latest);
//		Temp2Latest.addChildJob(XXXXobject2Atable);
//		XXXXobject2Atable.addChildJob(StdDb3A);
		
//		JobNode StdDb3ZTlatest = JobNodeModel.getJobNode4Std2Db3(
//				"rucjournalstdDb3",
//				"simple.jobstream.mapreduce.site.rucjournal.StdZT", 
//				latestDir_all, 
//				stdDir, 
//				"ezuezhe",
//				"/RawData/ruc/exuezhe/template/exuezhe_zt_template.db3",
//				1);
		
		
		JobNode StdDb3ZTnew = JobNodeModel.getJobNode4Std2Db3(
				"rucjournalstdDb3new",
				"simple.jobstream.mapreduce.site.rucjournal.StdZT_new", 
				latestDir_all, 
				stdDirnew, 
				"rdfybkjournal",
				"/RawData/_rel_file/zt_template.db3",
				1);
//		
//		XXXXobject2Atable.addChildJob(StdDb3A);
//		result.add(StdDb3ZTnew);
		
		
		JobNode StdDb3Anew = JobNodeModel.getJobNode4Std2Db3(
				"rucjournal.StdDb3A",
				"simple.jobstream.mapreduce.site.rucjournal.Std2Db3A_new", 
				latestDir, 
				stdDirAtablenew,
				"rdfybkjournal",
				"/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",
				1);
		//result.add(StdDb3Anew);
		
		//导出all
		JobNode StdDb3ZTall = JobNodeModel.getJobNode4Std2Db3(
				"rucjournalstdDb3",
				"simple.jobstream.mapreduce.site.rucjournal.StdZT", 
				latestDir_all, 
				stdDir, 
				"ezuezhe",
				"/RawData/_rel_file/zt_template.db3",
				1);
		
		result.add(StdDb3ZTall);
		return result;
		
		

		
	}

}
