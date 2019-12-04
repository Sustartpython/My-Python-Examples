package simple.jobstream.mapreduce.site.naturejournal;

import java.util.LinkedHashSet;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		JobNode Html2XXXXObject = new JobNode("RscParse", defaultRootDir,0, "simple.jobstream.mapreduce.site.naturejournal.json2xobj");
		Html2XXXXObject.setConfig("inputHdfsPath","/RawData/nature/big_json/20181218");
		Html2XXXXObject.setConfig("outputHdfsPath","/RawData/nature/xxobj");
		
		JobNode Std = new JobNode("RscParse", defaultRootDir, 0,"simple.jobstream.mapreduce.site.naturejournal.nature_journal_db");
		Std.setConfig("inputHdfsPath","/RawData/nature/xxobj");
		Std.setConfig("outputHdfsPath","/RawData/nature/db");
		
		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("naturejournal.Json2XXXXObject",
		   DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.naturejournal.Json2XXXXObject",
		   "/RawData/nature/big_json/20181218", "/RawData/nature/xxobj_a", 40);
		
		/*JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3A4QK("naturejournal.Std2Db3A", 
				      "/RawData/nature/xxobj_a", "/RawData/nature/db_a",  "base_obj_meta_a_qk.nature","/RawData/_rel_file/base_obj_meta_a_template_qk.db3", 3);
				 
		*/
		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("naturejournal.Std2Db3A","simple.jobstream.mapreduce.common.vip.Std2Db3A4QKWX" ,
			      "/RawData/nature/xxobj_a", "/RawData/nature/db_a",  "base_obj_meta_a_qk.nature","/RawData/_rel_file/base_obj_meta_a_template_qk.db3", 3);
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(Std);
		
//		Std2Db3A.addChildJob(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(Std);
		return result;
	}
}
