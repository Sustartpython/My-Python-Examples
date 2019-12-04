package simple.jobstream.mapreduce.site.hkmolib;

import java.util.LinkedHashSet;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
	
		
		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("hkmolib.Json2XXXXObject",
		   DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.hkmolib.Json2XXXXObject",
		   "/RawData/hkmolib/big_json/20190703", "/RawData/hkmolib/xxxobj", 40);
			 
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("hkmolib","simple.jobstream.mapreduce.site.hkmolib.hkmolib_db", 
				"/RawData/hkmolib/xxxobj", "/RawData/hkmolib/db_zt", "hkmolib","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode StdDb3a2 = JobNodeModel.getJobNode4Std2Db3("hkmolib","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				"/RawData/hkmolib/xxxobj", "/RawData/hkmolib/db_a", "hkmolib","/RawData/_rel_file/base_obj_meta_a_template_dt.db3",1);
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(StdDb3);
		StdDb3.addChildJob(StdDb3a2);
		
		
		return result;
	}
}
