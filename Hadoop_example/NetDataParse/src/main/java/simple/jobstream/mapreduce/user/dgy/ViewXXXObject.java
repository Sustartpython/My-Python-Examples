package simple.jobstream.mapreduce.user.dgy;

import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

import com.process.frame.JobStreamRun;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class ViewXXXObject {
	private static void _view(String ...strings) {
		try {
			Configuration conf = new Configuration();
			for (Entry<Object, Object> entry : JobStreamRun.getCustomProperties().entrySet())
			{
				conf.set(entry.getKey().toString(), entry.getValue().toString());
			}
			URI uri = URI.create(conf.get("fs.default.name"));
			FileSystem hdfs;
			hdfs = FileSystem.get(uri, conf);
			Path path = new Path(strings[0]); 	//文件或目录路径
			
			HashSet<String> fieldSet = new HashSet<String>();
			if (strings.length > 1) {
				for (String field : strings[1].split(";")) {
					fieldSet.add(field.trim());
				}
			}
			
			FileStatus[] files = hdfs.listStatus(path);
			long cntRecord = 0;
			for (FileStatus file : files) {
				System.out.println(file.getPath().getName());
				
				if (file.isDirectory()) {		//忽略叶子目录
					continue;
				}
				
				if (!file.getPath().getName().startsWith("part-")) {	//忽略干扰文件
					continue;
				}
				
//				if (Integer.parseInt(file.getPath().getName().substring(file.getPath().getName().length()-4)) < 160) {
//					continue;
//				}
				
//				if (file.getLen() > 1024*1024*1024) {	//忽略大于1G的文件
//					continue;
//				}
				
				System.out.println("file.getLen:" + file.getLen());

				XXXXObject xObj = new XXXXObject();
		        SequenceFile.Reader in = new SequenceFile.Reader(conf, Reader.file(file.getPath()));
		        
		        Object key = null;
		        Object value = null;

		        while (true) {
		        	key = in.next(key);
		        	if (key == null) {
		        		break;	
		        	}	
		        	
		        	System.out.println("key:" + key);
		        	
		        	value = in.getCurrentValue((Object) value);
		        	VipcloudUtil.DeserializeObject(((BytesWritable)value).getBytes(), xObj);
		        	//System.out.println("keyword_c:" + xObj.data.getOrDefault("keyword_c", "").trim());
		        	System.out.println("**********************data*******************************");
		        	System.out.println("*** _key:" + key.toString());
	        		for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
	        			String field = updateItem.getKey().trim();
	        			String fval = updateItem.getValue().trim();
	        			
	        			// fieldSet 为空或包含本字段时输出
	        			if (fieldSet.isEmpty() || fieldSet.contains(field)) {
	        				System.out.println(field + ":" + fval);
						}
		        	}
	        		cntRecord += 1;
	        		System.out.println("*** cntRecord: " + cntRecord);
	        		System.out.println("**********************data*******************************");
	        		System.out.println("\n\n");
		        	
				}
		        in.close();   
		        
		        System.out.println("*** total: " + cntRecord);
			}
			hdfs.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}

	/**
	 * <p>Description: 打印全字段 </p>  
	 * @author qiuhongyang 2018年11月8日 下午3:00:19
	 * @param sPath  XXXXObject 目录路径
	 */
	public static void view(String sPath) {
		_view(sPath);
	}
	
	/**
	 * <p>Description:根据sFilter打印部分字段 </p>  
	 * @author qiuhongyang 2018年11月8日 下午2:59:30
	 * @param sPath XXXXObject 目录路径
	 * @param sFilter 以分号分割的字段名
	 */
	public static void view(String sPath, String sFilter) {
		_view(sPath, sFilter);
	}
	
	public static void main(String args[]) throws Exception{
		ViewXXXObject.view("/RawData/ebsco/MEDLINE_cmedm/XXXXObject");
	}
}
