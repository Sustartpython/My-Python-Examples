package simple.jobstream.mapreduce.site.wfbs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ClassLoad 
{
	private Context context         = null;
	private static HashMap<String, String> FirstClassMap      = new HashMap<>();
	private static HashMap<String, String> SecondOnlyClassMap = new HashMap<>();
	private static HashMap<String, String> SecondClassMap     = new HashMap<>();
	
	public ClassLoad(Context context, String firstclass_info, String secondclass_info)
	{
		this.context = context;
		
		loadfirstclass(firstclass_info);
		loadsecondclass(secondclass_info);
	}
	
	public static String UniqLine(String line)
	{
		String[] vec = line.split(";");
        ArrayList<String> lst = new ArrayList<String>();
        for (int i = 0; i < vec.length; ++i )
        {
        	String val = vec[i].trim();
            if (val.length() < 1)
            {
                continue;
            }
            if (!lst.contains(val))
            {
                lst.add(val);
            }
        }
        String newLine = "";
        for(String val:lst)
        {
            newLine += val + ";";
        }

        return newLine;
	}
	
	public void loadfirstclass(String inputpath)
    {
		try 
		{
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fin = fs.open(new Path(inputpath)); 
			BufferedReader in = null;
			String line;
			try 
			{
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) 
				{
					String[] arrTempLine = line.toString().split("\t");
					
					String classid       = arrTempLine[0].trim();
					String classtypename = arrTempLine[1].trim();
					String classcode     = arrTempLine[2].trim().toUpperCase();
					//String classidlevel  = arrTempLine[3].trim();
					//String classlevelname= arrTempLine[4].trim();
					
					String[] arr_class = classcode.split(";");
					for (int i = 0; i < arr_class.length; i++) 
					{
						String tempclass = arr_class[i].trim();
						
						if (FirstClassMap.containsKey(tempclass)) 
						{
							String templine = FirstClassMap.get(tempclass);
							
							templine += "[" + classid + "]" + classtypename + ";";
							FirstClassMap.put(tempclass, templine);
							
						}else {
							
							String templine = "[" + classid + "]" + classtypename + ";";
							FirstClassMap.put(tempclass, templine);
						}
					}
					
				}
			}finally {
				if (in!= null) 
				{
					in.close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Iterator iter1 = FirstClassMap.entrySet().iterator();
		while (iter1.hasNext()) 
		{
			Map.Entry entry = (Map.Entry) iter1.next();
			String key = (String) entry.getKey();
			String val = (String) entry.getValue();
			
			FirstClassMap.put(key, UniqLine(val));
		}
    }
	
	public void loadsecondclass(String inputpath)
    {
		try 
		{
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fin = fs.open(new Path(inputpath)); 
			BufferedReader in = null;
			String line;
			try 
			{
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) 
				{
					String[] arrTempLine = line.toString().split("\t");
					
					String classid       = arrTempLine[0].trim();
					String classtypename = arrTempLine[1].trim();
					String classcode     = arrTempLine[2].trim().toUpperCase();
					String classidlevel  = arrTempLine[3].trim();
					String classlevelname= arrTempLine[4].trim();
					
					String[] arr_class = classcode.split(";");
					for (int i = 0; i < arr_class.length; i++) 
					{
						String tempclass = arr_class[i].trim();
						
						if (SecondOnlyClassMap.containsKey(tempclass)) 
						{
							String templine = SecondOnlyClassMap.get(tempclass);
							
							templine += "[" + classid + "]" + classtypename + ";";
							SecondOnlyClassMap.put(tempclass, templine);
							
						}else {
							
							String templine = "[" + classid + "]" + classtypename + ";";
							SecondOnlyClassMap.put(tempclass, templine);
						}
						
						if (SecondClassMap.containsKey(tempclass)) 
						{
							String templine = SecondClassMap.get(tempclass);
							
							templine += "[" + classidlevel.split(";")[0].trim() + "]" + 
									classlevelname.split(";")[0].trim() + ";" + 
									"[" + classidlevel.split(";")[1].trim() + "]" + 
									classlevelname.split(";")[1].trim() + ";";
							
							SecondClassMap.put(tempclass, templine);
							
						}else {
							
							String templine = "[" + classidlevel.split(";")[0].trim() + "]" + 
									classlevelname.split(";")[0].trim() + ";" + 
									"[" + classidlevel.split(";")[1].trim() + "]" + 
									classlevelname.split(";")[1].trim() + ";";
							SecondClassMap.put(tempclass, templine);
						}
					}
				}
			}finally {
				if (in!= null) 
				{
					in.close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Iterator iter2 = SecondClassMap.entrySet().iterator();
		while (iter2.hasNext()) 
		{
			Map.Entry entry = (Map.Entry) iter2.next();
			String key = (String) entry.getKey();
			String val = (String) entry.getValue();
			
			SecondClassMap.put(key, UniqLine(val));
		}
		
		Iterator iter3 = SecondOnlyClassMap.entrySet().iterator();
		while (iter3.hasNext()) 
		{
			Map.Entry entry = (Map.Entry) iter3.next();
			String key = (String) entry.getKey();
			String val = (String) entry.getValue();
			
			SecondOnlyClassMap.put(key, UniqLine(val));
		}
    }
	
	public HashMap<String, String> getfirstclass()
    {
		return FirstClassMap;
    }
	
	public HashMap<String, String> getsecondclass()
    {
		return SecondClassMap;
    }
	
	public HashMap<String, String> getsecondonlyclass()
    {
		return SecondOnlyClassMap;
    }
	
}
