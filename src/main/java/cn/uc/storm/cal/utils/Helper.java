package cn.uc.storm.cal.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

public class Helper {
	static public Logger LOG = Logger.getLogger(Helper.class);

	static public void ifNullAndSet(Map conf, String key, Object value) {
		if (conf.get(key) == null) {
			conf.put(key, value);
		}
	}

	static public Object getOrDefault(Map conf, String key, Object defaultValue) {
		Object temp = conf.get(key);
		return temp == null ? defaultValue : temp;
	}

	static public String ListToString(List<String> list, String split) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String o : list) {
			if (first) {
				first = false;
			} else {
				sb.append(split);
			}
			sb.append(o.toString());
		}
		return sb.toString();
	}

	@SuppressWarnings("rawtypes")
	static public Object newInstanceFromConf(Map conf, String key)
			throws Exception {
		try {
			String className = (String) conf.get(key);
			Class tempClass = Class.forName(className);
			Object temp = tempClass.newInstance();
			return temp;
		} catch (Exception e) {
			LOG.error("create instance catch Exception: " + key, e);
			throw e;
		}
	}

	@SuppressWarnings("rawtypes")
	static public Class getClassFromConf(Map conf, String key)
			throws ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		String className = (String) conf.get(key);
		Class tempClass = Class.forName(className);
		return tempClass;
	}

	public static String getMessage(ByteBuffer buffer) {
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes);
	}

	public static Map readConfigYaml(String file) {
		Map ret = null;
		try {
			Yaml yaml = new Yaml();
			ret = (Map) yaml.load(new InputStreamReader(new FileInputStream(
					new File(file))));

		} catch (Exception e) {
			LOG.error("read file catch Exception :" + file, e);
		}
		if (ret == null)
			ret = new HashMap();
		return new HashMap(ret);
	}

}
