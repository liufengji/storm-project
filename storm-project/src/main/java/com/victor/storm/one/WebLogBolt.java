package com.victor.storm.one;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class WebLogBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector = null;
	private int num = 0;
	private String valueString = null;

	@Override
	public void execute(Tuple input) {
		
		try {
			// 1 获取传递过来的数据
			valueString = input.getStringByField("log");
			//input.getString(0);

			// 2 如果输入的数据不为空，行数++
			String[] split =valueString.split("\t");
			String session = split[1];

			//统计发送数
			num++;
			/*if (valueString != null) { 
				num++;
				System.err.println(Thread.currentThread().getName() + "lines  :" + num + "   session_id:"
						+ valueString.split("\t")[1]);
			}*/

			// 3 应答Spout接收成功
			collector.ack(input);
			Thread.sleep(2000);
			
			//打印
			System.err.println(Thread.currentThread().getName() +  "     session_id:"+ session+"     lines  :" + num );

		} catch (Exception e) {
			// 4 应答Spout接收失败
			collector.fail(input);
			e.printStackTrace();
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明输出字段类型
		declarer.declare(new Fields(""));
	}

	@Override
	public void cleanup() {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
