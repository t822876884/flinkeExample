package com.example.flinkExample.action;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @author bertram
 * @date 2021/5/11 9:30
 * @desc
 *  1.实时计算出当天零点截止到当前时间的销售总额 11月11日 00:00:00 ~ 23:59:59
 *  2.计算出各个分类的销售top3
 *  3.每秒钟更新一次统计结果
 */
public class DoubleElevenScreen {

    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStream<Tuple2<String, Double>> orderDs = env.addSource(new MySource());

        //TODO 2.transformation
        DataStream<CategoryPojo> tmpAggResult = orderDs
                .keyBy(t -> t.f0) //按照类别分组
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))//自定义触发时机，1s触发一次
                .aggregate(new PriceAggregate(), new WindowResult());//aggregate支持复杂的自定义聚合

        //tmpAggResult.print("初步聚合的各个分类的销售总额");

        tmpAggResult.keyBy(CategoryPojo::getDateTime)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new FinalResultResultProcess());


        //TODO 3.sink


        //TODO 4.execute
        env.execute();
    }

    /**
     * 自定义数据源
     * 模拟双11销售数据
     */
    public static class MySource implements SourceFunction<Tuple2<String, Double>> {
        private boolean flag = true;
        private String[] categorys = {"女装","男装","图书","家电","洗护","美妆","运动","游戏","户外","家具","乐器","办公"};
        Random random = new Random();
        @Override
        public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
            while (flag) {
                int index = random.nextInt(categorys.length);
                //获取随机分类
                String category = categorys[index];
                //random.nextDouble() 随机生成[0，1)的小数，*100生成[0，100)的随机小数
                double privce = random.nextDouble() * 100;
                ctx.collect(Tuple2.of(category, privce));
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    //public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable
    private static class PriceAggregate implements AggregateFunction<Tuple2<String, Double>, Double, Double> {

        //初始化累加器
        @Override
        public Double createAccumulator() {
            return 0d;
        }

        //把数据累加到累加器上
        @Override
        public Double add(Tuple2<String, Double> value, Double accumulator) {
            return value.f1 + accumulator;
        }

        //获取累加结果
        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        //合并各个subTask的结果
        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }

    //public interface WindowFunction<IN, OUT, KEY, W extends Window> extends Function, Serializable
    private static class WindowResult implements WindowFunction<Double, CategoryPojo, String, TimeWindow> {
        private FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        @Override
        public void apply(String category, TimeWindow window, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
            long currentTimeMillis = System.currentTimeMillis();
            String dateTime = df.format(currentTimeMillis);
            double totalPrice = input.iterator().next();
            out.collect(new CategoryPojo(category, totalPrice, dateTime));
        }
    }

    /**
     * 用于存储聚合的结果
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CategoryPojo {
        private String category;//分类名称
        private double totalPrice;//该分类总销售额
        private String dateTime;// 截止到当前时间的时间,本来应该是EventTime,但是我们这里简化了直接用当前系统时间即可
    }


    private static class FinalResultResultProcess extends ProcessWindowFunction<CategoryPojo, Object, String, TimeWindow> {
        @Override
        public void process(String dateTime, Context context, Iterable<CategoryPojo> elements, Collector<Object> out) throws Exception {
            double total = 0d;//用来记录销售总额

            //创建一个小顶堆，用来记录top3
            Queue<CategoryPojo> queue = new PriorityQueue<>(3,  //初始容量
                    (c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? 1 : -1); //比较器
            for (CategoryPojo element: elements) {
                double price = element.getTotalPrice();
                total += price; //累加总金额

                //top3
                //刚开始的时候，队列内容小于3
                if (queue.size() < 3){
                    queue.add(element); //直接入队列
                } else {
                    if (price >= queue.peek().getTotalPrice()){ //peek()取出堆顶元素，但不删除，与当前price做比较
                        queue.poll(); //移除堆顶元素
                        queue.add(element);
                    }
                }
            }

            //此时，queue里面存放的是分类的top3，但是是升序结果，需要改为降序
            List<String> top3List = queue.stream()
                    .sorted((c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? -1 : 1)
                    .map(c -> "分类:" + c.getCategory() + " 金额:" + c.getTotalPrice())
                    .collect(Collectors.toList());

            double roundResult = new BigDecimal(total).setScale(2, RoundingMode.HALF_UP).doubleValue();//四舍五入保留2位小数

            System.out.println("时间: "+dateTime +" 总金额 :" + roundResult);
            System.out.println("top3: \n" + StringUtils.join(top3List,"\n"));
        }
    }
}
