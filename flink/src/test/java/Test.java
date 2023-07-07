import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class Test {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建输入数据源
        DataStream<String> input = env.addSource(new DataGeneratorSource());

        // 数据处理逻辑
        DataStream<Tuple2<String, Integer>> output = input
                .flatMap(new WordCountFlatMapFunction());

        // 输出结果
        output.print();

        // 执行任务
        env.execute("Stream Task Example");
    }

    // 数据生成器
    public static class DataGeneratorSource implements SourceFunction<String> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceFunction.SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                // 生成随机数据
                String data = generateData();

                // 发送数据到任务
                ctx.collect(data);

                // 控制生成速率，这里使用1秒发送一次
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        // 随机数据生成逻辑
        private String generateData() {
            // 你可以在这里定义你的数据生成逻辑
            return "Hello Flink!";
        }
    }

    // 数据处理逻辑
    public static class WordCountFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 分割单词
            String[] words = value.split("\\s+");

            // 计数器
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
