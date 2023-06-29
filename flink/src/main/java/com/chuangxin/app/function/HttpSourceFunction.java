package com.chuangxin.app.function;

import com.chuangxin.util.HttpClientUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class HttpSourceFunction extends RichSourceFunction<Tuple2<Map<String, String>,String>> {
    protected String url;

    public HttpSourceFunction(String url) {
        this.url = url;
    }

    @Override
    public void run(SourceContext<Tuple2<Map<String, String>,String>> sourceContext) throws Exception {
        List<Map<String, String>> parametersList = getRequestParametersList();
        parametersList.forEach(parameters -> {
            try {
                sourceContext.collect(new Tuple2<>(parameters,HttpClientUtils.doGet(url, parameters).body().string()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void cancel() {

    }

    /**
     * 推断所有需要执行的请求，这个接口不同只能自己实现
     *
     * @return 请求的json请求体
     */
    public abstract List<Map<String, String>> getRequestParametersList() throws IOException, IllegalAccessException;
}
