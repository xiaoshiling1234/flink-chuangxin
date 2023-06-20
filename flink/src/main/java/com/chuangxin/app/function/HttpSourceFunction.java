package com.chuangxin.app.function;

import com.chuangxin.util.HttpClientUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class HttpSourceFunction extends RichSourceFunction<String> {
    protected String url;

    public HttpSourceFunction(String url) {
        this.url = url;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        List<Map<String, String>> parametersList = getRequestParametersList();
        parametersList.forEach(parameters -> {
            try {
                System.out.println("当前正在同步第" + parameters.get("page").toString() + "页数据");
                sourceContext.collect(HttpClientUtils.doGet(url, parameters).body().string());
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
