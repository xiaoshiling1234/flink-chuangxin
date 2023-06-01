package com.chuangxin.app.function;

import com.chuangxin.util.HttpClientUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.List;
import java.util.Map;

public abstract class HttpSourceFunction extends RichSourceFunction<String> {
    private String json=null;
    private Map<String, String> params=null;
    protected String url;

    public HttpSourceFunction(String url) {
        this.url = url;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        List<String> requestJsonList = getRequestJsonList();
        requestJsonList.forEach(json->{
            //todo:成功失败的记录都应该存在数据库里，方便查询任务状态，以及重跑
            try {
                sourceContext.collect(HttpClientUtils.doPostJson(url,json));
            }catch (Exception e){
                e.printStackTrace();
            }
        });
    }

    @Override
    public void cancel() {

    }

    /**
     * 推断所有需要执行的请求，这个接口不同只能自己实现
     * @return 请求的json请求体
     */
    public abstract List<String> getRequestJsonList();
}
