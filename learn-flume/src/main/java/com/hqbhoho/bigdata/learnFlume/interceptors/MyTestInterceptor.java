package com.hqbhoho.bigdata.learnFlume.interceptors;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * describe:
 * 开发一个奇偶过滤的拦截器
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/05
 */
public class MyTestInterceptor implements Interceptor{

    //if ture , filter even
    private boolean isFilterEven;

    public MyTestInterceptor(boolean isFilterEven){
        this.isFilterEven=isFilterEven;
    }

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        if(isFilterEven){
            return list.stream()
                    .filter(e -> (Integer.valueOf(new String(e.getBody()))%2!=0))
                    .collect(Collectors.toList());
        }else{
            return list.stream()
                    .filter(e -> (Integer.valueOf(new String(e.getBody()))%2==0))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder {

        private final String IS_FLITER_EVEN="isFliterEven";
        // 默认过滤偶数
        private boolean isFilterEven = true;
        @Override
        public Interceptor build() {
            return new MyTestInterceptor(isFilterEven);
        }

        @Override
        public void configure(Context context) {
            String flag = context.getString(IS_FLITER_EVEN);
            if("false".equalsIgnoreCase(flag)) isFilterEven=false;
        }
    }

}
