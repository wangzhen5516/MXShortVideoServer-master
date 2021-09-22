package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import org.apache.http.*;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.*;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author ：zhongrenli
 * @date ：Created in 7:23 下午 2020/7/9
 */
public class HttpDataSource extends BaseDataSource {

    private final static Logger logger = LogManager.getLogger(HttpDataSource.class);
    private static final int CONNECT_TIMEOUT = 200;
    private static final int SOCKET_TIMEOUT = 1000;
    private final PoolingHttpClientConnectionManager manager;
    private CloseableHttpClient httpClient;
    private final RequestConfig requestConfig;

    /**
     * 相当于线程锁,用于线程安全
     */
    private final static Object SYNC_LOCK = new Object();

    private final static int MAX_CONN = 50;
    private final static int MAX_PRE_ROUTE = 500;

    private final ConnectionKeepAliveStrategy myStrategy = new ConnectionKeepAliveStrategy() {
        @Override
        public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
            HeaderElementIterator it = new BasicHeaderElementIterator
                    (response.headerIterator(HTTP.CONN_KEEP_ALIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && "timeout".equalsIgnoreCase
                        (param)) {
                    return Long.parseLong(value) * 1000;
                }
            }
            //如果没有约定，则默认定义时长为60s
            return 60 * 1000;
        }
    };

    private static class IdleConnectionEvictor extends Thread {
        private final HttpClientConnectionManager connMgr;
        private volatile boolean shutdown;

        private IdleConnectionEvictor(HttpClientConnectionManager connMgr) {
            super();
            this.connMgr = connMgr;
            this.start();
        }

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    synchronized (this) {
                        wait(5000);
                        // Close expired connections
                        connMgr.closeExpiredConnections();
                    }
                }
            } catch (InterruptedException ex) {
                // terminate
            }
        }

        public void shutdown() {
            shutdown = true;
            synchronized (this) {
                notifyAll();
            }
        }
    }

    public HttpDataSource() {
        ConnectionSocketFactory plainSocketFactory = PlainConnectionSocketFactory.getSocketFactory();
        LayeredConnectionSocketFactory sslSocketFactory = SSLConnectionSocketFactory.getSocketFactory();
        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create().register("http", plainSocketFactory)
                .register("https", sslSocketFactory).build();

        manager = new PoolingHttpClientConnectionManager(registry);
        //设置连接参数
        // 最大连接数
        manager.setMaxTotal(MAX_CONN);

        // 路由最大连接数
        manager.setDefaultMaxPerRoute(MAX_PRE_ROUTE);

        requestConfig = RequestConfig.custom().setConnectionRequestTimeout(CONNECT_TIMEOUT)
                .setConnectTimeout(CONNECT_TIMEOUT)
                .setSocketTimeout(SOCKET_TIMEOUT).build();

        httpClient = buildClient();

        //开启监控线程,对异常和空闲线程进行关闭
        new IdleConnectionEvictor(manager);
    }


    private CloseableHttpClient getHttpClient() {
        synchronized (SYNC_LOCK) {
            if (httpClient == null) {
                httpClient = buildClient();
            }
        }
        return httpClient;
    }

    private CloseableHttpClient buildClient() {
        return HttpClients.custom()
                .setConnectionManager(manager)
                .setKeepAliveStrategy(myStrategy)
                .setDefaultRequestConfig(requestConfig)
                .setRetryHandler(handler)
                .build();
    }

    private final HttpRequestRetryHandler handler = new HttpRequestRetryHandler() {
        @Override
        public boolean retryRequest(IOException e, int i, HttpContext httpContext) {
            if (i > 3) {
                //重试超过3次,放弃请求
                logger.error("retry has more than 3 time, give up request");
                NewRelic.noticeError("retry has more than 3 time, give up request");
                return false;
            }
            if (e instanceof NoHttpResponseException) {
                //服务器没有响应,可能是服务器断开了连接,应该重试
                logger.error("receive no response from server, retry");
                NewRelic.noticeError("receive no response from server, retry");
                return true;
            }
            if (e instanceof SSLHandshakeException) {
                // SSL握手异常
                logger.error("SSL hand shake exception");
                NewRelic.noticeError("SSL hand shake exception");
                return false;
            }
            if (e instanceof UnknownHostException) {
                // 服务器不可达
                logger.error("server host unknown");
                NewRelic.noticeError("server host unknown");
                return false;
            }
            if (e instanceof ConnectTimeoutException) {
                // 连接超时
                logger.error("Connection Time out");
                NewRelic.noticeError("Connection Time out");
                return false;
            }
            if (e instanceof SSLException) {
                logger.error("SSLException");
                NewRelic.noticeError("SSLException");
                return false;
            }
            if (e instanceof InterruptedIOException) {
                //超时
                logger.error("InterruptedIOException");
                NewRelic.noticeError("InterruptedIOException");
                return false;
            }

            HttpClientContext context = HttpClientContext.adapt(httpContext);
            HttpRequest request = context.getRequest();
            //如果请求不是关闭连接的请求
            return !(request instanceof HttpEntityEnclosingRequest);
        }
    };

    /**
     * get 方法
     *
     * @param url request url
     * @param paramMap   params
     */
    @Trace(dispatcher = true)
    public String get(String url, Map<String, String> paramMap) throws Exception {
        String result = "";
        HttpGet httpGet = new HttpGet(url);
        try {
            CloseableHttpClient httpClient = getHttpClient();
            List<NameValuePair> params = setHttpParams(paramMap);
            String param = URLEncodedUtils.format(params, "UTF-8");

            httpGet.setURI(URI.create(url + "?" + param));
            HttpResponse response = httpClient.execute(httpGet);
            result = getHttpEntityContent(response);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                result = null;
            }
        } catch (Exception e) {
            System.out.println("请求异常");
            NewRelic.noticeError("请求异常");
            throw new Exception(e);
        } finally {
            httpGet.abort();
        }
        return result;
    }

    /**
     * 设置 get 请求的参数
     *
     * @param paramMap   params
     */
    public List<NameValuePair> setHttpParams(Map<String, String> paramMap) {
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        Set<Map.Entry<String, String>> set = paramMap.entrySet();
        for (Map.Entry<String, String> entry : set) {
            params.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
        return params;
    }

    public String getHttpEntityContent(HttpResponse response) throws UnsupportedOperationException, IOException {
        String result = "";
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            InputStream in = entity.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            StringBuilder strBuilder = new StringBuilder();
            String line = null;
            while ((line = br.readLine()) != null) {
                strBuilder.append(line).append('\n');
            }
            br.close();
            in.close();
            result = strBuilder.toString();
        }
        return result;
    }

    /**
     * post 方法
     *
     * @param url request url
     * @param json Request body
     */
    @Trace(dispatcher = true)
    public String post(String url, JSONObject json) throws Exception {
        String result = "";
        HttpPost httpPost = new HttpPost(url);
        try {
            CloseableHttpClient httpClient = getHttpClient();
            // 规定请求 body 类型为JSON
            httpPost.addHeader("Content-Type","application/json;charset=utf-8");
            httpPost.setEntity(new StringEntity(json.toString(), "utf-8"));
            HttpResponse response = httpClient.execute(httpPost);
            result = getHttpEntityContent(response);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                result = null;
            }
        } catch (Exception e) {
            System.out.println("请求异常");
            NewRelic.noticeError("请求异常");
            throw new Exception(e);
        } finally {
            httpPost.abort();
        }
        return result;
    }
}
