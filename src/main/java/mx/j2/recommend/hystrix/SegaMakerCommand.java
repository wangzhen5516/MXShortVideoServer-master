package mx.j2.recommend.hystrix;

import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeAsync;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointRequest;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointResult;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixEventType;
import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.HystrixUtil;
import mx.j2.recommend.util.OptionalUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author zhongrenli
 */
public class SegaMakerCommand extends HystrixCommand<String> {

    private final static int FOLLOWERS_SIZE = 20;

    private BaseDataCollection baseDc;
    private AmazonSageMakerRuntimeAsync client;
    private InvokeEndpointRequest invokeEndpointRequest;

    private SegaMakerCommand() {
        super(HystrixUtil.segaMakerSetter);
    }

    public SegaMakerCommand(BaseDataCollection dc, AmazonSageMakerRuntimeAsync client, InvokeEndpointRequest invokeEndpointRequest) {
        this();
        this.baseDc = dc;
        this.client = client;
        this.invokeEndpointRequest = invokeEndpointRequest;
    }

    @Override
    protected String run() throws Exception {
        Future<InvokeEndpointResult> future = client.invokeEndpointAsync(invokeEndpointRequest);
        InvokeEndpointResult result = future.get(300, TimeUnit.MILLISECONDS);;
        final String[] body = new String[1];
        OptionalUtil.ofNullable(result)
                .getUtil(InvokeEndpointResult::getBody)
                .ifPresent(r -> body[0] = StandardCharsets.UTF_8.decode(r).toString());
        return body[0];
    }

    @Override
    protected String getFallback() {
        Throwable e = this.getExecutionException();
        List<HystrixEventType> es = this.getExecutionEvents();

        String msg = String.format("Command key: %s, ExecutionEvents: %s, ExecutionMessage: %s, Group: %s",
                this.getCommandKey().name(), es, e, this.getCommandGroup());

        if (e != null) {
            e.printStackTrace();
        } else {
            NewRelic.noticeError(msg);
        }
        return null;
    }
}
