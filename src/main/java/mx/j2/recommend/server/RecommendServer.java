package mx.j2.recommend.server;

import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.thrift.RecommendService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 推荐引擎入口
 *
 * @author zhuowei
 *
 */
public class RecommendServer {

	static RecommendHandler handler;

	public static String HostIp = getIP();

	static RecommendService.Processor<RecommendService.Iface> processor;
	static{
		try {
			System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
			System.setProperty("AsyncLogger.RingBufferSize","262144");
			System.setProperty("AsyncLoggerConfig.RingBufferSize","262144");
			File file = new File("conf/log4j2.xml");
			BufferedInputStream in = null;
			in = new BufferedInputStream(new FileInputStream(file));
			ConfigurationSource source;
			source = new ConfigurationSource(in);
			Configurator.initialize(null, source);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static Logger logger = LogManager.getLogger(RecommendServer.class);

	public static void main(String[] args) {
		String confFile=args[0];
		Conf.loadConf(confFile);
		try {
			handler = new RecommendHandler();
			processor = new RecommendService.Processor<RecommendService.Iface>(handler);
            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(new TNonblockingServerSocket.NonblockingAbstractServerSocketArgs()
					.port(Conf.getPort())
					.clientTimeout(Conf.getServerTransportClientTimeout())
					.backlog(Conf.getServerTransportBackLog()));
            TThreadedSelectorServer.Args ttpsArgs = new TThreadedSelectorServer.Args(serverTransport);
			ttpsArgs.processor(processor);
			ttpsArgs.workerThreads(Conf.getWorkThreadNum());
			ttpsArgs.selectorThreads(Conf.getSelectorThreadNum());
			ttpsArgs.acceptQueueSizePerThread(Conf.getAcceptQueueSizePerThread());

            ttpsArgs.protocolFactory(new TCompactProtocol.Factory());
			TServer server = new TThreadedSelectorServer(ttpsArgs);
			logger.info(String.format("{\"serverInfo\":\"server[%s] start sucessfully at port:%s\"}", Conf.getEnv(), Conf.getPort()));
			System.out.println(String.format("server[%s] start sucessfully at port:%s", Conf.getEnv(), Conf.getPort()));
			server.serve();
		} catch (Exception x) {
			x.printStackTrace();
		}
	}

	private static String getIP() {
		String ip = null;
		try {
			InetAddress addr = InetAddress.getLocalHost();
			ip = addr.getHostAddress();
		} catch (UnknownHostException e) {
			System.out.println("can't get HostIp! " + e.getMessage());
		}
		return ip;
	}
}
