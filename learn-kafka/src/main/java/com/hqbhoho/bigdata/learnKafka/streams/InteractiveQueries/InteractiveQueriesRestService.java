package com.hqbhoho.bigdata.learnKafka.streams.InteractiveQueries;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;


/**
 * describe:
 * <p>
 * A simple REST proxy that runs embedded in the {@link RemoteStateQueryExample}. This is used to
 * demonstrate how a developer can use the Interactive Queries APIs exposed by Kafka Streams to
 * locate and query the State Stores within a Kafka Streams Application.
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/01
 */
@Path("state")
public class InteractiveQueriesRestService {
    private static final Logger LOG = LoggerFactory.getLogger(InteractiveQueriesRestService.class);
    private final KafkaStreams streams;
    private Server jettyServer;
    private final HostInfo hostInfo;
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private static final Logger log = LoggerFactory.getLogger(InteractiveQueriesRestService.class);

    InteractiveQueriesRestService(final KafkaStreams streams,
                                  final HostInfo hostInfo) {
        this.streams = streams;
        this.hostInfo = hostInfo;
    }

    /**
     * Get all of the key-value pairs available  (local and remote)
     *
     * @param storeName store to query
     * @return A List of all of the key-values in the provided
     * store
     */
    @GET()
    @Path("/keyvalues/{storeName}/all")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> allForStore(@PathParam("storeName") final String storeName) {
        Map<String, String> results = new HashMap<>();
        Collection<StreamsMetadata> wordCountHosts = streams.allMetadataForStore(storeName);
        wordCountHosts.stream().forEach((metadata) -> {
            results.put(metadata.hostInfo().toString(), remoteForStore(storeName, metadata.hostInfo()));
        });
        return results;
    }

    /**
     * Get all of the key-value pairs available on local
     *
     * @param storeName
     * @return A List of all of the key-values on local
     */
    @GET()
    @Path("/keyvalues/{storeName}/local/all")
    @Produces(MediaType.APPLICATION_JSON)
    public String localForStore(@PathParam("storeName") final String storeName) {

        /*// 使用future实现
        FutureTask<List<String>> results = new FutureTask<>(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                List<String> list = new ArrayList<>();
                ReadOnlyKeyValueStore<String, Long> keyValueStore =
                        streams.store(storeName, QueryableStoreTypes.keyValueStore());
                // Get the values for all of the keys available in this application instance
                KeyValueIterator<String, Long> range = keyValueStore.all();
                while (range.hasNext()) {
                    KeyValue<String, Long> next = range.next();
                    list.add(next.key + ": " + next.value);
                }
                range.close();
                return list;
            }
        });
        String result = null;
        try {
            result = results.get().toString();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return result;*/

        // 使用java8中CompletableFuture实现
        CountDownLatch count = new CountDownLatch(1);
        List<String> results = new ArrayList<>();
        CompletableFuture.runAsync(() -> {
            ReadOnlyKeyValueStore<String, Long> keyValueStore =
                    streams.store(storeName, QueryableStoreTypes.keyValueStore());
            // Get the values for all of the keys available in this application instance
            KeyValueIterator<String, Long> range = keyValueStore.all();
            while (range.hasNext()) {
                KeyValue<String, Long> next = range.next();
                results.add(next.key + ": " + next.value);
            }
            range.close();
        }).whenComplete((s, e) -> {
            count.countDown();
            if (e != null) {
                throw new RuntimeException(e);
            }
        });
        try {
            count.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return results.toString();

    }

    /**
     * Get all of the key-value pairs available on special hostinfo local
     *
     * @param storeName
     * @param hostInfo
     * @return A List of all of the key-values on special hostinfo local
     */
    public String remoteForStore(final String storeName, HostInfo hostInfo) {
        OkHttpClient httpClient = new OkHttpClient();
        Request request = new Request.Builder()
                .url("http://" + hostInfo.host() + ":" + hostInfo.port() + "/state/keyvalues/" + storeName + "/local/all")
                .build();
        try {
            Response response = httpClient.newCall(request).execute();
            return response.body().string();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Start an embedded Jetty Server on the given port
     *
     * @param port port to run the Server on
     * @throws Exception if jetty can't start
     */
    void start(final int port) throws Exception {
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server();
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        final ServletContainer sc = new ServletContainer(rc);
        final ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        final ServerConnector connector = new ServerConnector(jettyServer);
        connector.setHost(hostInfo.host());
        connector.setPort(port);
        jettyServer.addConnector(connector);

        context.start();

        try {
            jettyServer.start();
        } catch (final java.net.SocketException exception) {
            log.error("Unavailable: " + hostInfo.host() + ":" + hostInfo.port());
            throw new Exception(exception.toString());
        }
    }

    /**
     * Stop the Jetty Server
     *
     * @throws Exception if jetty can't stop
     */
    void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }
}
