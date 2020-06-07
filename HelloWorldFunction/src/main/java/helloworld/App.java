package helloworld;

import java.io.*;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Handler for requests to Lambda function.
 */
public class App implements RequestHandler<Object, Object> {

    public Object handleRequest(final Object input, final Context context) {

        //File configFile = new java.io.File("classpath:/kafka.properties");
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try {
        
            
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

            producer.send(new ProducerRecord<String, String>("LOCAL_AUTO_B2B_LD_INGEST",getIngestPayload()));

            producer.close();
            return "sucess";
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
       /* Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "application/json");
        try {
            final String pageContents = this.getPageContents("https://checkip.amazonaws.com");
            String output = String.format("{ \"message\": \"hello world\", \"location\": \"%s\" }", pageContents);
            return new GatewayResponse(output, headers, 200);
        } catch (IOException e) {
            return new GatewayResponse("{}", headers, 500);
        }*/
    }

    private String getPageContents(String address) throws IOException{
        URL url = new URL(address);
        try(BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()))) {
            return br.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }

    private String getIngestPayload() {
        return "{\"planVersionId\":1765363,\"providerVersionId\":266264,\"piiReferenceId\":\"05bb9544-72c6-4405-ac35-8bfc0f740030\",\"eventSource\":\"IAU\",\"LeadData\":{\"leadId\":\"q55005aqqa5\"}}";
    }
}
