package com.mesosphere.dcos.cassandra;

import com.google.common.base.Charsets;
import org.apache.cassandra.locator.SeedProvider;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by kowens on 2/9/16.
 */
public class DcosSeedProvider implements SeedProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger
            (DcosSeedProvider.class);


    private final String seedsUrl;

    public DcosSeedProvider(final Map<String, String> properties) {

        seedsUrl = properties.get("seeds_url");

    }

    private static InetAddress getLocalAddress() throws UnknownHostException {
        String libProcessAddress = System.getenv("LIBPROCESS_IP");

        if (libProcessAddress == null || libProcessAddress.isEmpty()) {
            LOGGER.info("LIBPROCESS_IP address not found defaulting to " +
                    "localhost");
            return InetAddress.getLocalHost();

        } else {

            return InetAddress.getByName(libProcessAddress);
        }
    }


    public List<InetAddress> getRemoteSeeds() throws IOException {

        HttpURLConnection connection =
                (HttpURLConnection) new URL(seedsUrl).openConnection();
        connection.setConnectTimeout(1000);
        connection.setReadTimeout(10000);
        connection.setRequestMethod("GET");
        if (connection.getResponseCode() != 200)
            throw new RuntimeException("Unable to get data for URL " + seedsUrl);

        byte[] b = new byte[2048];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataInputStream responseStream = new DataInputStream((FilterInputStream)
                connection
                        .getContent());
        int c = 0;
        while ((c = responseStream.read(b, 0, b.length)) != -1)
            bos.write(b, 0, c);
        String response = new String(bos.toByteArray(), Charsets.UTF_8);
        LOGGER.info("Retrieved response {} from URL {}", response, seedsUrl);
        connection.disconnect();


        JSONObject json = (JSONObject) JSONValue.parse(response);

        boolean isSeed = (Boolean) json.get("isSeed");

        List<String> seedStrings = (json.containsKey("seeds"))
                ? (List<String>) json.get("seeds") : Collections.emptyList();

        List<InetAddress> addresses;

        if (isSeed) {
            addresses = new ArrayList<>(seedStrings.size() + 1);
            addresses.add(getLocalAddress());
        } else {

            addresses = new ArrayList<>(seedStrings.size());
        }

        for (String seed : seedStrings) {

            addresses.add(InetAddress.getByName(seed));
        }

        LOGGER.info("Retrieved remote seeds {}", addresses);

        return addresses;
    }


    @Override
    public List<InetAddress> getSeeds() {

        try {
            return getRemoteSeeds();
        } catch (Throwable ex) {
            LOGGER.error(
                    String.format("Failed to retrieve seeds from %s", seedsUrl)
                    , ex);

            return Collections.emptyList();
        }

    }
}
