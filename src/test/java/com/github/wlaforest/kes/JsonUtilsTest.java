package com.github.wlaforest.kes;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonUtilsTest {

    @Test
    public void testEmbedRawData()
    {
        String jsonString = "{\"type\":\"foo\"}";
        String returnedString = JsonUtils.embedRawData(jsonString);
        String expectedEndingString = "_raw_data\":\"{\\\"type\\\":\\\"foo\\\"}\"}";
        assertTrue(returnedString.endsWith(expectedEndingString),
                "returned string is bad: " + returnedString + ".  It should end with " +
                expectedEndingString);
    }

    @Test
    public void testJsonPath()
    {
        String testJsonString = "{\n" +
                "  \"sensorType\": \"SBS-3\",\n" +
                "  \"sensorLatitude\": {\n" +
                "    \"double\": 49.43663\n" +
                "  },\n" +
                "  \"sensorLongitude\": {\n" +
                "    \"double\": 7.76968\n" +
                "  },\n" +
                "  \"sensorAltitude\": {\n" +
                "    \"double\": 886\n" +
                "  },\n" +
                "  \"timeAtServer\": 1429617900.999499,\n" +
                "  \"timeAtSensor\": null,\n" +
                "  \"timestamp\": {\n" +
                "    \"double\": 7464012\n" +
                "  },\n" +
                "  \"rawMessage\": \"8d7c49299905b614400414000000\",\n" +
                "  \"sensorSerialNumber\": 30789,\n" +
                "  \"RSSIPacket\": null,\n" +
                "  \"RSSIPreamble\": null,\n" +
                "  \"SNR\": null,\n" +
                "  \"confidence\": null\n" +
                "}";

            Double results = JsonUtils.jsonPathDouble(testJsonString, "$.timeAtServer");
            assertEquals(1429617900.999499, results);
    }



}