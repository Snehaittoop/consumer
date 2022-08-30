import com.fasterxml.jackson.databind.util.JSONPObject;
import netscape.javascript.JSObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.sys.Prop;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import java.sql.*;
import org.json.*;

public class Consumer {
    public static void main(String[] args) {

        KafkaConsumer consumer;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("group.id", "test");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("Demonest1"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
                Connection conn = null;
                Statement stmt = null;
                String fetchedValue=record.value();
                System.out.println(fetchedValue);
                JSONObject jsonObject=new JSONObject(fetchedValue);
                System.out.println(jsonObject);

                String getTemp=String.valueOf(jsonObject.getInt("temp"));
                String getHumd=String.valueOf(jsonObject.getInt("humidity"));
                System.out.println(getTemp);
                System.out.println(getHumd);
                try {




                    Class.forName("com.mysql.cj.jdbc.Driver");
                    conn = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/iotdb", "root", "");
                    stmt = (Statement) conn.createStatement();


                    String qry = "INSERT INTO `temprature`(`temprature`, `humidity`) VALUES  (" +getTemp+ ","+getHumd+")";
                    stmt.executeUpdate(qry);
                    System.out.println(qry);
                    System.out.println("succesfully created");




                } catch (Exception e) {
                    System.out.println(e);
                }
            }


        }
    }
}
