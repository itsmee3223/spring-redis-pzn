package ramanda.ajisaka.asyraf.belajar.spring.redis;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.*;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest
public class StringTest {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void redisTemplate(){
        Assertions.assertNotNull(stringRedisTemplate);
    }

    @Test
    void string() throws InterruptedException {
        ValueOperations<String, String> operations = stringRedisTemplate.opsForValue();
        operations.set("name", "Ramanda", Duration.ofSeconds(2));
        Assertions.assertEquals("Ramanda", operations.get("name"));

        Thread.sleep(Duration.ofSeconds(3));
        Assertions.assertNull(operations.get("name"));
    }

    @Test
    void list() throws InterruptedException {
        ListOperations<String, String> operations = stringRedisTemplate.opsForList();
        operations.rightPush("names", "ramanda");
        operations.rightPush("names", "ajisaka");
        operations.rightPush("names", "asyraf");

        Assertions.assertEquals("ramanda", operations.leftPop("names"));
        Assertions.assertEquals("ajisaka", operations.leftPop("names"));
        Assertions.assertEquals("asyraf", operations.leftPop("names"));
    }

    @Test
    void set() throws InterruptedException {
        SetOperations<String, String> operations = stringRedisTemplate.opsForSet();
        operations.add("names", "ramanda");
        operations.add("names", "ramanda");
        operations.add("names", "ajisaka");
        operations.add("names", "ajisaka");
        operations.add("names", "asyraf");
        operations.add("names", "asyraf");

        Assertions.assertEquals(3, operations.members("names").size());
        assertThat(operations.members("names"), hasItems("ramanda", "ajisaka", "asyraf"));

        stringRedisTemplate.delete("names");
    }

    @Test
    void zSet() throws InterruptedException {
        ZSetOperations<String, String> operations = stringRedisTemplate.opsForZSet();
        operations.add("names", "ramanda", 100);
        operations.add("names", "ajisaka", 50);
        operations.add("names", "asyraf", 95);

        assertEquals("ramanda", operations.popMax("names").getValue());
        assertEquals("asyraf", operations.popMax("names").getValue());
        assertEquals("ajisaka", operations.popMax("names").getValue());
    }

    @Test
    void hash() throws InterruptedException {
        HashOperations<String, Object, Object> operations = stringRedisTemplate.opsForHash();
//        operations.put("user:1", "id", "1");
//        operations.put("user:1", "name", "ramanda");
//        operations.put("user:1", "email", "ramanda@mail.com");

        Map<Object, String> map = new HashMap<>();
        map.put("id", "1");
        map.put("name", "ramanda");
        map.put("email", "ramanda@mail.com");

        operations.putAll("user:1", map);

        assertEquals("1", operations.get("user:1", "id"));
        assertEquals("ramanda", operations.get("user:1", "name"));
        assertEquals("ramanda@mail.com", operations.get("user:1", "email"));

        stringRedisTemplate.delete("user:1");
    }

    @Test
    void geo() throws InterruptedException {
        GeoOperations<String, String> operations = stringRedisTemplate.opsForGeo();

        operations.add("sellers", new Point(105.259247, -5.382675), "toko a");
        operations.add("sellers", new Point(105.255272, -5.380674), "toko b");

        Distance distance = operations.distance("sellers", "toko a", "toko b", Metrics.KILOMETERS);
        assertEquals(0.4932, distance.getValue());

        GeoResults<RedisGeoCommands.GeoLocation<String>> sellers = operations.search(
                "sellers",
                new Circle(
                        new Point(105.258085, -5.382210),
                        new Distance(5, Metrics.KILOMETERS)
                )
        );


        assertEquals(2, sellers.getContent().size());
        assertEquals("toko b", sellers.getContent().get(0).getContent().getName());
        assertEquals("toko a", sellers.getContent().get(1).getContent().getName());
    }

    @Test
    void hyperLogLog() throws InterruptedException {
        HyperLogLogOperations<String, String> operations = stringRedisTemplate.opsForHyperLogLog();
        operations.add("traffics", "ramanda", "ajisaka", "asyraf");
        operations.add("traffics", "rama", "aji", "saka");
        operations.add("traffics", "ramanda", "ajisaka", "asyraf");

        assertEquals(6L, operations.size("traffics"));
    }

    @Test
    void transaction(){
        stringRedisTemplate.execute(new SessionCallback<Object>() {

            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();
                operations.opsForValue().set("firstName", "ramanda", Duration.ofSeconds(2));
                operations.opsForValue().set("lastName", "asyraf", Duration.ofSeconds(2));
                operations.exec();
                return null;
            }
        });

        assertEquals("ramanda", stringRedisTemplate.opsForValue().get("firstName"));
        assertEquals("asyraf", stringRedisTemplate.opsForValue().get("lastName"));
    }

    @Test
    void pipeline(){
        List<Object> list = stringRedisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.opsForValue().set("firstName", "ramanda");
                operations.opsForValue().set("middlename", "ajisaka");
                operations.opsForValue().set("lastName", "asyraf");
                return null;
            }
        });

        assertThat(list, hasSize(3));
        assertThat(list, hasItem(true));
        assertThat(list, not(hasItem(false)));
    }

    @Test
    void publish(){
        StreamOperations<String, Object, Object> operations = stringRedisTemplate.opsForStream();
        MapRecord<String, String, String> record = MapRecord.create("stream-1", Map.of(
                "name", "ramanda",
                "address", "Indonesia"
        ));

        for (int i = 0; i < 10; i++) {
            operations.add(record);
        }
    }

    @Test
    void subscribe(){
        StreamOperations<String, Object, Object> operations = stringRedisTemplate.opsForStream();
        try {
            operations.createGroup("stream-1", "sample-group");
        } catch (RedisSystemException exception){
            // group already exist
        }

        List<MapRecord<String, Object, Object>> records = operations.read(Consumer.from("sample-group", "sample1"),
                StreamOffset.create("stream-1", ReadOffset.lastConsumed()));

        for (MapRecord<String, Object, Object> record : records) {
            System.out.println(record);
        }
    }
}

