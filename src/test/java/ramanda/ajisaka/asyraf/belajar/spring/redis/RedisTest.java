package ramanda.ajisaka.asyraf.belajar.spring.redis;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.*;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.support.collections.RedisList;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

@SpringBootTest
public class RedisTest {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private ProductService productService;

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

    @Test
    void pubSub(){
        stringRedisTemplate.getConnectionFactory().getConnection().subscribe(new MessageListener() {
            @Override
            public void onMessage(Message message, byte[] pattern) {
                System.out.println("Message: " + new String(message.getBody()));
            }
        }, "my-channel".getBytes());

        for (int i = 0; i < 10; i++) {
            stringRedisTemplate.convertAndSend("my-channel", "Hallo kamu");
        }
    }

    @Test
    void redisList(){
        List<String> list = RedisList.create("names", stringRedisTemplate);
        list.add("Ramanda");
        list.add("Ajisaka");
        list.add("Asyraf");

        List<String> names = stringRedisTemplate.opsForList().range("names", 0, -1);
        assertThat(list, hasItems("Ramanda", "Ajisaka", "Asyraf"));
        assertThat(names, hasItems("Ramanda", "Ajisaka", "Asyraf"));
    }

    @Test
    void redisRepository(){
        Product product = Product.builder()
                .id("1")
                .name("kamu")
                .price(1000L)
                .build();

        productRepository.save(product);

        Product product2 = productRepository.findById("1").get();
        assertEquals(product, product2);

        Map<Object, Object> map = stringRedisTemplate.opsForHash().entries("products:1");
        assertEquals(product.getId(), map.get("id"));
        assertEquals(product.getName(), map.get("name"));
        assertEquals(product.getPrice().toString(), map.get("price"));
    }

    @Test
    void ttl() throws InterruptedException {
        Product product = Product.builder()
                .id("1")
                .name("kamu")
                .price(1000L)
                .ttl(3L)
                .build();

        productRepository.save(product);

        assertTrue(productRepository.findById("1").isPresent());
        Thread.sleep(Duration.ofSeconds(5L));
        assertFalse(productRepository.findById("1").isPresent());

    }

    @Test
    void cache(){
        Cache scores = cacheManager.getCache("scores");
        scores.put("Ramanda", 50);
        scores.put("Kamu", 50);

        assertEquals(scores.get("Kamu", Integer.class), scores.get("Ramanda", Integer.class));

        scores.evict("Ramanda");
        scores.evict("Kamu");
        assertNull(scores.get("Ramanda", Integer.class));
        assertNull(scores.get("Kamu", Integer.class));
    }

    @Test
    void cacheable(){
        Product product = productService.getProduct("001");
        assertEquals("001", product.getId());

        Product product2 = productService.getProduct("001");
        assertEquals(product, product2);
    }

    @Test
    void cachePut(){
        Product product = Product.builder()
                .id("P02")
                .name("kamu")
                .price(1000L)
                .ttl(3L)
                .build();

        productService.save(product);
        Product product2 = productService.getProduct("P02");
        assertEquals(product, product2);
    }

    @Test
    void cacheEvict(){
        Product product = productService.getProduct("P003");

        assertEquals("P003", product.getId());

        productService.remove("P003");

        Product product2 = productService.getProduct("P003");
        assertEquals(product, product2);
    }

}