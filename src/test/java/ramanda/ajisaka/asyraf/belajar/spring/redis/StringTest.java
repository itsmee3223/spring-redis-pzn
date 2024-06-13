package ramanda.ajisaka.asyraf.belajar.spring.redis;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;

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
}

