package ramanda.ajisaka.asyraf.belajar.spring.redis;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.*;

import java.time.Duration;

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
}

