package ramanda.ajisaka.asyraf.belajar.spring.redis;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

@SpringBootTest
public class StringTest {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void redisTemplate(){
        Assertions.assertNotNull(stringRedisTemplate);
    }
}

