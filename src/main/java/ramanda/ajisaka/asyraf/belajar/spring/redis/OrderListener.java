package ramanda.ajisaka.asyraf.belajar.spring.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderListener implements StreamListener<String, ObjectRecord<String, Order>> {

    @Override
    public void onMessage(ObjectRecord<String, Order> message) {
        log.info("Receive order: {}", message.getValue());
    }
}
