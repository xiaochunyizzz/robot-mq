package broker;

import cn.hutool.core.util.StrUtil;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

/**
 * @Author xcy
 * @Date 2023/4/20 14:54
 * @Description: TODO
 */
class Broker {

    private static final Logger logger = Logger.getLogger(Broker.class.getName());

    // 生产消息
    static void produce(String msg, ArrayBlockingQueue<String> messageQueue) {
        if (!messageQueue.offer(msg)) {
            logger.info("The maximum payload of messages that can be stored in the message processing center has been reached, and no further messages can be added!");
        }
    }

    // 消费消息
    static String consume(ArrayBlockingQueue<String> messageQueue) {
        String msg = messageQueue.poll();
        if(StrUtil.isBlank(msg)) {
            logger.info("the message queue is empty!");
        }
        return msg;
    }

}
