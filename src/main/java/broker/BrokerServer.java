package broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;


/**
 * @Author xcy
 * @Date 2023/4/20 14:58
 * @Description: TODO
 */
public class BrokerServer implements Runnable{

    private static final Logger logger = Logger.getLogger(BrokerServer.class.getName());

    public static int SERVICE_PORT = 9999;

    private final Socket socket;

    private final static int MAX_SIZE = 10;

    // 保存消息数据的容器
    private static ArrayBlockingQueue<String> messageQueue = new ArrayBlockingQueue<>(MAX_SIZE);


    private BrokerServer(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream())
        ) {
            while (true) {
                String str = in.readLine();
                if (str == null) {
                    continue;
                }

                if (str.equals("CONSUME")) { //CONSUME 表示要消费一条消息
                    //从消息队列中消费一条消息
                    String message = Broker.consume(messageQueue);
                    out.println(message);
                    out.flush();
                } else {
                    //其他情况都表示生产消息放到消息队列中
                    Broker.produce(str, messageQueue);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        initRobotMq();
    }

    private static void initRobotMq() throws IOException {
        ServerSocket server = new ServerSocket(SERVICE_PORT);
        logger.info("robotMq is ready");
        while (true) {
            BrokerServer brokerServer = new BrokerServer(server.accept());
            new Thread(brokerServer).start();
        }
    }

    private static void initRobotMq(Integer customQueuesize) throws IOException {
        messageQueue = new ArrayBlockingQueue<>(customQueuesize);
        ServerSocket server = new ServerSocket(SERVICE_PORT);
        logger.info("robotMq is ready");
        while (true) {
            BrokerServer brokerServer = new BrokerServer(server.accept());
            new Thread(brokerServer).start();
        }
    }
}
