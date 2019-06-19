/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jms.example;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiPredicate;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.InitialContext;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple example that demonstrates server side load-balancing of messages between the queue instances on different
 * nodes of the cluster.
 */
public class ColocatedFailoverScaledownFailbackTest {

   private static Process server0;

   private static Process server1;

   static Logger log = LoggerFactory.getLogger(ColocatedFailoverScaledownFailbackTest.class);

   public static void main(final String[] args) throws Exception {
      final int numMessages = 30;

      Connection connection = null;
      Connection connection1 = null;

      InitialContext initialContext = null;
      InitialContext initialContext1 = null;

      try {
         server0 = ServerUtil.startServer(args[0], ColocatedFailoverScaledownFailbackTest.class.getSimpleName() + "0", 0, 5000);
         server1 = ServerUtil.startServer(args[1], ColocatedFailoverScaledownFailbackTest.class.getSimpleName() + "1", 1, 5000);

         System.out.println(args[0]);
         System.out.println(args[1]);

         Thread.sleep(15_000);

         validateState((nodeIds0, nodeIds1) -> 2 == nodeIds0.size() && 2 == nodeIds1.size() && nodeIds0.containsAll(nodeIds1),"each server should have the same 2 nodeIds");

         // Step 1. Get an initial context for looking up JNDI for both servers
         Hashtable<String, Object> properties = new Hashtable<>();
         properties.put("java.naming.factory.initial", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
         properties.put("connectionFactory.ConnectionFactory", "tcp://localhost:61617");
         initialContext1 = new InitialContext(properties);

         properties = new Hashtable<>();
         properties.put("java.naming.factory.initial", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
         properties.put("connectionFactory.ConnectionFactory", "tcp://localhost:61616?ha=true&retryInterval=1000&retryIntervalMultiplier=1.0&reconnectAttempts=-1");
         properties.put("queue.queue/exampleQueue", "exampleQueue");
         initialContext = new InitialContext(properties);

         // Step 2. Look up the JMS resources from JNDI
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");
         ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup("ConnectionFactory");
         ConnectionFactory connectionFactory1 = (ConnectionFactory) initialContext1.lookup("ConnectionFactory");

         // Step 3. Create a JMS Connections
         connection = connectionFactory.createConnection();
         connection1 = connectionFactory1.createConnection();

         // Step 4. Create a *non-transacted* JMS Session with client acknowledgement
         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         Session session1 = connection1.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         // Step 5. Create a JMS MessageProducers
         MessageProducer producer = session.createProducer(queue);
         MessageProducer producer1 = session1.createProducer(queue);

         // Step 6. Send some messages to both servers
         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session.createTextMessage("This is text message " + i);
            producer.send(message);
            System.out.println("Sent message: " + message.getText());
            message = session1.createTextMessage("This is another text message " + i);
            producer1.send(message);
            System.out.println("Sent message: " + message.getText());
         }

         // Step 7. Crash server #0, the live server, and wait a little while to make sure
         // it has really crashed
         ServerUtil.killServer(server0);
         System.out.println("Waiting for scale-down to complete...");
         Thread.sleep(10000);

         validateState((nodeIds0, nodeIds1) -> 0 == nodeIds0.size() && 2 == nodeIds1.size(),"server0 should be dead, server1 should have 2 nodeIds");

         // Step 8. start the connection ready to receive messages
         connection1.start();

         // Step 9.create a consumer
         MessageConsumer consumer = session1.createConsumer(queue);

         // Step 10. Receive and acknowledge all of the sent messages, notice that they will be out of order, this is
         // because they were initially round robined to both nodes then when the server failed were reloaded into the
         // live server.
         TextMessage message0 = null;
         for (int i = 0; i < numMessages * 2; i++) {
            message0 = (TextMessage) consumer.receive(5000);
            message0.acknowledge();
            if (message0 == null) {
               throw new IllegalStateException("Message not received!");
            }
            System.out.println("Got message: " + message0.getText());
         }

         // Step 11. Start server #0 back up
         System.out.println("STARTING SERVER AGAIN");
         server0 = ServerUtil.startServer(args[0], ColocatedFailoverScaledownFailbackTest.class.getSimpleName() + "0", 0, 5000);
         // give time to vote
         Thread.sleep(20_000);

         // sometimes we see server0 could not get a backup. sometimes we see server1 could not get a backup
         // we would expect that the server could rejoin and both brokers could each have a backup
         validateState((nodeIds0, nodeIds1) -> 2 == nodeIds0.size() && 2 == nodeIds1.size() && nodeIds0.containsAll(nodeIds1),"each server should have the same 2 nodeIds");

         //Step 12. Rebuild our connection to server #0
         connection = connectionFactory.createConnection();
         connection.start();
         session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         producer = session.createProducer(queue);
         producer1 = session.createProducer(queue);


         //Step 13. Send more messages to server #0
         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session.createTextMessage("This is text message 2 " + i);
            producer.send(message);
            System.out.println("Sent message: " + message.getText());
            message = session1.createTextMessage("This is another text message 2 " + i);
            producer1.send(message);
            System.out.println("Sent message: " + message.getText());
         }

         //Step 14. Kill server #0 again
         System.out.println("KILLING SERVER");
         ServerUtil.killServer(server0);
         System.out.println("Waiting for scale-down to complete...");
         Thread.sleep(15000);

         //Step 15. Try to read messages that were sent.
         message0 = null;
         for (int i = 0; i < numMessages * 2; i++) {
            message0 = (TextMessage) consumer.receive(5000);
            if (message0 == null) {
               throw new IllegalStateException("Message not received!");
            }
            System.out.println("Got message: " + message0.getText());
         }
         message0.acknowledge();

      } finally {
         // Step 16. Be sure to close our resources!

         if (connection != null) {
            connection.close();
         }

         if (initialContext != null) {
            initialContext.close();
         }
         if (connection1 != null) {
            connection1.close();
         }

         if (initialContext1 != null) {
            initialContext1.close();
         }

         ServerUtil.killServer(server0);
         ServerUtil.killServer(server1);
      }
   }


   private static SortedSet<String> obtainBrokerNodeIds(int i) throws Exception {

      SortedSet<String> nodeIds = new TreeSet<>();

      String label="broker"+i;
      int port = 9010+i;
      System.out.println("**********************************");

      try {
         MBeanServerConnection mbs = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:"+port+"/jmxrmi")).getMBeanServerConnection();
         Set<ObjectName> brokers = mbs.queryNames(new ObjectName("org.apache.activemq.artemis:broker=*"),null);
         for (ObjectName broker : brokers) {
            ActiveMQServerControl asc = JMX
                .newMBeanProxy(mbs, new ObjectName(broker.getCanonicalName()),
                    ActiveMQServerControl.class);
            String brokerName = broker.getCanonicalKeyPropertyListString();
            String nodeId = asc.getNodeID();
            log.info("{} contains {} {} nodeId={} ", label, brokerName, brokerName.contains("colocated_backup")?"of":"with", nodeId);
            nodeIds.add(nodeId);
         }
      } catch (IOException e) {
         log.warn("connect exception: {}", e.getMessage());
      }
      System.out.println("**********************************");
      return nodeIds;
   }

      private static void validateState(BiPredicate<SortedSet<String>, SortedSet<String>> test, String msg) throws Exception {
         validateState(test,msg, true);
      }

      private static void validateState(BiPredicate<SortedSet<String>,SortedSet<String>> test, String msg, boolean fullExit) throws Exception {

      System.out.println("**********************************");

      SortedSet<String> nodeIds0 = obtainBrokerNodeIds(0);
      SortedSet<String> nodeIds1 = obtainBrokerNodeIds(1);
      if(test.test(nodeIds0,nodeIds1)) {
         log.info("VALIDATION passed. {}", msg);
      } else {
         log.info("VALIDATION failed. {}", msg);
         if (fullExit) {
            throw new AssertionError(msg);
         }
      }
      System.out.println("**********************************");

      Thread.sleep(5_000);

   }
}
