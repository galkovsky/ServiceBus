package def;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Scanner;

import org.codehaus.jackson.util.CharTypes;

import com.microsoft.windowsazure.services.core.Configuration;
import com.microsoft.windowsazure.services.core.ServiceException;
import com.microsoft.windowsazure.services.serviceBus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.serviceBus.ServiceBusContract;
import com.microsoft.windowsazure.services.serviceBus.ServiceBusService;
import com.microsoft.windowsazure.services.serviceBus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.serviceBus.models.GetQueueResult;
import com.microsoft.windowsazure.services.serviceBus.models.QueueInfo;
import com.microsoft.windowsazure.services.serviceBus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.serviceBus.models.ReceiveMode;
import com.microsoft.windowsazure.services.serviceBus.models.ReceiveQueueMessageResult;
import com.microsoft.windowsazure.services.serviceBus.models.ReceiveSubscriptionMessageResult;
import com.microsoft.windowsazure.services.serviceBus.models.SubscriptionInfo;
import com.microsoft.windowsazure.services.serviceBus.models.TopicInfo;

public class AnotherTest
{
	final static String TOPIC_PATH = "mytopic3";
	final static String SUB_NAME = "subscriptioninfoname2";

	static Configuration config;
	static ServiceBusContract serviceBusContract;

	public static void main(String[] args) throws ServiceException, IOException, ClassNotFoundException
	{
		config = ServiceBusConfiguration
				.configureWithWrapAuthentication("testbus", "owner", "uM00afnbOrtzIhUQjOQPXKtg7g/iJs8KAjG3xmTS/2E=");
		serviceBusContract = ServiceBusService.create(config);

		// TopicInfo topicInfo = new TopicInfo(TOPIC_PATH);
		// serviceBusContract.createTopic(topicInfo);

		// SubscriptionInfo subscription = new SubscriptionInfo(SUB_NAME);
		// serviceBusContract.createSubscription(TOPIC_PATH, subscription);

		BrokeredMessage message = new BrokeredMessage("TEST message".getBytes(Charset.forName("UTF-8")));
		message.setLabel("HER");

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream stream = new ObjectOutputStream(byteArrayOutputStream);

		Person p = new Person();
		p.fname = "FUCK";
		stream.writeObject(p);

		message.setBody(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
		serviceBusContract.sendTopicMessage(TOPIC_PATH, message);

		System.out.println(serviceBusContract.getSubscription(TOPIC_PATH, SUB_NAME).getValue().getMessageCount());

		while (true)
		{
			ReceiveMessageOptions options = new ReceiveMessageOptions();
			options.setReceiveMode(ReceiveMode.RECEIVE_AND_DELETE);
			ReceiveSubscriptionMessageResult result = serviceBusContract.receiveSubscriptionMessage(TOPIC_PATH, SUB_NAME, options);
			BrokeredMessage received_message = result.getValue();

			ObjectInputStream inputStream = new ObjectInputStream(received_message.getBody());

			System.out.println(((Person)inputStream.readObject()).fname);
		}

	}

	public static String toString(InputStream in)
	{
		Scanner s = new Scanner(in);

		return s.nextLine();
	}

	private static void queue() throws ServiceException
	{

		GetQueueResult result = serviceBusContract.getQueue("DataCollectionQueue1");
		QueueInfo queueInfo = result.getValue();

		// serviceBusContract.createQueue(queueInfo);

		BrokeredMessage message = new BrokeredMessage("LINUX");
		message.setLabel("DataCollectionQueue-Message");
		message.setProperty("Type", new String("FUCK"));

		serviceBusContract.sendQueueMessage(TOPIC_PATH, message);

		System.out.println("Count : " + queueInfo.getMessageCount());

		ReceiveMessageOptions options = new ReceiveMessageOptions();
		options.setReceiveMode(ReceiveMode.RECEIVE_AND_DELETE);

		ReceiveQueueMessageResult another_result;
		do
		{
			another_result = serviceBusContract.receiveQueueMessage(TOPIC_PATH, options);
			BrokeredMessage another_message = another_result.getValue();

			System.out.println(toString(another_message.getBody()));

		} while (true);
	}

}

class Person implements Serializable
{

	public String fname = "FUCK";
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
