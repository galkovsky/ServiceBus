package def;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.microsoft.windowsazure.services.serviceBus.*;
import com.microsoft.windowsazure.services.serviceBus.models.*;
import com.microsoft.windowsazure.services.core.*;

import javax.xml.datatype.*;
import javax.xml.datatype.DatatypeConstants.Field;

public class TestServiceBusRelay
{

	// Все операции по управлению вашим контейнером Service Bus выполняются с
	// помощью класса ServiceBusContract, при создании которого конструктору
	// передаётся
	// конфигурация вашего контейнера - название контейнера, имя issuer и ключ.
	// Все эти данные можно получить из панели свойств Properties вашего
	// контейнера Service Bus на портале Windows Azure.
	// Далее можно воспользоваться функциональностью класса ServiceBusService,
	// предоставляющего управление очередями - создание, удаление и т.д. В
	// данном методе создаётся контейнер Service Bus с именем ahrimansb.

	private static ServiceBusContract createServiceBus(String issuer, String key)
	{

		Configuration config = ServiceBusConfiguration.configureWithWrapAuthentication("testbus", issuer, key);
		ServiceBusContract service = ServiceBusService.create(config);
		return service;
	}

	// В данном методе используется функциональность класса TopicInfo, с помощью
	// которой в данном случае определяется
	// максимальный размер очереди в мегабайтах (указывается максимальный размер
	// в 5Гб). С ��омощью методов TopicInfo можно настраивать различные
	// параметры
	// ваших очередей, в т.ч. Time To Live для сообщений, максимальный размер и
	// многое другое.

	private static void createTopic(String name, ServiceBusContract service)
	{

		long queueSize = 5120;

		TopicInfo topicInfo = new TopicInfo(name);
		try
		{
			topicInfo.setMaxSizeInMegabytes(queueSize);
			CreateTopicResult result = service.createTopic(topicInfo);

		} catch (ServiceException e)
		{
			System.out.print("ServiceException: " + e.getMessage());
		}

	}

	// В данном методе используется фильтр по умолчанию MatchAll (поэтому нет
	// никаких дополнительных указаний значения фильтра).
	// При использовании фильтра по умолчанию все сообщения, которые поступают в
	// топик, помещаются в подписку-очередь.

	private static void createSubscriptionWithFilterMatchAll(String subscriptionInfoName, String topicName, ServiceBusContract service)
	{

		try
		{
			SubscriptionInfo subInfo = new SubscriptionInfo(subscriptionInfoName);
			CreateSubscriptionResult result = service.createSubscription(topicName, subInfo);

		} catch (ServiceException e)
		{
			System.out.print("ServiceException: " + e.getMessage());
		}
	}

	// В данном методе создаётся ещё одна подписка с SQL-фильтром сообщений
	// (SqlFilter). При этом
	// в качестве условия используется сравнение некоторого custom-свойства
	// MessageSequenceId.
	// После создания двух подписок сообщения, поступающие в соответствующий
	// топик, буду уходить -
	// все в первую подписку, и только удовлетворяющие условиям фильтра - во
	// вторую, таким образом распределяясь по обработчикам. Естественно,
	// что гораздо оптимальнее создать несколько подписок с разными фильтрами и
	// распределять сообщения по определенному условию.

	private static void createSubscriptionWithFilter(String subscriptionInfoName, String topicName, ServiceBusContract service)
	{
		SubscriptionInfo subInfo = new SubscriptionInfo(subscriptionInfoName);

		try
		{
			CreateSubscriptionResult result = service.createSubscription(topicName, subInfo);
		} catch (ServiceException e)
		{

			e.printStackTrace();
		}
		RuleInfo ruleInfo = new RuleInfo();
		ruleInfo = ruleInfo.withSqlExpressionFilter("MessageNumber > 4");
		try
		{
			CreateRuleResult ruleResult = service.createRule(topicName, "subscriptioninfoname2", ruleInfo);
		} catch (ServiceException e)
		{
			e.printStackTrace();
		}

	}

	// Для отображения сообщения в объектной модели существует класс
	// BrokeredMessage, объекты которого содержат набор методов для управления
	// сообщением,
	// набор параметров и набор данных. В набор данных можно передать любой
	// сериализуемый объект. Очереди Service Bus имеют ограничение на размер
	// сообщения в 256 мегабайт(заголовок, содержащий свойства - 64 мегабайт),
	// однако ограничений как таковых на количество хранимых в очереди сообщений
	// нет, кроме задаваемого вами ограничения-максимального размера очереди. В
	// данном случае мы также указываем custom-свойство MessageSequenceId,
	// которое будет использоваться для фильтра

	private static void putMessageToTopic(String topicName, ServiceBusContract service, BrokeredMessage message)
	{

		try
		{
			message.setProperty("MessageNumber", "6");
			service.sendTopicMessage(topicName, message);
		} catch (ServiceException e)
		{
			System.out.print("ServiceException: " + e.getMessage());
		}

	}

	// Метод для добавления множества сообщений в топик.
	private static void putMessagesToTopic(String topicName, ServiceBusContract service, List<BrokeredMessage> messages)
	{

		try
		{
			for (BrokeredMessage message : messages)
			{

				service.sendTopicMessage(topicName, message);
			}
		} catch (ServiceException e)
		{
			e.printStackTrace();
		}
	}

	// Аналогично сервису очередей хранилища Windows Azure, вы можете
	// использовать два метода для извлечения сообщений из очередей топиков -
	// получение и удаление (ReceiveAndDelete) и "подсматривание" - получение
	// сообщение, но не удаление его из очереди топика (PeekLock). При
	// использовании
	// метода ReceiveAndDelete при получении запроса на извлечение
	// сообщения, очередь помечает это сообщение как "потреблённое".
	// При использовании метода PeekLock процесс получения дробится на два этапа
	// - когда Service Bus получает запрос на извлечение сообщения, он находит
	// это сообщение, помечает его как locked (в этот момент другие обработчики
	// перестают видеть это сообщение) и возвращает его приложению. После
	// окончания
	// обработки приложением сообщения закрывается второй этап процесса с
	// помощью вызова метода Delete полученного сообщения. После этого сообщение
	// помечается как удаленное.
	// Типичным паттерном опроса очереди на наличие новых сообщений является
	// использование бесконечного цикла while. В данном методе очередь
	// опрашивается постоянно. Если вы хотите
	// ограничить выполнение каким-либо количеством полученных сообщений, вам
	// необходимо реализовать логику с использованием break.

	private static void getMessageFromTopic(String topicName, String subscriptionName, ServiceBusContract service) throws ServiceException
	{

		ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
		opts.setReceiveMode(ReceiveMode.PEEK_LOCK);
		while (true)
		{

			ReceiveSubscriptionMessageResult result = service.receiveSubscriptionMessage(topicName, subscriptionName, opts);
			BrokeredMessage message = result.getValue();
			if (message != null && message.getMessageId() != null)
			{
				try
				{
					System.out.println("Начало работы по опросу очереди подписки с именем:" + subscriptionName);

					System.out.println("Сообщение: " + convertStreamToString(message.getBody()));
					System.out.println("ID сообщения: " + message.getMessageId());
					System.out.println("Если вы задали какое-то свойство, его можно получить с помощью метода getProperty(): "
							+ message.getProperty("MessageNumber"));
					System.out.println("Сообщение прочитано - удалено.");
					service.deleteMessage(message);
				} catch (Exception ex)
				{
					// если было выброшено исключение, сообщение будет
					// разблокировано для других обработчиков
					System.out.println("Исключение!");
					service.unlockMessage(message);
				}
			} else
			{
				System.out.println("Больше нет сообщений, но топик продолжает опрашиваться.");
			}
		}

	}

	private static void deleteTopic(String topicName, ServiceBusContract service)
	{

		try
		{
			service.deleteTopic(topicName);
		} catch (ServiceException e)
		{
			e.printStackTrace();
		}

	}

	private static void deleteSubscription(String subscriptionName, String subscriptionInfoName, ServiceBusContract service)
	{

		try
		{
			service.deleteSubscription(subscriptionName, subscriptionInfoName);
		} catch (ServiceException e)
		{

			e.printStackTrace();
		}

	}

	public static void main(String args[]) throws FileNotFoundException
	{

		ServiceBusContract service = createServiceBus("owner", "uM00afnbOrtzIhUQjOQPXKtg7g/iJs8KAjG3xmTS/2E=");
		String topicName = "mytopic3";
		createTopic(topicName, service);
		createSubscriptionWithFilterMatchAll("subscriptioninfoname1", topicName, service);
//		createSubscriptionWithFilter("subscriptioninfoname2", "mytopic1", service);
		InputStream input = new FileInputStream("c:\\1.txt");
		BrokeredMessage message = new BrokeredMessage("Our message.");
		message.setBody(input);
		ArrayList<BrokeredMessage> messages = new ArrayList<BrokeredMessage>();
		for (int i = 0; i < 5; i++)
		{

			BrokeredMessage msg = new BrokeredMessage("Message text: " + i);
			msg.setProperty("MessageNumber", i);
			messages.add(msg);

		}

		putMessageToTopic(topicName, service, message);
		putMessagesToTopic(topicName, service, messages);

		try
		{
			getMessageFromTopic(topicName, "subscriptioninfoname1", service);
			getMessageFromTopic(topicName, "mysubscriptionwithfilter", service);

		} catch (ServiceException e)
		{
			e.printStackTrace();
		}

	}

	public static String convertStreamToString(InputStream is) throws IOException
	{

		if (is != null)
		{
			Writer writer = new StringWriter();

			char[] buffer = new char[1024];
			try
			{
				Reader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
				int n;
				while ((n = reader.read(buffer)) != -1)
				{
					writer.write(buffer, 0, n);
				}
			} finally
			{
				is.close();
			}
			return writer.toString();
		} else
		{
			return "";
		}
	}

}