/*
   Copyright 2013 Nationale-Nederlanden

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package nl.nn.adapterframework.receivers;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.QueueConfiguration;
import com.amazonaws.services.s3.model.S3Event;
import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import nl.nn.adapterframework.configuration.ConfigurationException;
import nl.nn.adapterframework.core.IPullingListener;
import nl.nn.adapterframework.core.ListenerException;
import nl.nn.adapterframework.core.PipeLineResult;

public class SQSListener extends SQSFacade implements IPullingListener
{
	private String name;
	
	@Override
	public void configure() throws ConfigurationException
	{
		super.configure();
System.out.println("congfiguring");
	}

	@Override
	public void open() throws ListenerException
	{
		super.open();
System.out.println("open method: starting everything");
	}

	@Override
	public Map<String, Object> openThread() throws ListenerException
	{
System.out.println("open thread method");
		return null;
	}

	@Override
	public void close() throws ListenerException
	{
		super.close();
System.out.println("close methode: stops everything before exiting");
	}

	@Override
	public void closeThread(Map<String, Object> threadContext) throws ListenerException
	{
System.out.println("close thread method");
	}

	@Override
	public synchronized Object getRawMessage(Map<String, Object> threadContext) throws ListenerException
	{
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest("https://sqs.eu-west-1.amazonaws.com/025885598068/S3NotificationQueue.fifo").withMaxNumberOfMessages(1);
		final List<Message> message = getSqsClient().receiveMessage(receiveMessageRequest).getMessages();
System.out.println("messages: "+message);
		
		return message;
	}
	
	@Override
	public String getIdFromRawMessage(Object rawMessage, Map<String, Object> context) throws ListenerException
	{
System.out.println("getIdFromRawMessage method");
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStringFromRawMessage(Object rawMessage, Map<String, Object> context) throws ListenerException
	{
System.out.println("getStringFromRawMessage");
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void afterMessageProcessed(PipeLineResult processResult, Object rawMessage, Map<String, Object> context)
			throws ListenerException
	{
		System.out.println("afterMessageProcessed");
		// TODO Auto-generated method stub
		
	}

	//temp method for SQSSender
	public void setBucketNotificationConfiguration(String bucketName)
	{
		try 
		{
			BucketNotificationConfiguration notificationConfiguration = new BucketNotificationConfiguration();
			notificationConfiguration.addConfiguration("sqsQueueObjectCreatedConfig", new QueueConfiguration("arn:aws:sqs:eu-west-1:025885598068:S3NotificationQueue.fifo", EnumSet.of(S3Event.ObjectCreated)));
			notificationConfiguration.addConfiguration("sqsQueueObjectRemovedConfig", new QueueConfiguration("arn:aws:sqs:eu-west-1:025885598068:S3NotificationQueue.fifo", EnumSet.of(S3Event.ObjectRemoved)));
			SetBucketNotificationConfigurationRequest request = new SetBucketNotificationConfigurationRequest(bucketName, notificationConfiguration);
			System.out.println(bucketName+", "+notificationConfiguration.getConfigurations().toString());
			//s3Client.setBucketNotificationConfiguration(request);
		}
		catch(AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process 
            // it, so it returned an error response.
            e.printStackTrace();
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
	}

	
	@Override
	public String getName()
	{
		return name;
	}
	
	@Override
	public void setName(String name)
	{
		this.name = name;
	}
}
