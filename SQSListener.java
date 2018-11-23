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

import java.util.List;
import java.util.Map;

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
	}

	@Override
	public void open() throws ListenerException
	{
		super.open();
		
	}

	@Override
	public Map<String, Object> openThread() throws ListenerException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws ListenerException
	{
		super.close();
		
	}

	@Override
	public void closeThread(Map<String, Object> threadContext) throws ListenerException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getRawMessage(Map<String, Object> threadContext) throws ListenerException
	{
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest("https://sqs.eu-central-1.amazonaws.com/025885598068/S3NotificationsQueue").withMaxNumberOfMessages(1).withWaitTimeSeconds(30);
		final List<Message> message = getSqsClient().receiveMessage(receiveMessageRequest).getMessages();
		
		return message;
	}
	
	@Override
	public String getIdFromRawMessage(Object rawMessage, Map<String, Object> context) throws ListenerException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStringFromRawMessage(Object rawMessage, Map<String, Object> context) throws ListenerException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void afterMessageProcessed(PipeLineResult processResult, Object rawMessage, Map<String, Object> context)
			throws ListenerException
	{
		// TODO Auto-generated method stub
		
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
