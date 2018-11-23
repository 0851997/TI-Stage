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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import nl.nn.adapterframework.configuration.ConfigurationException;
import nl.nn.adapterframework.core.HasPhysicalDestination;
import nl.nn.adapterframework.core.ListenerException;

public class SQSFacade implements HasPhysicalDestination
{
	private String name;
	private AmazonSQSClientBuilder sqsClientBuilder;
	private AmazonSQS sqsClient;
	public static final List<String> AVAILABLE_REGIONS = getAvailableRegions();
	
	private String clientRegion = Regions.EU_CENTRAL_1.getName();

	public void configure() throws ConfigurationException
	{
		if(StringUtils.isEmpty(getClientRegion()) || !AVAILABLE_REGIONS.contains(getClientRegion()))
			throw new ConfigurationException(getLogPrefix() + " region unknown or is not specified [" + getClientRegion() + "] please use following supported regions: " + AVAILABLE_REGIONS.toString());
		
		sqsClientBuilder = AmazonSQSClientBuilder.standard()
												 .withRegion(getClientRegion())
												 .withCredentials(new EnvironmentVariableCredentialsProvider());
	}

	public void open() throws ListenerException
	{
		sqsClient = sqsClientBuilder.build();
	}

	public void close() throws ListenerException
	{
		sqsClient.shutdown();
	}

	protected String getLogPrefix() 
	{
		return "["+this.getClass().getName()+"] ["+getName()+"] ";
	}
	
	public static List<String> getAvailableRegions()
	{
		//this method checks for available regions in AWS
		List<String> availableRegions = new ArrayList<String>(Regions.values().length);
		for (Regions region : Regions.values())
		{
			availableRegions.add(region.getName());
			//System.out.println(region.getName());
		}
		
		return availableRegions;
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

	public String getClientRegion()
	{
		return clientRegion;
	}
	
	public void setClientRegion(String clientRegion)
	{
		this.clientRegion = clientRegion;
	}

	@Override
	public String getPhysicalDestinationName()
	{
		return null;
	}
	
	public AmazonSQS getSqsClient() {
		return sqsClient;
	}
	
}