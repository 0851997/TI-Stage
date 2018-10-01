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
package nl.nn.adapterframework.senders;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;

import nl.nn.adapterframework.configuration.ConfigurationException;
import nl.nn.adapterframework.core.SenderException;
import nl.nn.adapterframework.core.SenderWithParametersBase;
import nl.nn.adapterframework.core.TimeOutException;
import nl.nn.adapterframework.parameters.ParameterResolutionContext;
import nl.nn.adapterframework.util.CredentialFactory;

/**
 * To be S3Sender.
 * 
 */

public class S3Sender extends SenderWithParametersBase
{
	// fields
	/*private String credsAlias;
	private String accessKeyID;
	private String secretAccessKey;
	private CredentialFactory credentials;*/	
	private String name;
	
	private AmazonS3 s3Client;
	private boolean pathStyleAccessEnabled = false;
	private boolean chunkedEncodingDisabled = false;
	private boolean accelerateModeEnabled = false;				//this may involve some extra costs
	private boolean payloadSigningEnabled = false;
	private boolean dualstackEnabled = false;
	private boolean forceGlobalBucketAccessEnabled = false;
	private String clientRegion = Regions.EU_CENTRAL_1.getName();
	private List<String> regions = getAvailableRegions();
	
	private String bucketName;
	private String objectKey;
	
	
	@Override
	public void configure() throws ConfigurationException
	{
		super.configure();
		/*if (StringUtils.isEmpty(getAccessKeyID()) && StringUtils.isEmpty(getSecretAccessKey()))
		{
			throw new ConfigurationException(getLogPrefix() + " must specify accessKeyID and secretAccessKey to access the service");
		}*/
		if (StringUtils.isEmpty(bucketName))
			throw new ConfigurationException(getLogPrefix() + " must specify bucketName for the destination");

		if(!regions.contains(getClientRegion()))
			throw new ConfigurationException(getLogPrefix()+" unknown or invalid region ["+getClientRegion()+"] supported regions are: "+regions.toString());


		//credentials = new CredentialFactory(getCredsAlias(), getAccessKeyID(), getSecretAccessKey());
		//BasicAWSCredentials awsCreds = new BasicAWSCredentials(getAccessKeyID(), getSecretAccessKey());
		s3Client = AmazonS3ClientBuilder.standard()
										.withPathStyleAccessEnabled(isPathStyleAccessEnabled())
										.withChunkedEncodingDisabled(isChunkedEncodingDisabled())
										.withAccelerateModeEnabled(isAccelerateModeEnabled())
										.withPayloadSigningEnabled(isPayloadSigningEnabled())
										.withDualstackEnabled(isDualstackEnabled())
										.withForceGlobalBucketAccessEnabled(isForceGlobalBucketAccessEnabled())
										.withRegion(getClientRegion())
										.withCredentials(new EnvironmentVariableCredentialsProvider() /*new AWSStaticCredentialsProvider(awsCreds)*/)
										.build();

	}

	public void open()
	{

	}

	public void close()
	{
		s3Client.shutdown();
	}

	public String sendMessage(String correlationID, String message, ParameterResolutionContext prc) throws SenderException, TimeOutException
	{
		try
		{
			if (s3Client.doesBucketExistV2(bucketName))
			{
				System.out.println("Bucket exists.\n" + "bucketName: " + bucketName);
				s3Client.putObject(bucketName, objectKey, message);
			} 
			else
			{
				System.out.println("Creating new bucket...");
				s3Client.createBucket(bucketName);
				s3Client.putObject(bucketName, objectKey, message);
			}
		}
		catch (AmazonServiceException e)
		{
			// The call was transmitted successfully, but Amazon S3 couldn't process
			// it, so it returned an error response.
			e.printStackTrace();
		} 
		catch (SdkClientException e)
		{
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}
		
		return message;
	}
	
	public List<String> getAvailableRegions()
	{
		List<String> availableRegions = new ArrayList<String>(Regions.values().length);
		for (Regions region : Regions.values())
		{
			availableRegions.add(region.getName());
		}
		//System.out.println(availableRegions);
		
		return availableRegions;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public boolean isPathStyleAccessEnabled()
	{
		return pathStyleAccessEnabled;
	}

	public void setPathStyleAccessEnabled(boolean pathStyleAccessEnabled)
	{
		this.pathStyleAccessEnabled = pathStyleAccessEnabled;
	}

	public boolean isChunkedEncodingDisabled()
	{
		return chunkedEncodingDisabled;
	}

	public void setChunkedEncodingDisabled(boolean chunkedEncodingDisabled)
	{
		this.chunkedEncodingDisabled = chunkedEncodingDisabled;
	}

	public boolean isAccelerateModeEnabled()
	{
		return accelerateModeEnabled;
	}

	public void setAccelerateModeEnabled(boolean accelerateModeEnabled)
	{
		this.accelerateModeEnabled = accelerateModeEnabled;
	}

	public boolean isPayloadSigningEnabled()
	{
		return payloadSigningEnabled;
	}

	public void setPayloadSigningEnabled(boolean payloadSigningEnabled)
	{
		this.payloadSigningEnabled = payloadSigningEnabled;
	}

	public boolean isDualstackEnabled()
	{
		return dualstackEnabled;
	}

	public void setDualstackEnabled(boolean dualstackEnabled)
	{
		this.dualstackEnabled = dualstackEnabled;
	}

	public boolean isForceGlobalBucketAccessEnabled()
	{
		return forceGlobalBucketAccessEnabled;
	}

	public void setForceGlobalBucketAccessEnabled(boolean forceGlobalBucketAccessEnabled)
	{
		this.forceGlobalBucketAccessEnabled = forceGlobalBucketAccessEnabled;
	}

	public String getClientRegion()
	{
		return clientRegion;
	}

	public void setClientRegion(String clientRegion)
	{
		this.clientRegion = clientRegion;
	}

	public String getBucketName()
	{
		return bucketName;
	}

	public void setBucketName(String bucketName)
	{
		this.bucketName = bucketName;
	}

	public String getObjectKey()
	{
		return objectKey;
	}

	public void setObjectKey(String objectKey)
	{
		this.objectKey = objectKey;
	}

	/*
	 * private String[] regions = {"us-gov-west-1", "us-east-1", "us-east-2",
	 * "us-west-1", "us-west-2", "eu-west-1", "eu-west-2", "eu-west-3",
	 * "eu-central-1", "ap-south-1", "ap-southeast-1", "ap-southeast-2",
	 * "ap-northeast-1", "ap-northeast-2", "sa-east-1", "cn-north-1",
	 * "cn-northwest-1", "ca-central-1"};
	 */
}