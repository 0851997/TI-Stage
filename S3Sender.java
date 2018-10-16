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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import nl.nn.adapterframework.configuration.ConfigurationException;
import nl.nn.adapterframework.core.ParameterException;
import nl.nn.adapterframework.core.SenderException;
import nl.nn.adapterframework.core.SenderWithParametersBase;
import nl.nn.adapterframework.core.TimeOutException;
import nl.nn.adapterframework.parameters.ParameterList;
import nl.nn.adapterframework.parameters.ParameterResolutionContext;
import nl.nn.adapterframework.parameters.ParameterValue;
import nl.nn.adapterframework.parameters.ParameterValueList;
import nl.nn.adapterframework.util.CredentialFactory;

/**
 * <p>
 * S3Sender, makes possible for Ibis developer to interact with Amazon Simple Storage Service (Amazon S3). It allows to create
 * and delete buckets. More so it makes it possible for you to upload object(s) inside a bucket, delete object(s) from a bucket and 
 * copy object(s) from one bucket too another.
 * </p>
 * 
 * <p>
 * <b>Configuration:</b>
 * <table border="1">
 * <tr><th>attributes</th><th>description</th><th>default</th></tr>
 * <tr><td>{@link #setName(String) name}</td><td>Set a name for your sender</td><td></td></tr>
 * <tr><td>{@link #setChunkedEncodingDisabled(boolean) chunkedEncodingDisabled}</td><td>Configures the client to disable chunked encoding for all requests.</td><td>false</td></tr>
 * <tr><td>{@link #setAccelerateModeEnabled(boolean) accelerateModeEnabled}</td><td>Configures the client to use S3 accelerate endpoint for all requests. A bucket by default cannot be accessed in accelerate mode. If you wish to do so, you need to enable the accelerate configuration for the bucket in advance. (This includes extra costs)</td><td>false</td></tr>
 * <tr><td>{@link #setForceGlobalBucketAccessEnabled(boolean) forceGlobalBucketAccessEnabled}</td><td>Configure whether global bucket access is enabled for this client. When enabled client in a specified region is allowed to create buckets in other regions.</td><td>false</td></tr>
 * <tr><td>{@link #setAction(String) action}</td><td>Action to be performed. Possible action values:
 * <ul>
 * <li>createBucket: create a new bucket inside Amazon S3</li>
 * <li>deleteBucket: delete an existing bucket from S3</li>
 * <li>uploadObject: puts an object inside a S3 bucket</li>
 * <li>downloadObject: gets an object from a S3 bucket for further and safes it in ???</li>
 * <li>copyObject: copies an object from one S3 bucket to another S3 bucket</li>
 * <li>deleteObject: delete an object from inside a S3 bucket</li>
 * </ul></td><td></td></tr>
 * <tr><td>{@link #setBucketName(String) bucketName}</td><td>Set a new or an existing name for S3 bucket depending on your action</td><td></td></tr>
 * <tr><td>{@link #setObjectKey(String) objectKey}</td><td>Set a new or an existing name for your object depending on your action</td><td></td></tr>
 * </table>
 * </p>
 * 
 * <p>
 * <b>Parameters:</b>
 * <table border="1">
 * <tr><th>name</th><th>type</th><th>remarks</th></tr>
 * <tr><td>objectKey</td><td><i>String</i></td><td>When a parameter with name objectKey is present, it is used instead of the objectKey attribute</td></tr>
 * <tr><td>outputFile</td><td><i>Stream</i></td><td>This parameter contains InputStream, it must be present when performing uploadObject action </td></tr>
 * <tr><td>destinationBucketName</td><td><i>String</i></td><td>This parameter specifies the name of destination bucket, it must be present when performing copyObject action</td></tr>
 * <tr><td>destinationObjectKey</td><td><i>String</i></td><td>This parameter specifies the name of the copied object, it must be present when performing copyObject action</td></tr>
 * </table>
 * </p>
 * 
 * <p>
 * This is a list containing configurations that exist in AmazonS3 API and are not used in this sender:
 * <ul>
 * <li>bucket analytics</li>
 * <li>bucket cross origin configuration</li>
 * <li>bucket encryption</li>
 * <li>bucket inventory configuration</li>
 * <li>bucket lifecycle configuration</li>
 * <li>bucket metric configuration</li>
 * <li>bucket policy</li>
 * <li>bucket replication configuration</li>
 * <li>bucket tagging configuration</li>
 * <li>bucket website configuration</li>
 * <li>bucket requester configuration</li>
 * </ul>
 * </p>
 * 
 * @author R. Karajev
 * @author Robert Karajev
 *
 */

public class S3Sender extends SenderWithParametersBase
{
	private String name;

	private AmazonS3ClientBuilder s3ClientBuilder;
	private AmazonS3 s3Client;
	private boolean chunkedEncodingDisabled = false;
	private boolean accelerateModeEnabled = false; // this may involve some extra costs
	private boolean forceGlobalBucketAccessEnabled = false;
	private List<String> regions = getAvailableRegions();
	private String clientRegion = Regions.EU_CENTRAL_1.getName();
	private List<String> actions = Arrays.asList("createBucket", "deleteBucket", "uploadObject", "downloadObject", "copyObject", "deleteObject");
	private String action;
	private String bucketName;
	private String objectKey;

	@Override
	public void configure() throws ConfigurationException
	{
		super.configure();
		
		if(getClientRegion() == null || !regions.contains(getClientRegion()))
			throw new ConfigurationException(getLogPrefix() + " region unknown or is not specified [" + getClientRegion() + "] please use following supported regions: " + regions.toString());
		
		if(getAction() == null || !actions.contains(getAction()))
			throw new ConfigurationException(getLogPrefix()+" action unknown or is not specified [" + getAction() + "} please use following supported actions: " + actions.toString());
		
		if(getBucketName() == null || !com.amazonaws.services.s3.internal.BucketNameUtils.isValidV2BucketName(getBucketName()))
			throw new ConfigurationException(getLogPrefix() + " bucketName not specified or correct bucket naming is not used, visit AWS to see correct bucket naming");

		ParameterList parameterList = getParameterList();
		if(!getAction().equals("createBucket") || !getAction().equals("deleteBucket"))
		{
			if(getObjectKey() == null && parameterList.findParameter("objectKey") == null)
				throw new ConfigurationException(getLogPrefix() + " minimal objectKey requirement not met, please specify objectKey or objectKey parameter");
			
			if(getAction().equals("uploadObject") && parameterList.findParameter("outputFile") == null)
				throw new ConfigurationException(getLogPrefix()+" outputFile parameter requires to be present to perform [" + getAction() + "]");
		
			if(getAction().equals("copyObject") && parameterList.findParameter("destinationBucketName") == null)
				throw new ConfigurationException(getLogPrefix()+" destinationBucketName parameter requires to be present to perform [" + getAction() + "] for copying an object to another bucket");
		}
			
		s3ClientBuilder = AmazonS3ClientBuilder.standard()
											.withChunkedEncodingDisabled(isChunkedEncodingDisabled())
                            				.withAccelerateModeEnabled(isAccelerateModeEnabled())
                            				.withForceGlobalBucketAccessEnabled(isForceGlobalBucketAccessEnabled())
                            				.withRegion(getClientRegion())
                            				.withCredentials(new EnvironmentVariableCredentialsProvider());
	}

	public void open()
	{
		s3Client = s3ClientBuilder.build();
	}

	public void close()
	{
		s3Client.shutdown();
	}

	public String sendMessage(String correlationID, String message, ParameterResolutionContext prc) throws SenderException, TimeOutException
	{
		//fills ParameterValueList pvl with the set parameters from S3Sender
		ParameterValueList pvl = null;
		try
		{
			if (prc != null && paramList != null)
				pvl = prc.getValues(paramList);
		} 
		catch (ParameterException e)
		{
			throw new SenderException(getLogPrefix() + "Sender [" + getName() + "] caught exception evaluating parameters", e);
		}
		
		if(getAction().equals("createBucket"))
			createBucket(getBucketName());
		else if(getAction().equals("deleteBucket"))
			deleteBucket(getBucketName());
		else if(getAction().equals("uploadObject"))
			uploadObject(getBucketName(), getObjectKey(), pvl);
		else if(getAction().equals("downloadObject"))
			downloadObject(getBucketName(), getObjectKey());
		else if(getAction().equals("copyObject"))
			copyObject(pvl);
		else if(getAction().equals("deleteObject"))
			deleteBucket(getBucketName());
		
		return message;
	}
	
	//check for available regions in AWS
	public List<String> getAvailableRegions()
	{
		List<String> availableRegions = new ArrayList<String>(Regions.values().length);
		for (Regions region : Regions.values())
		{
			availableRegions.add(region.getName());
			// System.out.println(region.getName());
		}

		return availableRegions;
	}
	
	//creates a bucket, if it does not already exist
	public void createBucket(String bucketName) throws SenderException
	{
		if(!s3Client.doesBucketExistV2(bucketName))
		{
			CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName);
			s3Client.createBucket(createBucketRequest);
		}
		else
			throw new SenderException(getLogPrefix() + " bucketName already exists, please specify a unique name for your bucket");
	}
	
	//deletes a bucket, if there exists one
	public void deleteBucket(String bucketName) throws SenderException
	{
		if(s3Client.doesBucketExistV2(bucketName))
		{
			DeleteBucketRequest deleteBucketRequest = new DeleteBucketRequest(bucketName);
			s3Client.deleteBucket(deleteBucketRequest);
		}			
		else
			throw new SenderException(getLogPrefix() + " bucket with given name doesn't exists, please specify an existing bucketName");	
	}
	
	//upload an InputStream to S3 Bucket as an object
	public void uploadObject(String bucketName, String objectKey, ParameterValueList pvl) throws SenderException
	{
		if (pvl.getParameterValue("outputFile") != null)
		{
			//parameters for a PutObjectRequest
			InputStream inputStream = (InputStream) pvl.getParameterValue("outputFile").getValue();
			String tempObjectKey = pvl.getParameterValue("objectKey").asStringValue(objectKey);
			ObjectMetadata metadata = new ObjectMetadata();
			//String contentType = pvl.getParameterValue("contentType").asStringValue("application/octet-stream");
			metadata.setContentType("application/octet-stream");
			
			//request construction and object upload
			PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, tempObjectKey, inputStream, metadata);
			s3Client.putObject(putObjectRequest);
			
			System.out.println("Object uploaded into a S3 bucket!");
		}
		else
			throw new SenderException(getLogPrefix() + " possible that outputFile parameter is not found in the ParameterValueList");
	}
	
	//get an object from S3 Bucket and 
	public void downloadObject(String bucketName, String objectKey)
	{
		try
		{
			GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, objectKey);
			S3Object s3Object = s3Client.getObject(getObjectRequest);
			S3ObjectInputStream s3InputStream = s3Object.getObjectContent(); //slaan de data van S3 object in s3InputStream op
			FileOutputStream fileOutputStream = new FileOutputStream(new File("C:\\Users\\Robert\\Desktop\\ObjectContentFromS3Bucket.jpg"));
			byte[] readBuffer = new byte[s3InputStream.available()];
			int readLength = 0;
	        while ((readLength = s3InputStream.read(readBuffer)) != -1) 
	        {
	            fileOutputStream.write(readBuffer, 0, readLength);
	        }
	        System.out.println("Object downloaded.");
	        
	        s3InputStream.close();
	        fileOutputStream.close();
		}
		catch (FileNotFoundException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		System.out.println("Object downloaded or forwarded!");
	}
	
	public void copyObject(ParameterValueList pvl) throws SenderException
	{
		if (pvl.getParameterValue("destinationBucketName") != null && pvl.getParameterValue("destinationObjectKey") != null)
		{
			String destinationBucketName = pvl.getParameterValue("destinationBucketName").getValue().toString();
			String destinationObjectKey = pvl.getParameterValue("destinationObjectKey").getValue().toString();
			CopyObjectRequest copyObjectRequest = new CopyObjectRequest(getBucketName(), getObjectKey(), destinationBucketName, destinationObjectKey);
			if(!s3Client.doesBucketExistV2(destinationBucketName))
				createBucket(destinationBucketName);
			s3Client.copyObject(copyObjectRequest);
			System.out.println("Object copied from one bucket to another");
		}
		else
			throw new SenderException(getLogPrefix() + "possible that destinationBucketName or destinationObjectKey parameters are not found in the ParameterValueList");
	}
	
	public void deleteObject(String bucketName, String objectKey) throws SenderException
	{
		if(s3Client.doesObjectExist(objectKey, objectKey))
		{
			DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, objectKey);
			s3Client.deleteObject(deleteObjectRequest);
		}			
		else
			throw new SenderException(getLogPrefix() + " object with given name doesn't exist, please specify existing bucketKey");	
	}

	
	//getters and setters
	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
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
	
	public String getAction()
	{
		return action;
	}

	public void setAction(String action)
	{
		this.action = action;
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