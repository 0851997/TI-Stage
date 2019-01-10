package nl.nn.adapterframework.senders;

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import nl.nn.adapterframework.configuration.ConfigurationException;
import nl.nn.adapterframework.core.IPipeLineSession;
import nl.nn.adapterframework.core.SenderException;
import nl.nn.adapterframework.parameters.ParameterResolutionContext;

public class AmazonS3SenderTest
{
	protected Log log = LogFactory.getLog(this.getClass());
	protected AmazonS3Sender sender;
	
	@Rule
	public ExpectedException exception = ExpectedException.none();
	
	@Mock
	public IPipeLineSession session;
	
	public AmazonS3Sender createAmazonS3Sender()
	{
		return new AmazonS3Sender();
	}

	@Before
	public void setup()
	{
		sender = createAmazonS3Sender();
	}
	
	@Test
	public void wrongClientRegion()  throws ConfigurationException
	{
		exception.expect(ConfigurationException.class);
		
		sender.setClientRegion("Unvalid client region");
		sender.configure();
	}
	
	@Test
	public void wrongBucketName() throws ConfigurationException
	{
		exception.expect(ConfigurationException.class);
		
		sender.setBucketName("Unvalid bucket name");
		sender.configure();
	}
	
	@Test
	public void wrongAction() throws ConfigurationException
	{
		exception.expect(ConfigurationException.class);
		
		sender.setBucketName("bucket-created-for-tests");
		sender.setActions("Not createBucket, deleteBucket, upload, download, copy or delete action.");
		sender.configure();
	}
	
	@Test
	public void wrongBucketRegionForGlobalBucketAccess() throws ConfigurationException
	{
		exception.expect(ConfigurationException.class);
		
		sender.setBucketName("bucket-created-for-tests");
		sender.setForceGlobalBucketAccessEnabled(true);
		sender.setActions("createBucket");
		sender.setBucketRegion("Unvalid bucket region");
		sender.configure();
	}
	
	@Test
	public void missingFileParameter() throws ConfigurationException
	{
		exception.expect(ConfigurationException.class);
		
		sender.setBucketName("bucket-created-for-tests");
		sender.setActions("upload");
		ParameterResolutionContext prc = new ParameterResolutionContext("random", session);
		
		sender.configure();
	}

	@Test
	public void createBucketTrue() throws SenderException
	{
		String input = "bucket-created-for-tests";
		sender.setBucketName(input);
		sender.open();
		String output = sender.createBucket(sender.getBucketName(), true);
		assertEquals(input, output);
	}
	
	@Test
	public void createBucketFalse() throws SenderException
	{
		exception.expect(SenderException.class);
		
		String input = "bucket-created-for-tests";
		sender.setBucketName(input);
		sender.open();
		sender.createBucket(sender.getBucketName(), true);
	}
	
	
	
//	@Before
//	public void setup() throws ConfigurationException {
//		pipe = createPipe();
//		pipe.registerForward(new PipeForward("success",null));
//		pipe.setName(pipe.getClass().getSimpleName()+" under test");
//		pipeline = new PipeLine();
//		pipeline.addPipe(pipe);
//		adapter = new Adapter();
//		adapter.registerPipeLine(pipeline);
//	}

	
//	@Test
//	public void notConfigured() throws ConfigurationException {
//		pipe = createPipe();
//		exception.expect(ConfigurationException.class);
//		pipe.configure();
//	}
//
//	@Test
//	public void basicNoAdditionalConfig() throws ConfigurationException {
//		adapter.configure();
//	}

	
}
