package de.tarent.cumulocity.connector;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.config.base.ConfigBase;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;

import com.telekom.m2m.cot.restsdk.CloudOfThingsPlatform;
import com.telekom.m2m.cot.restsdk.devicecontrol.CotCredentials;

/**
 * creates the API-access object to the cumulocity cloud from the given
 * configuration
 * 
 * note that this class is identical to the one in package
 * de.tarent.cumulocity.getMeasurements (to avoid class loader issues with the cot jar
 * file)
 * 
 * @author tarent solutions GmbH (dwk)
 *
 */
public class CotPlatformProvider {

	static final String CREDENTIAL_NAME = "credential_name";

	static final String CFG_USER_NAME = "username";

	static final String CFGKEY_tenant = "tenant";

	static final String CFGKEY_url = "url";

	/**
	 * creates a new connection whenever it is called
	 * 
	 * @return API object to query the CoT platform
	 */
	public static CloudOfThingsPlatform getCoTPlatform(final CredentialsProvider aCredentialsProvider,
			final ConfigBase aConfig) {
		try {

			final String uName;
			final String pwd;
			if (aConfig.containsKey(CREDENTIAL_NAME)) {
				final ICredentials credentials = aCredentialsProvider.get(aConfig.getString(CREDENTIAL_NAME));
				uName = credentials.getLogin();
				pwd = credentials.getPassword();
			} else {
				uName = aConfig.getString(CFG_USER_NAME);
				pwd = aConfig.getPassword("passwordEncrypted", ";Op5~pK{31AIN^eH~Ab`:Yaikm8CM`8_Dw:1Kl4_WHrvuAXO");
			}

			final CloudOfThingsPlatform platform;
			if (aConfig.containsKey(CotPlatformProvider.CFGKEY_tenant)) {
				platform = new CloudOfThingsPlatform(
						aConfig.getString(CotPlatformProvider.CFGKEY_url),
						new CotCredentials(aConfig.getString(CotPlatformProvider.CFGKEY_tenant),
								uName, pwd));
			} else {
				platform = new CloudOfThingsPlatform(
						aConfig.getString(CotPlatformProvider.CFGKEY_url), uName, pwd);
			}
			return platform;
		} catch (InvalidSettingsException e) {
			final NodeLogger logger = NodeLogger.getLogger(CotPlatformProvider.class);
			logger.error("Error: failed to process stored cumulocity connection info. Will use dummy defaults!", e);
			return new CloudOfThingsPlatform("DB URL", "user name", "some password");
		}
	}
}
