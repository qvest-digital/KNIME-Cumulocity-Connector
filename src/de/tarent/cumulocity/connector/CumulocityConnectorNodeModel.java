package de.tarent.cumulocity.connector;

import java.io.File;
import java.util.Optional;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.config.base.ConfigBase;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * <code>NodeModel</code> for the "CumulocityConnectorDeviceRetriever" node.
 *
 * @author tarent solutions GmbH (vlahar)
 */
public class CumulocityConnectorNodeModel extends NodeModel {

	private static final NodeLogger logger = NodeLogger.getLogger(CumulocityConnectorNodeModel.class);

	private static final String DEFAULT_STRING_VAL = "";

	/**
	 * the settings keys which are used to retrieve and store the settings (from the
	 * dialog or from a settings file)
	 */
	private static final String CFGKEY_m_auth = "auth";

	private final SettingsModelAuthentication m_auth = settingsModelAuthentication();

	private final SettingsModelString m_url = settingsModelUrl();
	private final SettingsModelString m_tenant = settingsModelTenant();

	/**
	 * Constructor for the node model. There is no input port and one output port
	 */
	public CumulocityConnectorNodeModel() {
		super(new PortType[0], new PortType[] { CumulocityPortObject.TYPE });
	}

	static SettingsModelString settingsModelUrl() {
		return new SettingsModelString(CotPlatformProvider.CFGKEY_url, DEFAULT_STRING_VAL);
	}

	static SettingsModelString settingsModelTenant() {
		return new SettingsModelString(CotPlatformProvider.CFGKEY_tenant, DEFAULT_STRING_VAL);
	}

	static SettingsModelAuthentication settingsModelAuthentication() {
		return new SettingsModelAuthentication(CFGKEY_m_auth, AuthenticationType.USER_PWD);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * canceling makes no sense here
	 * @throws InvalidSettingsException - if connection settings are incomplete
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws InvalidSettingsException {
		final CumulocityPortObject cotPortObject = new CumulocityPortObject(convertConnectionSettingsToConfig());
		return new PortObject[] { cotPortObject };
	}

	private ConfigBase convertConnectionSettingsToConfig() throws InvalidSettingsException {
		final Optional<String> tenant;
		if (m_tenant.getStringValue().trim().length() == 0) {
			tenant = Optional.empty();
		} else {
			tenant = Optional.of(m_tenant.getStringValue().trim());
		}

		final ConfigBase settings;
		if (m_auth.useCredential()) {
			if (isEmpty(getCredentialsProvider().get(m_auth.getCredential()).getLogin())) {
				logger.error("Credentials " + m_auth.getCredential() + " does not contain any user name!");
				throw new InvalidSettingsException(
						"Credentials " + m_auth.getCredential() + " does not contain any user name!");
			}
			if (isEmpty(getCredentialsProvider().get(m_auth.getCredential()).getPassword())) {
				logger.error("Credentials " + m_auth.getCredential() + " does not contain any user password!");
				throw new InvalidSettingsException(
						"Credentials " + m_auth.getCredential() + " does not contain any user password!");
			}

			settings = CumulocityPortObject.saveConnectionInfoInConfigObject(tenant, m_url.getStringValue(),
					Optional.of(m_auth.getCredential()), Optional.empty(), Optional.empty());
		} else {
			settings = CumulocityPortObject.saveConnectionInfoInConfigObject(tenant, m_url.getStringValue(),
					Optional.empty(), Optional.of(m_auth.getUsername()), Optional.of(m_auth.getPassword()));
		}
		return settings;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
		// nothing to do
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) {
		// no input ports
		return new PortObjectSpec[] {
				// create a dummy connection configuration and the known (fixed) output table
				// spec
				new CumulocityPortObjectSpec(
						CumulocityPortObject.saveConnectionInfoInConfigObject(Optional.of("Tenant"), "Host",
								Optional.of("credential name"), Optional.of("username"), Optional.of("password"))) };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		m_auth.saveSettingsTo(settings);
		m_tenant.saveSettingsTo(settings);
		m_url.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_auth.loadSettingsFrom(settings);
		m_tenant.loadSettingsFrom(settings);
		m_url.loadSettingsFrom(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_auth.validateSettings(settings);
		if (m_auth.useCredential()) {
			if (isEmpty(getCredentialsProvider().get(m_auth.getCredential()).getLogin())) {
				logger.error("Credentials " + m_auth.getCredential() + " does not contain any user name!");
				throw new InvalidSettingsException(
						"Credentials " + m_auth.getCredential() + " does not contain any user name!");
			}
			if (isEmpty(getCredentialsProvider().get(m_auth.getCredential()).getPassword())) {
				logger.error("Credentials " + m_auth.getCredential() + " does not contain any user password!");
				throw new InvalidSettingsException(
						"Credentials " + m_auth.getCredential() + " does not contain any user password!");
			}
		}
		m_tenant.validateSettings(settings);
		m_url.validateSettings(settings);
	}

	private boolean isEmpty(final String aStr) {
		return aStr == null || aStr.trim().length() == 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(final File internDir, final ExecutionMonitor exec) {
		// nothing to do
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(final File internDir, final ExecutionMonitor exec) {
		// nothing to do
	}
}
