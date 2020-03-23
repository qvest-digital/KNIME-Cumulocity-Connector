package de.tarent.cumulocity.connector;

import java.util.Optional;

import javax.swing.JComponent;

import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.config.base.ConfigBase;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;

/**
 * port object implementation for cumulocity database connections
 * @author tarent solutions GmbH (dwk)
 *
 */
public class CumulocityPortObject implements PortObject {

	public static final PortType TYPE = PortTypeRegistry.getInstance().getPortType(CumulocityPortObject.class);

	private static final String NODE_CONFIG_PARAM_NAME = "C8yNodeSettings";

	private final CumulocityPortObjectSpec m_spec;

	public CumulocityPortObject(final ConfigBase aConfig) {
		this(new CumulocityPortObjectSpec(aConfig));
	}

	private CumulocityPortObject(final CumulocityPortObjectSpec aSpec) {
		m_spec = aSpec;
	}
	
	/**
     * Serializer used to save {@link CumulocityPortObject}s.
     *
     * @noreference This class is not intended to be referenced by clients.
     * @since 3.0
     */
    public static final class Serializer extends PortObjectSerializer<CumulocityPortObject> {
        /**
         * {@inheritDoc}
         */
        @Override
        public void savePortObject(final CumulocityPortObject portObject,
            final PortObjectZipOutputStream out, final ExecutionMonitor exec) {
            // nothing to save
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CumulocityPortObject loadPortObject(final PortObjectZipInputStream in,
            final PortObjectSpec spec, final ExecutionMonitor exec) {
            return new CumulocityPortObject((CumulocityPortObjectSpec)spec);
        }
    }

	@Override
	public String getSummary() {
		return m_spec.getSummary();
	}

	@Override
	public CumulocityPortObjectSpec getSpec() {
		return m_spec;
	}

	public ConfigBase getConfig() {
		return m_spec.getConfig();
	}

	@Override
	public JComponent[] getViews() {
		return m_spec.getConnectionView();
	}

	static ConfigBase saveConnectionInfoInConfigObject(final Optional<String> aTenant, final String aUrl,
			final Optional<String> aCredentialName, final Optional<String> aUsername,
			final Optional<String> aPassword) {
		final ConfigBase settings = new NodeSettings(NODE_CONFIG_PARAM_NAME);
		if (aTenant.isPresent()) {
			settings.addString(CotPlatformProvider.CFGKEY_tenant, aTenant.get());
		}

		settings.addString(CotPlatformProvider.CFGKEY_url, aUrl);

		if (aCredentialName.isPresent()) {
			settings.addString(CotPlatformProvider.CREDENTIAL_NAME, aCredentialName.get());
		} else {
			settings.addString(CotPlatformProvider.CFG_USER_NAME, aUsername.get());
			// always save the password encrypted...
			settings.addPassword("passwordEncrypted", ";Op5~pK{31AIN^eH~Ab`:Yaikm8CM`8_Dw:1Kl4_WHrvuAXO",
					aPassword.get());
			/// ... and set the password to null to indicate for the loadConnection method
			/// that the password is saved encrypted (introduced with KNIME 3.4)
			settings.addString("password", null);
		}
		return settings;
	}
}
