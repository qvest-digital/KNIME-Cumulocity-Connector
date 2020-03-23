package de.tarent.cumulocity.connector;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.io.IOException;
import java.util.zip.ZipEntry;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextPane;

import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.config.base.ConfigBase;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;

/**
 * storage container for cumulocity connection parameters
 * 
 * @author tarent solutions GmbH (dwk)
 *
 */
public class CumulocityPortObjectSpec implements PortObjectSpec {

	private final ConfigBase m_config;

	public CumulocityPortObjectSpec(final ConfigBase aConfig) {
		m_config = aConfig;
	}

	@Override
	public JComponent[] getViews() {
		return getConnectionView();
	}

	JComponent[] getConnectionView() {
		return new JComponent[] { new ConnectionView() };
	}

	String getSummary() {
		final StringBuffer buf = new StringBuffer();
		if (m_config.containsKey(CotPlatformProvider.CFGKEY_tenant)) {
			buf.append("Tenant:\n");
			try {
				buf.append(m_config.getString(CotPlatformProvider.CFGKEY_tenant));
			} catch (InvalidSettingsException e) {
				// should not be possible, we checked above that the key exists
				e.printStackTrace();
			}
			buf.append("\n");
		}
		buf.append("Database URL:\n");
		final String databaseURL = m_config.getString(CotPlatformProvider.CFGKEY_url, "");
		buf.append(databaseURL);
		buf.append("\n");
		boolean useCredential = m_config.containsKey(CotPlatformProvider.CREDENTIAL_NAME);
		if (useCredential) {
			String credName = m_config.getString(CotPlatformProvider.CREDENTIAL_NAME, "");
			buf.append("Credential Name:\n");
			buf.append(credName).append("\n");
		} else {
			buf.append("User Name:\n");
			final String user;
			if (m_config.containsKey(CotPlatformProvider.CFG_USER_NAME)) {
				user = m_config.getString(CotPlatformProvider.CFG_USER_NAME, "");
			} else {
				user = m_config.getString("user", "");
			}
			buf.append(user);
		}
		buf.append("\n");
		return buf.toString();
	}

	final class ConnectionView extends JPanel {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * @param sett the {@link ModelContentRO} with the settings
		 */
		ConnectionView() {
			super(new GridBagLayout());
			super.setName("Cumulocity Connection Info");
			StringBuilder buf = new StringBuilder("<html><body>");
			if (m_config.containsKey(CotPlatformProvider.CFGKEY_tenant)) {
				buf.append("<strong>Tenant:</strong>&nbsp;&nbsp;");
				try {
					buf.append("<tt>")
							.append(m_config.getString(CotPlatformProvider.CFGKEY_tenant))
							.append("</tt>");
				} catch (InvalidSettingsException e) {
					// should not be possible, we checked above that the key exists
					e.printStackTrace();
				}
				buf.append("<br/><br/>");
			}
			buf.append("<strong>Database URL:</strong><br/>");
			final String databaseURL = m_config.getString(CotPlatformProvider.CFGKEY_url, "");
			buf.append("<tt>").append(databaseURL).append("</tt>");
			buf.append("<br/><br/>");
			boolean useCredential = m_config.containsKey(CotPlatformProvider.CREDENTIAL_NAME);
			if (useCredential) {
				String credName = m_config.getString(CotPlatformProvider.CREDENTIAL_NAME, "");
				buf.append("<strong>Credential Name:</strong>&nbsp;&nbsp;");
				buf.append("<tt>").append(credName).append("</tt>");
			} else {
				buf.append("<strong>User Name:</strong>&nbsp;&nbsp;");
				final String user;
				if (m_config.containsKey(CotPlatformProvider.CFG_USER_NAME)) {
					user = m_config.getString(CotPlatformProvider.CFG_USER_NAME, "");
				} else {
					user = m_config.getString("user", "");
				}
				buf.append("<tt>").append(user).append("</tt>");
			}
			buf.append("</body></html>");
			final JTextPane textArea = new JTextPane();
			textArea.setContentType("text/html");
			textArea.setEditable(false);
			textArea.setText(buf.toString());
			textArea.setCaretPosition(0);
			final JScrollPane jsp = new JScrollPane(textArea);
			jsp.setPreferredSize(new Dimension(300, 300));
			final GridBagConstraints c = new GridBagConstraints();
			c.gridx = 0;
			c.gridy = 0;
			c.anchor = GridBagConstraints.CENTER;
			c.fill = GridBagConstraints.BOTH;
			c.weightx = 1;
			c.weighty = 1;
			super.add(jsp, c);
		}
	}

	ConfigBase getConfig() {
		return m_config;
	}

	/**
	 * A serializer for {@link CumulocityPortObject}s.
	 *
	 * @author dwk (tarent solutions GmbH)
	 */
	public static final class Serializer extends PortObjectSpecSerializer<CumulocityPortObjectSpec> {

		private static final String KEY_DATABASE_CONNECTION = "c8y_connection.zip";

		@Override
		public CumulocityPortObjectSpec loadPortObjectSpec(final PortObjectSpecZipInputStream in) throws IOException {
			ConfigBase modelContent = loadModelContent(in);
			return new CumulocityPortObjectSpec(modelContent);
		}

		@Override
		public void savePortObjectSpec(final CumulocityPortObjectSpec portObjectSpec,
				final PortObjectSpecZipOutputStream out) throws IOException {
			saveModelContent(out, portObjectSpec);
		}

		/**
		 * Reads the model content from the input stream.
		 * 
		 * @param in an input stream
		 * @return the model content containing the spec information
		 * @throws IOException if an I/O error occurs
		 */
		protected static ConfigBase loadModelContent(final PortObjectSpecZipInputStream in) throws IOException {
			ZipEntry ze = in.getNextEntry();
			if (!ze.getName().equals(KEY_DATABASE_CONNECTION)) {
				throw new IOException("Key \"" + ze.getName() + "\" does not " + " match expected zip entry name \""
						+ KEY_DATABASE_CONNECTION + "\".");
			}
			return (NodeSettings) NodeSettings.loadFromXML(new NonClosableInputStream.Zip(in));
		}

		/**
		 * Saves the given spec object into the output stream.
		 * 
		 * @param os             an output stream
		 * @param portObjectSpec the port spec
		 * @throws IOException if an I/O error occurs
		 */
		protected static void saveModelContent(final PortObjectSpecZipOutputStream os,
				final CumulocityPortObjectSpec portObjectSpec) throws IOException {
			os.putNextEntry(new ZipEntry(KEY_DATABASE_CONNECTION));
			portObjectSpec.getConfig().saveToXML(new NonClosableOutputStream.Zip(os));
		}
	}
}
