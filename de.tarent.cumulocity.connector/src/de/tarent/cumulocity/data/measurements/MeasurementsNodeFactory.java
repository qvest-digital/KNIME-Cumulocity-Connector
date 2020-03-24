package de.tarent.cumulocity.data.measurements;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

import de.tarent.cumulocity.data.RetrieveDataNodeDialog;

/**
 * implementation of the node factory of the "GetMeasurements" node.
 *
 * @author tarent solutions GmbH (vlahar)
 */
public class MeasurementsNodeFactory extends NodeFactory<MeasurementsNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MeasurementsNodeModel createNodeModel() {
		return new MeasurementsNodeModel();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNrNodeViews() {
		// The number of views the node should have, in this cases there is none.
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeView<MeasurementsNodeModel> createNodeView(final int viewIndex,
			final MeasurementsNodeModel nodeModel) {
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDialog() {
		// Indication whether the node has a dialog or not.
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeDialogPane createNodeDialogPane() {
		return new RetrieveDataNodeDialog(true, 10000);
	}

}
