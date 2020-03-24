package de.tarent.cumulocity.data.events;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * implementation of the node factory of the "CreateEvents" node.
 *
 * @author tarent solutions GmbH
 */
public class CreateEventsNodeFactory extends NodeFactory<CreateEventsNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CreateEventsNodeModel createNodeModel() {
		return new CreateEventsNodeModel();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNrNodeViews() {
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeView<CreateEventsNodeModel> createNodeView(final int viewIndex, final CreateEventsNodeModel nodeModel) {
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDialog() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeDialogPane createNodeDialogPane() {
		return new CreateEventsNodeDialog();
	}

}
