package de.tarent.cumulocity.data.measurements;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * implementation of the node factory of the
 * "MeasurementCreator" node.
 *
 * @author tarent solutions GmbH
 */
public class MeasurementCreatorNodeFactory 
        extends NodeFactory<MeasurementCreatorNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public MeasurementCreatorNodeModel createNodeModel() {
        return new MeasurementCreatorNodeModel();
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
    public NodeView<MeasurementCreatorNodeModel> createNodeView(final int viewIndex,
            final MeasurementCreatorNodeModel nodeModel) {
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
        return new MeasurementCreatorNodeDialog();
    }
}

