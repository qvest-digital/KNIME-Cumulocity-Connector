package de.tarent.cumulocity.data;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Optional;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.container.CloseableRowIterator;

import com.telekom.m2m.cot.restsdk.util.Filter;
import com.telekom.m2m.cot.restsdk.util.Filter.FilterBuilder;
import com.telekom.m2m.cot.restsdk.util.FilterBy;

/**
 * Implementation of an iterator that either iterates over the rows of a table and creates selection
 * filters from the values of one column of that table or that once returns a single empty Optional
 * 
 * @author tarent solutions GmbH
 *
 */
public final class IdIterator implements Iterator<Optional<FilterBuilder>>, Closeable {
	private final CloseableRowIterator m_rowIterator;
	private final int m_colIx;
	private Optional<FilterBuilder> m_emptyFilter = null;

	public IdIterator() {
		m_rowIterator = null;
		m_colIx = -1;
		m_emptyFilter = Optional.empty();
	}

	public IdIterator(final CloseableRowIterator rowIterator, final int colIx) {
		m_rowIterator = rowIterator;
		m_colIx = colIx;
	}

	@Override
	public boolean hasNext() {
		if (m_rowIterator != null) {
			return m_rowIterator.hasNext();
		} else {
			return m_emptyFilter != null;
		}
	}

	@Override
	public Optional<FilterBuilder> next() {
		if (m_rowIterator != null) {
			final DataRow row = m_rowIterator.next();
			final DataCell id_value = row.getCell(m_colIx);
			return Optional.of(Filter.build().setFilter(FilterBy.BYSOURCE, id_value.toString()));
		} else {
			final Optional<FilterBuilder> tmp = m_emptyFilter;
			m_emptyFilter = null;
			return tmp;
		}
	}

	@Override
	public void close() {
		if (m_rowIterator != null) {
			m_rowIterator.close();
		}
	}
}
