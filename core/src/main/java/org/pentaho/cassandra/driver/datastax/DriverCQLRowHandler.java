/*******************************************************************************
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.cassandra.driver.datastax;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.NotImplementedException;
import org.pentaho.cassandra.util.CassandraUtils;
import org.pentaho.cassandra.spi.CQLRowHandler;
import org.pentaho.cassandra.spi.ITableMetaData;
import org.pentaho.cassandra.spi.Keyspace;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.StepInterface;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverCQLRowHandler implements CQLRowHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DriverCQLRowHandler.class);

    private final Session session;
    private DriverKeyspace keyspace;
    private ResultSet result;

    private ColumnDefinitions columns;

    private int batchInsertTimeout;
    private int ttlSec;

    private boolean unloggedBatch = true;

    private boolean expandCollection = true;
    private int primaryCollectionOutputIndex = -1;

    public DriverCQLRowHandler(final DriverKeyspace keyspace, final Session session, boolean expandCollection) {
        this.keyspace = keyspace;
        this.session = session;
        this.expandCollection = expandCollection;
    }

    public DriverCQLRowHandler(final DriverKeyspace keyspace) {
        this(keyspace, keyspace.getConnection().getSession(keyspace.getName()), true);
    }

    public boolean supportsCQLVersion(int cqMajorlVersion) {
        return cqMajorlVersion >= 3 && cqMajorlVersion <= 3;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        keyspace.setOptions(options);
        if (options.containsKey(CassandraUtils.BatchOptions.BATCH_TIMEOUT)) {
            batchInsertTimeout = Integer.parseInt(options.get(CassandraUtils.BatchOptions.BATCH_TIMEOUT));
        }
        if (options.containsKey(CassandraUtils.BatchOptions.TTL)) {
            ttlSec = Integer.parseInt(options.get(CassandraUtils.BatchOptions.TTL));
        }
    }

    @Override
    public void setKeyspace(final Keyspace keyspace) {
        this.keyspace = (DriverKeyspace) keyspace;
    }

    @Override
    public void newRowQuery(final StepInterface requestingStep, final String tableName, final String cqlQuery, final String compress,
                            final String consistencyLevel, final LogChannelInterface log) throws Exception {
        result = getSession().execute(cqlQuery);
        columns = result.getColumnDefinitions();
        if (expandCollection) {
            for (int i = 0; i < columns.size(); i++) {
                if (columns.getType(i).isCollection()) {
                    if (primaryCollectionOutputIndex < 0) {
                        primaryCollectionOutputIndex = i;
                    } else if (!keyspace.getTableMetaData(tableName).getValueMetaForColumn(columns.getName(i))
                            .isString()) {
                        throw new KettleException(BaseMessages.getString(DriverCQLRowHandler.class,
                                "DriverCQLRowHandler.Error.CantHandleAdditionalCollectionsThatAreNotOfTypeText"));
                    }
                }
            }
        }
    }

    @Override
    public Object[][] getNextOutputRow(final RowMetaInterface outputRowMeta, final Map<String, Integer> outputFormatMap)
            throws Exception {
        if (result == null || result.isExhausted()) {
            result = null;
            columns = null;
            return null;
        }

        final Row row = result.one();
        Object[][] outputRowData = new Object[1][];
        final Object[] baseOutputRowData = RowDataUtil.allocateRowData(Math.max(outputRowMeta.size(), columns.size()));
        for (int i = 0; i < columns.size(); i++)
            baseOutputRowData[i] = readValue(outputRowMeta.getValueMeta(i), row, i);

        outputRowData[0] = baseOutputRowData;
        if (primaryCollectionOutputIndex > 0) {
            final Collection<?> collection = (Collection<?>) row.getObject(primaryCollectionOutputIndex);
            if (collection != null && !collection.isEmpty()) {
                outputRowData = new Object[collection.size()][];
                int i = 0;
                for (final Object obj : collection) {
                    outputRowData[i] = Arrays.copyOf(baseOutputRowData, baseOutputRowData.length);
                    outputRowData[i++][primaryCollectionOutputIndex] = obj;
                }
            } else {
                outputRowData[0][primaryCollectionOutputIndex] = null;
            }
        }

        return outputRowData;
    }

    public static Object readValue(final ValueMetaInterface meta, final Row row, int i) {
        switch (meta.getType()) {
            case ValueMetaInterface.TYPE_INTEGER:
                return row.getLong(i);
            case ValueMetaInterface.TYPE_NUMBER:
                return row.getDouble(i);
            case ValueMetaInterface.TYPE_BIGNUMBER:
                return row.getDecimal(i);
            case ValueMetaInterface.TYPE_DATE:
                // Check whether this is a CQL Date or Timestamp
                final ColumnDefinitions cdef = row.getColumnDefinitions();
                if (cdef.getType(i).getName() == DataType.Name.DATE) {
                    final LocalDate ld = row.getDate(i);
                    return new java.util.Date(ld.getMillisSinceEpoch());
                } else {
                    return row.getTimestamp(i);
                }
            default:
                return row.getObject(i);
        }
    }

    public static Object readValue(final ValueMetaInterface meta, final Object value) throws KettleValueException {
        switch (meta.getType()) {
            case ValueMetaInterface.TYPE_STRING:
                return meta.getString(value);
            case ValueMetaInterface.TYPE_INTEGER:
                return meta.getInteger(value);
            case ValueMetaInterface.TYPE_NUMBER:
                return meta.getNumber(value);
            case ValueMetaInterface.TYPE_BIGNUMBER:
                return meta.getBigNumber(value);
            case ValueMetaInterface.TYPE_DATE:
                return meta.getDate(value);
            case ValueMetaInterface.TYPE_TIMESTAMP:
                return meta.getDate(value).getTime();
            default:
                return value;
        }
    }

    public void batchInsert(final RowMetaInterface inputMeta, final Iterable<Object[]> rows, final ITableMetaData tableMeta,
                            final String consistencyLevel, boolean insertFieldsNotInMetadata, final LogChannelInterface log) throws Exception {
        final Batch batch = unloggedBatch ? QueryBuilder.unloggedBatch() : QueryBuilder.batch();
        if (!Utils.isEmpty(consistencyLevel)) {
            try {
                batch.setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel));
            } catch (Exception e) {
                log.logError(e.getLocalizedMessage(), e);
            }
        }

        String[] columnNames = getColumnNames(inputMeta);
        final List<Integer> toRemove = new ArrayList<>();
        if (!insertFieldsNotInMetadata) {
            for (int i = 0; i < columnNames.length; i++)
                if (!tableMeta.columnExistsInSchema(columnNames[i]))
                    toRemove.add(i);

            if (toRemove.size() > 0)
                columnNames = copyExcluding(columnNames, new String[columnNames.length - toRemove.size()], toRemove);
        }

        for (final Object[] row : rows) {
            final Object[] r = new Object[columnNames.length];
            for (int i = 0; i < columnNames.length; i++)
                r[i] = readValue(inputMeta.getValueMeta(i), row[i]);

            final Object[] values = toRemove.size() == 0
                    ? Arrays.copyOf(r, columnNames.length)
                    : copyExcluding(r, new Object[columnNames.length], toRemove);
            Insert insert = QueryBuilder.insertInto(keyspace.getName(), tableMeta.getTableName());
            insert = ttlSec > 0 ? insert.using(QueryBuilder.ttl(ttlSec)).values(columnNames, values)
                    : insert.values(columnNames, values);
            batch.add(insert);
        }
        if (batchInsertTimeout > 0) {
            try {
                getSession().executeAsync(batch).getUninterruptibly(batchInsertTimeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                log.logError(BaseMessages.getString(DriverCQLRowHandler.class, "DriverCQLRowHandler.Error.TimeoutReached"));
            }
        } else {
           getSession().execute(batch);
        }
    }

    protected static <T> T[] copyExcluding(final T[] source, final T[] target, final List<Integer> toRemove) {
        int removed = toRemove.size();
        int start = 0, dest = 0, removeCount = 0;
        for (int idx : toRemove) {
            int len = idx - start;
            if (len > 0) {
                System.arraycopy(source, start, target, dest, len);
                dest += len;
            }
            start = idx + 1;
            removeCount++;
            if (removeCount == removed && dest < target.length) // last one
                System.arraycopy(source, start, target, dest, target.length - dest);
        }
        return target;
    }

    private String[] getColumnNames(final RowMetaInterface inputMeta) {
        final String[] columns = new String[inputMeta.size()];
        for (int i = 0; i < inputMeta.size(); i++)
            columns[i] = inputMeta.getValueMeta(i).getName();

        return columns;
    }

    @Override
    public void commitCQLBatch(final StepInterface requestingStep, final StringBuilder batch, final String compress,
                               final String consistencyLevel, final LogChannelInterface log) throws Exception {
        throw new NotImplementedException();
    }

    public void setUnloggedBatch(boolean unloggedBatch) {
        this.unloggedBatch = unloggedBatch;
    }

    public boolean isUnloggedBatch() {
        return unloggedBatch;
    }

    private Session getSession() {
        return session;
    }

}
