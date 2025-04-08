/*
 * Copyright (c) 2025, Beijing ShinData Technology Co., Ltd. All rights reserved.
 * ShinData PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package io.debezium.connector.db2;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.*;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.util.Loggings;

/**
 * @author licd
 * @since 2025/4/8
 */
public class Db2TableSchemaBuilder extends TableSchemaBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(Db2TableSchemaBuilder.class);

    public Db2TableSchemaBuilder(ValueConverterProvider valueConverterProvider,
                                 SchemaNameAdjuster schemaNameAdjuster,
                                 CustomConverterRegistry customConverterRegistry,
                                 Schema sourceInfoSchema,
                                 FieldNameSelector.FieldNamer<Column> fieldNamer,
                                 boolean multiPartitionMode) {
        super(valueConverterProvider, schemaNameAdjuster, customConverterRegistry, sourceInfoSchema,
                fieldNamer, multiPartitionMode);
    }

    public Db2TableSchemaBuilder(ValueConverterProvider valueConverterProvider,
                                 SchemaNameAdjuster schemaNameAdjuster,
                                 CustomConverterRegistry customConverterRegistry,
                                 Schema sourceInfoSchema,
                                 Schema transactionSchema, FieldNameSelector.FieldNamer<Column> fieldNamer,
                                 boolean multiPartitionMode) {
        super(valueConverterProvider, schemaNameAdjuster, customConverterRegistry, sourceInfoSchema,
                transactionSchema, fieldNamer, multiPartitionMode);
    }

    public Db2TableSchemaBuilder(ValueConverterProvider valueConverterProvider,
                                 DefaultValueConverter defaultValueConverter,
                                 SchemaNameAdjuster schemaNameAdjuster,
                                 CustomConverterRegistry customConverterRegistry,
                                 Schema sourceInfoSchema,
                                 FieldNameSelector.FieldNamer<Column> fieldNamer,
                                 boolean multiPartitionMode) {
        super(valueConverterProvider, defaultValueConverter, schemaNameAdjuster, customConverterRegistry,
                sourceInfoSchema, fieldNamer, multiPartitionMode);
    }

    public Db2TableSchemaBuilder(ValueConverterProvider valueConverterProvider,
                                 DefaultValueConverter defaultValueConverter,
                                 SchemaNameAdjuster schemaNameAdjuster,
                                 CustomConverterRegistry customConverterRegistry,
                                 Schema sourceInfoSchema,
                                 FieldNameSelector.FieldNamer<Column> fieldNamer,
                                 boolean multiPartitionMode,
                                 CommonConnectorConfig.EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode) {
        super(valueConverterProvider, defaultValueConverter, schemaNameAdjuster, customConverterRegistry,
                sourceInfoSchema, fieldNamer, multiPartitionMode, eventConvertingFailureHandlingMode);
    }

    public Db2TableSchemaBuilder(ValueConverterProvider valueConverterProvider,
                                 DefaultValueConverter defaultValueConverter,
                                 SchemaNameAdjuster schemaNameAdjuster,
                                 CustomConverterRegistry customConverterRegistry,
                                 Schema sourceInfoSchema, Schema transactionSchema,
                                 FieldNameSelector.FieldNamer<Column> fieldNamer,
                                 boolean multiPartitionMode,
                                 CommonConnectorConfig.EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode) {
        super(valueConverterProvider, defaultValueConverter, schemaNameAdjuster, customConverterRegistry,
                sourceInfoSchema, transactionSchema, fieldNamer, multiPartitionMode, eventConvertingFailureHandlingMode);
    }

    protected StructGenerator createValueGenerator(Schema schema, TableId tableId, List<Column> columns,
                                                   Tables.ColumnNameFilter filter, ColumnMappers mappers) {
        if (schema != null) {
            List<Column> columnsThatShouldBeAdded = columns.stream()
                    .filter(column -> filter == null || filter.matches(tableId.catalog(), tableId.schema(), tableId.table(), column.name()))
                    .collect(Collectors.toList());
            int[] recordIndexes = indexesForColumns(columnsThatShouldBeAdded);
            Field[] fields = fieldsForColumns(schema, columnsThatShouldBeAdded);
            int numFields = recordIndexes.length;
            ValueConverter[] converters = convertersForColumns(schema, tableId, columnsThatShouldBeAdded, mappers);

            return (row) -> {
                boolean useXMode = row.length / numFields == 2;
                Struct result = new Struct(schema);
                for (int i = 0; i != numFields; ++i) {
                    validateIncomingRowToInternalMetadata(recordIndexes, fields, converters, row, i);
                    if (fields[i] == null) {
                        continue;
                    }
                    Object value = row[useXMode ? recordIndexes[i] * 2 : recordIndexes[i]];

                    ValueConverter converter = converters[i];

                    if (converter != null) {
                        LOGGER.trace("converter for value object: *** {} ***", converter);
                        try {
                            value = converter.convert(value);
                            result.put(fields[i], value);
                        }
                        catch (final Exception e) {
                            Column col = columnsThatShouldBeAdded.get(i);
                            String message = "Failed to properly convert data value for '{}.{}' of type {}";
                            Loggings.logErrorAndTraceRecord(LOGGER, row,
                                    message, tableId, col.name(), col.typeName(), e);
                        }
                    }
                    else {
                        LOGGER.trace("converter is null...");
                    }
                }
                return result;
            };
        }
        return null;
    }

    private void validateIncomingRowToInternalMetadata(int[] recordIndexes, Field[] fields, ValueConverter[] converters,
                                                       Object[] row, int position) {
        if (position >= converters.length) {
            LOGGER.error("Error requesting a converter, converters: {}, requested index: {}", converters.length, position);
            throw new ConnectException(
                    "Column indexing array is larger than number of converters, internal schema representation is probably out of sync with real database schema");
        }
        if (position >= fields.length) {
            LOGGER.error("Error requesting a field, fields: {}, requested index: {}", fields.length, position);
            throw new ConnectException("Too few schema fields, internal schema representation is probably out of sync with real database schema");
        }
        if (recordIndexes[position] >= row.length) {
            LOGGER.error("Error requesting a row value, row: {}, requested index: {} at position {}", row.length, recordIndexes[position], position);
            throw new ConnectException("Data row is smaller than a column index, internal schema representation is probably out of sync with real database schema");
        }
    }

}
