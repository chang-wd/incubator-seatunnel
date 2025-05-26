/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.hdfs.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;

import com.google.auto.service.AutoService;

import java.util.Arrays;

@AutoService(Factory.class)
public class HdfsFileSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.HDFS.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HdfsSourceConfigOptions.FILE_PATH)
                .required(HdfsSourceConfigOptions.DEFAULT_FS)
                .required(FileBaseSourceOptions.FILE_FORMAT_TYPE)
                .conditional(
                        FileBaseSourceOptions.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        FileBaseSourceOptions.FIELD_DELIMITER,
                        FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER)
                .conditional(
                        FileBaseSourceOptions.FILE_FORMAT_TYPE,
                        FileFormat.XML,
                        FileBaseSourceOptions.XML_ROW_TAG,
                        FileBaseSourceOptions.XML_USE_ATTR_FORMAT)
                .conditional(
                        FileBaseSourceOptions.FILE_FORMAT_TYPE,
                        FileFormat.CSV,
                        FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER)
                .conditional(
                        FileBaseSourceOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT,
                                FileFormat.JSON,
                                FileFormat.EXCEL,
                                FileFormat.CSV,
                                FileFormat.XML),
                        ConnectorCommonOptions.SCHEMA)
                .conditional(
                        FileBaseSourceOptions.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT, FileFormat.JSON, FileFormat.CSV, FileFormat.XML),
                        FileBaseSourceOptions.ENCODING)
                .optional(FileBaseSourceOptions.PARSE_PARTITION_FROM_PATH)
                .optional(FileBaseSourceOptions.DATE_FORMAT_LEGACY)
                .optional(FileBaseSourceOptions.DATETIME_FORMAT_LEGACY)
                .optional(FileBaseSourceOptions.TIME_FORMAT_LEGACY)
                .optional(FileBaseSourceOptions.FILE_FILTER_PATTERN)
                .optional(FileBaseSourceOptions.COMPRESS_CODEC)
                .optional(FileBaseSourceOptions.ARCHIVE_COMPRESS_CODEC)
                .optional(FileBaseSourceOptions.NULL_FORMAT)
                .optional(FileBaseSourceOptions.FILENAME_EXTENSION)
                .optional(FileBaseSourceOptions.READ_COLUMNS)
                .optional(FileBaseSourceOptions.HDFS_SITE_PATH)
                .optional(FileBaseSourceOptions.KERBEROS_PRINCIPAL)
                .optional(FileBaseSourceOptions.KERBEROS_KEYTAB_PATH)
                .optional(FileBaseSourceOptions.KRB5_PATH)
                .optional(FileBaseSourceOptions.REMOTE_USER)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return HdfsFileSource.class;
    }
}
