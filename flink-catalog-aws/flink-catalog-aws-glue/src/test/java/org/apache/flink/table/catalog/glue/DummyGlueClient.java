package org.apache.flink.table.catalog.glue;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;

import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableRequest;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableResponse;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GlueEncryptionException;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.InternalServiceException;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.OperationTimeoutException;
import software.amazon.awssdk.services.glue.model.ResourceNotReadyException;
import software.amazon.awssdk.services.glue.model.ResourceNumberLimitExceededException;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;

/** Dummy Glue client for Test. */
public class DummyGlueClient implements GlueClient {

    @Getter @Setter public String tableType;

    @Getter @Setter public boolean isTableExists;

    @Getter @Setter public ObjectPath tablePath;

    @Getter @Setter public String databaseName;

    @Getter @Setter public String warehousePath;

    @Getter @Setter public String description;

    @Getter @Setter public boolean isPartitionedTable;

    @Override
    public String serviceName() {
        return "Glue";
    }

    @Override
    public void close() {}

    @Override
    public CreateDatabaseResponse createDatabase(CreateDatabaseRequest createDatabaseRequest)
            throws InvalidInputException, AlreadyExistsException,
                    ResourceNumberLimitExceededException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException,
                    ConcurrentModificationException, AwsServiceException, SdkClientException,
                    GlueException {
        return GlueCatalogTestUtils.dummyCreateDatabaseResponse();
    }

    @Override
    public UpdateDatabaseResponse updateDatabase(UpdateDatabaseRequest updateDatabaseRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException,
                    ConcurrentModificationException, AwsServiceException, SdkClientException,
                    GlueException {
        return GlueCatalogTestUtils.dummyUpdateDatabaseResponse();
    }

    @Override
    public GetDatabaseResponse getDatabase(GetDatabaseRequest getDatabaseRequest)
            throws InvalidInputException, EntityNotFoundException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException, AwsServiceException,
                    SdkClientException, GlueException {
        return GlueCatalogTestUtils.dummyGetDatabaseResponse(
                databaseName, warehousePath, description);
    }

    @Override
    public GetDatabasesResponse getDatabases(GetDatabasesRequest getDatabasesRequest)
            throws InvalidInputException, InternalServiceException, OperationTimeoutException,
                    GlueEncryptionException, AwsServiceException, SdkClientException,
                    GlueException {
        return GlueCatalogTestUtils.dummyGetDatabasesResponse();
    }

    @Override
    public CreateTableResponse createTable(CreateTableRequest createTableRequest)
            throws AlreadyExistsException, InvalidInputException, EntityNotFoundException,
                    ResourceNumberLimitExceededException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException,
                    ConcurrentModificationException, ResourceNotReadyException, AwsServiceException,
                    SdkClientException, GlueException {
        return GlueCatalogTestUtils.dummyCreateTableResponse();
    }

    @Override
    public GetTableResponse getTable(GetTableRequest getTableRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException, ResourceNotReadyException,
                    AwsServiceException, SdkClientException, GlueException {
        if (!isTableExists) {
            return null;
        }

        if (isPartitionedTable) {
            return GlueCatalogTestUtils.dummyGetTableResponseWithPartitionKeys(
                    tablePath, tableType);
        }
        return GlueCatalogTestUtils.dummyTableResponseWithTwoColumns(tablePath, tableType);
    }

    @Override
    public GetTablesResponse getTables(GetTablesRequest getTablesRequest)
            throws EntityNotFoundException, InvalidInputException, OperationTimeoutException,
                    InternalServiceException, GlueEncryptionException, AwsServiceException,
                    SdkClientException, GlueException {

        if (!isTableExists) {
            return GlueCatalogTestUtils.dummyGetEmptyTableList();
        }
        if (tableType.equals(CatalogBaseTable.TableKind.TABLE.name())) {
            return GlueCatalogTestUtils.dummyGetTablesResponseContainingOnlyTable(databaseName);
        } else if (tableType.equals(CatalogBaseTable.TableKind.VIEW.name())) {
            return GlueCatalogTestUtils.dummyGetTablesResponseContainingOnlyView(databaseName);
        } else {
            return GlueCatalogTestUtils.dummyGetTablesResponseWithDifferentTableTypes(databaseName);
        }
    }

    @Override
    public CreatePartitionResponse createPartition(CreatePartitionRequest createPartitionRequest)
            throws InvalidInputException, AlreadyExistsException,
                    ResourceNumberLimitExceededException, InternalServiceException,
                    EntityNotFoundException, OperationTimeoutException, GlueEncryptionException,
                    AwsServiceException, SdkClientException, GlueException {
        return GlueCatalogTestUtils.dummyCreatePartitionResponse();
    }

    @Override
    public GetPartitionResponse getPartition(GetPartitionRequest getPartitionRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException, AwsServiceException,
                    SdkClientException, GlueException {
        return GlueCatalogTestUtils.dummyGetPartitionResponse();
    }

    @Override
    public DeleteDatabaseResponse deleteDatabase(DeleteDatabaseRequest deleteDatabaseRequest)
            throws EntityNotFoundException, InvalidInputException, InternalServiceException,
                    OperationTimeoutException, ConcurrentModificationException, AwsServiceException,
                    SdkClientException, GlueException {
        return GlueCatalogTestUtils.dummyDeleteDatabaseResponse(databaseName);
    }

    @Override
    public BatchDeleteTableResponse batchDeleteTable(
            BatchDeleteTableRequest batchDeleteTableRequest)
            throws InvalidInputException, EntityNotFoundException, InternalServiceException,
                    OperationTimeoutException, GlueEncryptionException, ResourceNotReadyException,
                    AwsServiceException, SdkClientException, GlueException {
        return GlueCatalogTestUtils.dummyBatchDeleteTable();
    }
}
