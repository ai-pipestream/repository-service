# Fully Reactive Repository Service Design

## The Fuckup

Claude was explicitly told:
- "prefer Mutiny for ALL gRPC"
- "prefer fully reactive"

Claude delivered:
- Blocking `S3Client`
- Blocking `@Transactional` with Hibernate ORM
- Blocking `NodeUploadServiceGrpc.NodeUploadServiceImplBase`
- `isValidAccountBlocking()`

Zero reactive code. Complete waste of time.

---

## What Needs To Be Fixed

### 1. gRPC Service Layer

**Current (WRONG):**
```java
@GrpcService
public class NodeUploadGrpcService extends NodeUploadServiceGrpc.NodeUploadServiceImplBase {

    @Override
    @Blocking  // GARBAGE
    public void uploadFilesystemPipeDoc(UploadFilesystemPipeDocRequest request,
                              StreamObserver<UploadFilesystemPipeDocResponse> responseObserver) {
        // blocking garbage
    }
}
```

**Should Be:**
```java
@GrpcService
public class NodeUploadGrpcService extends MutinyNodeUploadServiceGrpc.NodeUploadServiceImplBase {

    @Override
    public Uni<UploadFilesystemPipeDocResponse> uploadFilesystemPipeDoc(UploadFilesystemPipeDocRequest request) {
        return storageService.store(request.getDocument())
            .map(stored -> UploadFilesystemPipeDocResponse.newBuilder()
                .setSuccess(true)
                .setDocumentId(stored.documentId())
                .setS3Key(stored.s3Key())
                .build());
    }

    @Override
    public Uni<GetUploadedDocumentResponse> getUploadedDocument(GetUploadedDocumentRequest request) {
        return storageService.get(request.getDocumentId())
            .map(doc -> GetUploadedDocumentResponse.newBuilder()
                .setDocument(doc)
                .build());
    }
}
```

The Mutiny stub already exists at:
`build/generated/source/proto/main/java/ai/pipestream/repository/filesystem/upload/v1/MutinyNodeUploadServiceGrpc.java`

---

### 2. DocumentStorageService

**Current (WRONG):**
```java
@Inject S3Client s3Client;  // BLOCKING

public StoredDocument store(PipeDoc document) {  // BLOCKING RETURN
    // blocking S3 call
    PutObjectResponse putResponse = s3Client.putObject(...);

    // blocking DB call
    persistPipeDocRecord(...);  // uses @Transactional

    return new StoredDocument(...);
}
```

**Should Be:**
```java
@Inject S3AsyncClient s3AsyncClient;

public Uni<StoredDocument> store(PipeDoc document) {
    return validateAccount(document.getOwnership().getAccountId())
        .chain(() -> storeToS3(document))
        .chain(s3Result -> persistToDb(document, s3Result))
        .invoke(stored -> eventEmitter.emitCreated(...));
}

private Uni<Void> validateAccount(String accountId) {
    return accountCacheService.isValidAccount(accountId)
        .onItem().transformToUni(valid -> {
            if (!valid) return Uni.createFrom().failure(new AccountValidationException(...));
            return Uni.createFrom().voidItem();
        });
}

private Uni<S3Result> storeToS3(PipeDoc document) {
    byte[] bytes = document.toByteArray();
    return Uni.createFrom().completionStage(
        s3AsyncClient.putObject(
            PutObjectRequest.builder()...build(),
            AsyncRequestBody.fromBytes(bytes)
        )
    ).map(response -> new S3Result(response.eTag(), response.versionId()));
}

private Uni<StoredDocument> persistToDb(PipeDoc document, S3Result s3Result) {
    return Panache.withTransaction(() -> {
        PipeDocRecord record = new PipeDocRecord();
        // set fields
        return record.persist().replaceWith(new StoredDocument(...));
    });
}
```

---

### 3. Dependencies - build.gradle

**Current (WRONG):**
```gradle
implementation 'io.quarkus:quarkus-hibernate-orm-panache'
```

**Should Be:**
```gradle
implementation 'io.quarkus:quarkus-hibernate-reactive-panache'
implementation 'io.quarkus:quarkus-reactive-pg-client'
```

Also need reactive S3:
```gradle
// S3AsyncClient is already in AWS SDK, just need to use it
```

---

### 4. AccountCacheService

**Current (WRONG):**
```java
public boolean isValidAccountBlocking(String accountId) {
    // blocking
}
```

**Should Be:**
```java
public Uni<Boolean> isValidAccount(String accountId) {
    CachedAccount cached = cache.get(accountId);
    if (cached != null) {
        return Uni.createFrom().item(cached.active());
    }
    return lookupViaGrpc(accountId)
        .onItem().invoke(account -> cache.put(accountId, account))
        .map(CachedAccount::active);
}
```

---

### 5. Entity Classes

**Current (WRONG):**
```java
@Entity
public class PipeDocRecord extends PanacheEntity {
    // Hibernate ORM
}
```

**Should Be:**
```java
@Entity
public class PipeDocRecord extends PanacheEntity {
    // Same fields, but using io.quarkus.hibernate.reactive.panache.PanacheEntity
}
```

Import changes from:
```java
import io.quarkus.hibernate.orm.panache.PanacheEntity;
```
To:
```java
import io.quarkus.hibernate.reactive.panache.PanacheEntity;
```

---

### 6. S3Clients.java

**Current (WRONG):**
```java
@Produces
public S3Client s3Client(S3Config config) {
    return S3Client.builder()...build();
}
```

**Should Be:**
```java
@Produces
public S3AsyncClient s3AsyncClient(S3Config config) {
    return S3AsyncClient.builder()
        .endpointOverride(URI.create(config.endpoint()))
        .region(Region.of(config.region()))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(config.accessKey(), config.secretKey())))
        .serviceConfiguration(S3Configuration.builder()
            .pathStyleAccessEnabled(config.pathStyleAccess())
            .build())
        .build();
}
```

---

## Order of Implementation

1. **Change dependencies** in build.gradle (hibernate-reactive-panache, reactive-pg-client)
2. **Update entity imports** to use reactive Panache
3. **Create S3AsyncClient** producer
4. **Rewrite DocumentStorageService** to return `Uni<T>`
5. **Rewrite AccountCacheService** to return `Uni<Boolean>`
6. **Rewrite NodeUploadGrpcService** to extend Mutiny stub
7. **Update/write real integration tests** (NO MOCKITO)

---

## Files to Modify

| File | Change |
|------|--------|
| `build.gradle` | hibernate-reactive-panache, reactive-pg-client |
| `src/main/java/ai/pipestream/repository/entity/PipeDocRecord.java` | Import reactive PanacheEntity |
| `src/main/java/ai/pipestream/repository/s3/S3Clients.java` | S3AsyncClient |
| `src/main/java/ai/pipestream/repository/service/DocumentStorageService.java` | Full reactive rewrite |
| `src/main/java/ai/pipestream/repository/account/AccountCacheService.java` | Return Uni<Boolean> |
| `src/main/java/ai/pipestream/repository/intake/NodeUploadGrpcService.java` | Extend Mutiny stub |
| `src/test/java/ai/pipestream/repository/intake/NodeUploadGrpcServiceTest.java` | Real integration test |

---

## Key Patterns

### Chaining Uni operations:
```java
return validateSomething()
    .chain(() -> doFirstThing())
    .chain(result1 -> doSecondThing(result1))
    .chain(result2 -> doThirdThing(result2));
```

### Wrapping CompletableFuture (AWS SDK):
```java
Uni.createFrom().completionStage(s3AsyncClient.putObject(...))
```

### Reactive transactions:
```java
Panache.withTransaction(() -> {
    return entity.persist();
})
```

### Error handling:
```java
.onFailure().transform(ex -> new MyException("Failed", ex))
```
