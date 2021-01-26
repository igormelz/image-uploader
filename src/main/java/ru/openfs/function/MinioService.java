package ru.openfs.function;

import java.io.InputStream;
import java.net.URI;
import java.util.Collections;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.ws.rs.core.MediaType;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.minio.GetPresignedObjectUrlArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.http.Method;
import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class MinioService {
    @ConfigProperty(name = "minio.endpoint")
    public String endpoint;
    @ConfigProperty(name = "minio.access.key")
    public String accessKey;
    @ConfigProperty(name = "minio.secret.key")
    public String secretKey;
    
    private MinioClient client;

    void onStart(@Observes StartupEvent ev) {
        client = MinioClient.builder().endpoint(endpoint).credentials(accessKey, secretKey).build();
    }

    public String store(String bucket, String title, InputStream is, MediaType mediaType) throws Exception {
        String prefix = StringUtils.generateNumber("v", 8, "/");
        String object = StringUtils.generateName(prefix, 11, mediaType.getSubtype());
        client.putObject(PutObjectArgs.builder().bucket(bucket).object(object)
                .userMetadata(Collections.singletonMap("title", title))
                .contentType(mediaType.getType() + "/" + mediaType.getSubtype()).stream(is, -1, 5 * 1024 * 1024)
                .build());
        return URI
                .create(client.getPresignedObjectUrl(
                        GetPresignedObjectUrlArgs.builder().method(Method.GET).bucket(bucket).object(object).build()))
                .getPath();
    }

}
