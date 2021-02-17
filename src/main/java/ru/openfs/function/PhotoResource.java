package ru.openfs.function;

import java.io.InputStream;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.annotations.cache.NoCache;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;

@Path("/")
public class PhotoResource {

        @ConfigProperty(name = "minio.bucket", defaultValue = "test")
        String bucket;

        @Inject
        MinioClient client;

        @POST
        @Consumes(MediaType.MULTIPART_FORM_DATA)
        @Produces(MediaType.APPLICATION_JSON)
        @NoCache
        public String upload(MultipartFormDataInput body) throws Exception {
                String title = StringUtils.decodeTitle(body.getFormDataPart("title", String.class, null));
                InputPart fileInput = body.getFormDataMap().get("file").get(0);
                // put file to object store
                store(fileInput.getBody(InputStream.class, null), fileInput.getMediaType());
                return title;
        }

        @HEAD
        public String head() {
                return "OK";
        }

        private void store(InputStream is, MediaType mediaType) throws Exception {
                String prefix = StringUtils.generateNumber("v", 8, "/");
                String object = StringUtils.generateName(prefix, 11, mediaType.getSubtype());
                client.putObject(PutObjectArgs.builder().bucket(bucket).object(object)
                                .contentType(mediaType.getType() + "/" + mediaType.getSubtype())
                                .stream(is, -1, 5 * 1024 * 1024).build());
        }

}