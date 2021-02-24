package ru.openfs.function;

import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.annotations.cache.NoCache;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.NQuad;
import io.dgraph.DgraphProto.Request;
import io.dgraph.DgraphProto.Value;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.quarkus.grpc.runtime.annotations.GrpcService;

@Path("/")
public class FileUploadResource {
    static int OBJECT_NAME_LEN = 22;
    Random random = new Random();

    @ConfigProperty(name = "minio.bucket", defaultValue = "test")
    String bucket;

    @Inject
    MinioClient minio;

    @Inject
    @GrpcService("db")
    DgraphGrpc.DgraphBlockingStub db;

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.TEXT_PLAIN)
    @NoCache
    public String upload(MultipartFormDataInput body) throws Exception {
        String title = decodeTitle(body.getFormDataPart("title", String.class, null));
        InputPart fileInput = body.getFormDataMap().get("file").get(0);
        // create fileObjectName
        String object = createObjectName(fileInput.getMediaType().getSubtype());
        // put file to object store
        minio.putObject(PutObjectArgs.builder().bucket(bucket).object(object)
                .contentType(fileInput.getMediaType().getType() + "/" + fileInput.getMediaType().getSubtype())
                .stream(fileInput.getBody(InputStream.class, null), -1, 5 * 1024 * 1024).build());
        StatObjectResponse statFile = minio.statObject(StatObjectArgs.builder().bucket(bucket).object(object).build());
        // commit to db
        Set<NQuad> setNQuads = new HashSet<NQuad>();
        setNQuads.add(
            NQuad.newBuilder().setSubject("_:image").setPredicate("dgraph.type")
                .setObjectValue(Value.newBuilder().setStrVal("Image").build()).build());
        setNQuads.add(
            NQuad.newBuilder().setSubject("_:image").setPredicate("Image.title")
                .setObjectValue(Value.newBuilder().setStrVal(title).build()).build());
        setNQuads.add(
            NQuad.newBuilder().setSubject("_:image").setPredicate("Image.dt")
                .setObjectValue(Value.newBuilder().setIntVal(System.currentTimeMillis()).build()).build());
        setNQuads.add(
            NQuad.newBuilder().setSubject("_:image").setPredicate("Image.date")
                .setObjectValue(Value.newBuilder().setStrVal(statFile.lastModified().toString()).build()).build());
        setNQuads.add(
            NQuad.newBuilder().setSubject("_:image").setPredicate("Image.sizes").setObjectId("_:size").build());
        setNQuads.add(
            NQuad.newBuilder().setSubject("_:size").setPredicate("dgraph.type")
                .setObjectValue(Value.newBuilder().setStrVal("ImageSize").build()).build());
        setNQuads.add(
            NQuad.newBuilder().setSubject("_:size").setPredicate("ImageSize.imageType")
                .setObjectValue(Value.newBuilder().setStrVal("orig").build()).build());
        setNQuads.add(
            NQuad.newBuilder().setSubject("_:size").setPredicate("ImageSize.image")
                .setObjectValue(Value.newBuilder().setStrVal(object).build()).build());
        return db.query(Request.newBuilder().addMutations(Mutation.newBuilder().addAllSet(setNQuads).build())
                .setCommitNow(true).build()).getUidsOrDefault("image", "defaultValue");
    }

    private String createObjectName(String suffix) {
        return random.ints(48, 122).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97)).limit(OBJECT_NAME_LEN)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString() + "."
                + suffix;
    }

    private String decodeTitle(String title) {
        String decodedTitle = URLDecoder.decode(title, Charset.forName("UTF-8"));
        return decodedTitle.contains(".") ? decodedTitle.substring(0, decodedTitle.lastIndexOf(".")) : decodedTitle;
    }
}