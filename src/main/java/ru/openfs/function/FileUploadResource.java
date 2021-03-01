package ru.openfs.function;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.protobuf.ByteString;

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
import io.minio.RemoveObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.quarkus.grpc.runtime.annotations.GrpcService;
import io.vertx.core.json.JsonObject;

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
        // generate objectName
        String object = createObjectName(fileInput.getMediaType().getSubtype());
        // create db request
        Set<NQuad> setNQuads = new HashSet<NQuad>();
        // set type Image
        setNQuads.add(NQuad.newBuilder().setSubject("_:image").setPredicate("dgraph.type")
                .setObjectValue(Value.newBuilder().setStrVal("Image").build()).build());
        // set title
        setNQuads.add(NQuad.newBuilder().setSubject("_:image").setPredicate("Image.title")
                .setObjectValue(Value.newBuilder().setStrVal(title).build()).build());
        // set date (iso)
        setNQuads.add(NQuad.newBuilder().setSubject("_:image").setPredicate("Image.date")
                .setObjectValue(
                        Value.newBuilder().setStrVal(ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT)).build())
                .build());
        // set dt as usec
        setNQuads.add(NQuad.newBuilder().setSubject("_:image").setPredicate("Image.dt")
                .setObjectValue(Value.newBuilder().setIntVal(System.currentTimeMillis() / 1000).build()).build());
        // add ImageSize reference
        setNQuads.add(
                NQuad.newBuilder().setSubject("_:image").setPredicate("Image.sizes").setObjectId("_:size").build());
        // set type ImageSize
        setNQuads.add(NQuad.newBuilder().setSubject("_:size").setPredicate("dgraph.type")
                .setObjectValue(Value.newBuilder().setStrVal("ImageSize").build()).build());
        // set image type orig
        setNQuads.add(NQuad.newBuilder().setSubject("_:size").setPredicate("ImageSize.type")
                .setObjectValue(Value.newBuilder().setStrVal("og").build()).build());
        // set image path (bucket + object)
        setNQuads.add(NQuad.newBuilder().setSubject("_:size").setPredicate("ImageSize.path")
                .setObjectValue(Value.newBuilder().setStrVal(bucket + '/' + object).build()).build());
        // store to db
        Map<String, String> uids = db.query(Request.newBuilder()
                .addMutations(Mutation.newBuilder().addAllSet(setNQuads).build()).setCommitNow(true).build())
                .getUidsMap();
        // put file to object store with tag fileid
        minio.putObject(PutObjectArgs.builder().bucket(bucket).object(object).tags(uids)
                .contentType(fileInput.getMediaType().getType() + "/" + fileInput.getMediaType().getSubtype())
                .stream(fileInput.getBody(InputStream.class, null), -1, 5 * 1024 * 1024).build());
        return uids.get("image");
    }

    @DELETE
    @Path("/{id}")
    public void deleteFile(@PathParam("id") String id) throws Exception {
        // get file path list
        String pathJson = db.query(Request.newBuilder()
                .setQuery(String.format("{dd(func: uid(%s)) { Image.sizes { ImageSize.path }}}", id)).setReadOnly(true)
                .build()).getJson().toStringUtf8();
        JsonObject path = new JsonObject(pathJson);
        // delete files
        for (Object sizes : path.getJsonArray("dd").getJsonObject(0).getJsonArray("Image.sizes")) {
            deleteFilePath(((JsonObject) sizes).getString("ImageSize.path"));
        }
        // cleanup sizes
        db.query(Request.newBuilder()
                .setQuery(String.format("query {q(func: uid(%s)) { Image.sizes { v as uid }}}", id))
                .addMutations(Mutation.newBuilder().setDelNquads(ByteString.copyFromUtf8("uid(v) * * .\n")).build())
                .setCommitNow(true).build());
        // remove id
        db.query(Request.newBuilder()
                .addMutations(Mutation.newBuilder()
                        .setDelNquads(ByteString.copyFromUtf8(String.format("<%s> * * .\n", id))).build())
                .setCommitNow(true).build());
    }

    private void deleteFilePath(String filePath) throws Exception {
        String bucket = filePath.split("/")[0];
        String filename = filePath.split("/")[1];
        minio.removeObject(RemoveObjectArgs.builder().bucket(bucket).object(filename).build());
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