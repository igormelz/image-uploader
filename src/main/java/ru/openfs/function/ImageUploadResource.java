package ru.openfs.function;

import java.io.InputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.protobuf.ByteString;

import org.eclipse.microprofile.config.inject.ConfigProperty;
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
import io.quarkus.grpc.runtime.annotations.GrpcService;
import io.vertx.core.json.JsonObject;

@Path("/")
public class ImageUploadResource {
    static int PREFIX_LENGTH = 7;
    static int NAME_LENGTH = 12;

    @ConfigProperty(name = "minio.bucket", defaultValue = "test")
    String bucket;

    @ConfigProperty(name = "thumbnail.url", defaultValue = "http://localhost:8009")
    URI thumbnailUrl;

    @Inject
    MinioClient minio;

    @Inject
    @GrpcService("db")
    DgraphGrpc.DgraphBlockingStub db;

    HttpClient client;

    @PostConstruct
    void initialize() {
        this.client = HttpClient.newHttpClient();
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response uploadImage(MultipartFormDataInput body) throws Exception {
        String title = decodeTitle(body.getFormDataPart("title", String.class, null));
        InputPart fileInput = body.getFormDataMap().get("file").get(0);

        // test access thumbnail
        try {
            client.send(HttpRequest.newBuilder(thumbnailUrl).method("HEAD", BodyPublishers.noBody())
                    .timeout(Duration.ofSeconds(1L)).build(), BodyHandlers.discarding());
        } catch (Exception e) {
            return Response.status(Response.Status.GATEWAY_TIMEOUT).entity("Service unavailable. Try again later.")
                    .build();
        }

        // generate objectName
        String object = createObjectName(fileInput.getMediaType().getSubtype());

        // store to db
        Map<String, String> uids = db
                .query(Request.newBuilder().addMutations(setImage(bucket, object, title)).setCommitNow(true).build())
                .getUidsMap();

        // put file to object store
        minio.putObject(PutObjectArgs.builder().bucket(bucket).object(object).tags(uids)
                .contentType(fileInput.getMediaType().getType() + "/" + fileInput.getMediaType().getSubtype())
                .stream(fileInput.getBody(InputStream.class, null), -1, 5 * 1024 * 1024).build());
        body.close();

        // call thumbnail
        String thumbnailJson = String.format(
                "{ \"bucket\":\"%s\", \"object\":\"%s\", \"imageUid\":\"%s\", \"imageSizeUid\": \"%s\"}", bucket,
                object, uids.get("image"), uids.get("size"));
        var response = client.send(HttpRequest.newBuilder(thumbnailUrl).header("Content-type", "application/json")
                .POST(BodyPublishers.ofString(thumbnailJson)).build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            return Response.ok("ok").build();
        }
        // cleanup file
        minio.removeObject(RemoveObjectArgs.builder().bucket(bucket).object(object).build());
        cleanupImage(uids.get("image"));
        return Response.status(Response.Status.BAD_REQUEST).entity("Can not store file to cloud").build();
    }

    @DELETE
    @Path("{id}")
    public void deleteImage(@PathParam("id") String id) throws Exception {
        // get file path list
        String pathJson = db.query(Request.newBuilder()
                .setQuery(String.format("{dd(func: uid(%s)) { Image.sizes { ImageSize.path }}}", id)).setReadOnly(true)
                .build()).getJson().toStringUtf8();
        JsonObject path = new JsonObject(pathJson);
        if (path.getJsonArray("dd").getList().size() == 0) {
            return;
        }
        // delete files
        for (Object sizes : path.getJsonArray("dd").getJsonObject(0).getJsonArray("Image.sizes")) {
            deleteFilePath(((JsonObject) sizes).getString("ImageSize.path"));
        }
        cleanupImage(id);
    }

    private void cleanupImage(String id) {
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

    private Mutation setImage(String bucket, String object, String title) {
        Set<NQuad> setNQuads = new HashSet<NQuad>();
        setNQuads.add(NQuad.newBuilder().setSubject("_:image").setPredicate("dgraph.type")
                .setObjectValue(Value.newBuilder().setStrVal("Image").build()).build());
        setNQuads.add(NQuad.newBuilder().setSubject("_:image").setPredicate("Image.title")
                .setObjectValue(Value.newBuilder().setStrVal(title).build()).build());
        setNQuads.add(NQuad.newBuilder().setSubject("_:image").setPredicate("Image.date")
                .setObjectValue(
                        Value.newBuilder().setStrVal(ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT)).build())
                .build());
        setNQuads.add(NQuad.newBuilder().setSubject("_:image").setPredicate("Image.dt")
                .setObjectValue(Value.newBuilder().setIntVal(System.currentTimeMillis() / 1000).build()).build());
        setNQuads.add(
                NQuad.newBuilder().setSubject("_:image").setPredicate("Image.sizes").setObjectId("_:size").build());
        setNQuads.add(NQuad.newBuilder().setSubject("_:size").setPredicate("dgraph.type")
                .setObjectValue(Value.newBuilder().setStrVal("ImageSize").build()).build());
        setNQuads.add(NQuad.newBuilder().setSubject("_:size").setPredicate("ImageSize.type")
                .setObjectValue(Value.newBuilder().setStrVal("og").build()).build());
        setNQuads.add(NQuad.newBuilder().setSubject("_:size").setPredicate("ImageSize.path")
                .setObjectValue(Value.newBuilder().setStrVal(bucket + '/' + object).build()).build());
        return Mutation.newBuilder().addAllSet(setNQuads).build();
    }

    private void deleteFilePath(String filePath) throws Exception {
        int sep = filePath.indexOf("/");
        String bucket = filePath.substring(0, sep);
        String object = filePath.substring(sep + 1);
        minio.removeObject(RemoveObjectArgs.builder().bucket(bucket).object(object).build());
    }

    private String createObjectName(String suffix) {
        String generatedString = ThreadLocalRandom.current().ints(48, 122)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97)).limit(NAME_LENGTH)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
        return createPrefix() + generatedString + "." + suffix;
    }

    private String createPrefix() {
        String generatedString = ThreadLocalRandom.current().ints(48, 58).limit(PREFIX_LENGTH)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
        return "x" + generatedString + "/";
    }

    private String decodeTitle(String title) {
        String decodedTitle = URLDecoder.decode(title, Charset.forName("UTF-8"));
        return decodedTitle.contains(".") ? decodedTitle.substring(0, decodedTitle.lastIndexOf(".")) : decodedTitle;
    }
}