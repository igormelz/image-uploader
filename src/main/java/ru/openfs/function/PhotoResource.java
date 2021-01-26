package ru.openfs.function;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.jboss.resteasy.annotations.cache.NoCache;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

@Path("/")
public class PhotoResource {

    @Inject
    MinioService storage;

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @NoCache
    public String upload(MultipartFormDataInput body) throws Exception {
        String title = StringUtils.decodeTitle(body.getFormDataPart("title", String.class, null));
        var fileInput = body.getFormDataMap().get("file").get(0);
        String answer = storage.store("test", title, fileInput.getBody(InputStream.class, null),
                fileInput.getMediaType());
        var response = HttpClient.newHttpClient()
                .send(HttpRequest.newBuilder(URI.create("http://83.68.33.151:8080/function/tconvh"))
                        .POST(BodyPublishers.ofString("{\"image\":\"" + answer + "\"}")).build(),
                        BodyHandlers.ofString());
        return response.body();
    }

}