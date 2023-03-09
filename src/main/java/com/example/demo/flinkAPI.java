//package com.example.demo;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import okhttp3.*;
//import org.apache.kafka.common.protocol.types.Field;
//
//import java.io.File;
//import java.io.IOException;
//
//public class flinkAPI {
//
//    public static void main(String[] args) throws IOException {
//        File f = new File("C:\\Users\\xyl\\Desktop\\test.jar");
//        uploadJar(f);
//    }
//
//
//    public static boolean uploadJar( File jarFile) throws IOException {
//        OkHttpClient okHttpClient = new OkHttpClient();
//        RequestBody requestBody = new MultipartBody.Builder()
//                .setType(MultipartBody.FORM)
//                .addFormDataPart("file", jarFile.getName(),
//                        RequestBody.create(MediaType.parse("multipart/form-data"), jarFile))
//                .build();
//
//        Request request = new Request.Builder()
//                .url("http://124.221.62.44:6016/jars/upload")
//                .addHeader("Content-Type", "application/x-java-archive")
//                .post(requestBody)
//                .build();
//        Response resp = okHttpClient.newCall(request).execute();
//        System.out.println("resp:"+resp.toString());
//        if (200 == resp.code()) {
//            JSONObject body = JSON.parseObject(resp.body().toString());
//            if ("success".equals(body.getString("status"))) {
//                return true;
//            }
//        }
//        return false;
//    }
//
//}
