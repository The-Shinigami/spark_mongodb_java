//package org.example.controller;
//
//import jakarta.ws.rs.*;
//import jakarta.ws.rs.core.MediaType;
//
//@Path("hello")
//@Produces(MediaType.TEXT_PLAIN)
//public class HelloResource {
//    @GET
//    public String getHello() {
//        return "Bonjour ENSMA";
//    }
//    @GET
//    @Path("{id}")
//    public String getHello(@PathParam("id") String id,
//                           @DefaultValue(
//                                   "votre serviteur") @HeaderParam("name") String name) {
//        return "Bonjour " + id + " de la part de " + name;
//    }
//}
