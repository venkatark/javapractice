package com.clusterdb.kafka;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.common.metrics.Metrics;
import org.bson.Document;
import sun.text.normalizer.Trie;

import java.util.List;

public class TestMongo {
    public static  void main(String [] args) {
        // Since 2.10.0, uses MongoClient
        MongoClient mongo = new MongoClient( "localhost" , 32792 );
        for(String db : mongo.listDatabaseNames()){
            System.out.println(db);
        }
        MongoDatabase db = mongo.getDatabase("clusterdb");
        db.getCollection(db.listCollectionNames().first()).find().limit(10).map(Document::toJson).iterator().forEachRemaining(s -> System.out.println(s));
        mongo.close();

        Metrics.
    }
}
