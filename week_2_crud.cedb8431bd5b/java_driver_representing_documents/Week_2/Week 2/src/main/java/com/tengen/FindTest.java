/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.tengen;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.*;

public class FindTest {
    public static void main(String[] args) throws UnknownHostException {
        //MongoClientOptions options = MongoClientOptions.builder().build();
        //  MongoClient client = new MongoClient(new ServerAddress(),options);

        MongoClient client = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        DB db = client.getDB("school");
        DBCollection collection = db.getCollection("students");

        // MongoCollection<BsonDocument> coll = db.getCollection("test",BsonDocument.class);
        System.out.println("Find one:");
        DBObject one = collection.findOne();
        System.out.println(one);

//        Date d = new Date();
//        System.out.println(d);
//      hw2
//        DBObject query = QueryBuilder.start("scores.type").is("homework").get();
//
//        System.out.println("\nFind all: ");
//        DBCursor cursor = collection.find(query)
//                .sort(new BasicDBObject("_id", 1).append("scores.", 1));
//        DBObject prevCur = null;
//        int flag=0;
//        try {
//          while (cursor.hasNext()) {
//              DBObject cur = cursor.next();
//              System.out.println(cur);
//              if (flag==0){
//                  flag=1;
//                  collection.remove(cur);
//                  prevCur=cur;
//                  System.out.println(prevCur.get("student_id"));
//              }else{
//                  if (cur.get("student_id").equals(prevCur.get("student_id"))){
//                      prevCur=cur;
//                  }else{
//                      collection.remove(cur);
//                      prevCur=cur;
//                  }
//              }
//          }
//        } finally {
//            cursor.close();
//        }
        // hw3.1
        DBCursor cursor = collection.find();

        boolean flag= true;
        Object tmpScore = 0;
        BasicDBObject prev = null;
        try {
            while (cursor.hasNext()){
                DBObject cur = cursor.next();
                BasicDBList scores = (BasicDBList) cur.get("scores");
                BasicDBObject[] scoresArr = scores.toArray(new BasicDBObject[0]);
                for(BasicDBObject obj : scoresArr) {
                    if (obj.get("type").equals("homework")) {
                        if (flag) {
                            prev = obj;
                            flag=false;
                        }else {
                            flag = true;
                            if (Float.valueOf(prev.get("score").toString()) < Float.valueOf(obj.get("score").toString())) {
                              //  collection.update(scores, new BasicDBObject("$set",obj));

                                scores.remove(prev);
                             //   System.out.println(scores);

                                BasicDBObject upObj = new BasicDBObject();
                                upObj.putAll(cur);

                                upObj.removeField("scores");
                                System.out.println(upObj);
                             //   upObj.put("scores",scores);
                                collection.update(new BasicDBObject("_id", cur.get("_id")), upObj);
                            } else {
                               // collection.update(scores, new BasicDBObject("$set",prev));
                                scores.remove(obj);
                                System.out.println(scores);

                                BasicDBObject upObj = new BasicDBObject();
                                upObj.putAll(cur);

                                upObj.removeField("scores");
                                upObj.put("scores",scores);
                                collection.update(new BasicDBObject("_id", cur.get("_id")), upObj);
                            //    collection.update(cur, new BasicDBObject("$set",scores));
                            }
                        }
                    }


                }



            }
        }finally{
            cursor.close();
        }

        System.out.println("\nCount:");
        long count = collection.count();
        System.out.println(count);
    }
}
