# COD351
Software Designed Storage

## JSON Responses

1. Create Bucket - PUT
* http://localhost:8080/createBucket?bucketKey=b11
* {"response":"Create Bucket is Successful"}
* {"response":"failed. Bucket with same name is already present"}

2. List Buckets - GET
* http://localhost:8080/listBuckets
* {"bucketList":[{"1":"b11"},{"2":"b3"},{"3":"b4"}]}

3. Delete Bucket - DELETE
* http://localhost:8080/deleteBucket?bucketKey=b11
* {"response":"success"}
* {"response":"failed. No such bucket exists"}

4. Put Object - PUT
* http://localhost:8080/putObject?bucketKey=b12&objectKey=o1
* {"response":" len: 32 successpath : \/user\/rishit\/smallFiles\/b12,o1_1493201588726\nname : o1\nencoding : null\ncontentType : text\/plain\n"}

5. Get Object - GET
* http://localhost:8080/getObject?bucketKey=b12&objectKey=o1

6. List Objects - GET
* http://localhost:8080/listObjects?bucketKey=b12
* {"objectList":[{"1":"o1"}]}
* {"objectList":[]} (if bucket not present)

7. Delete Object - GET
* http://localhost:8080/deleteObject?bucketKey=b12&objectKey=o1
* {"response":"success"}
* {"response":"failed"}